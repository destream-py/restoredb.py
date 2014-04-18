import io
import os
import unittest2
import subprocess
import tempfile
import psycopg2
from textwrap import dedent
import tarfile

import restoredb

if not hasattr(subprocess, 'DEVNULL'):
    subprocess.DEVNULL = io.open(os.devnull, 'wb')

def run(command, *a, **kw):
    stdin = kw.pop('stdin', None)
    command_arguments = [command]
    command_arguments.extend([
        (("-" if len(k) == 1 else "--") + str(k) + "=" + str(v))
        for k, v in kw.items() if v is not None
    ])
    command_arguments.extend(list(a))
    r, w = os.pipe()
    # NOTE: FileIO will automatically close the fd when deleted
    with io.FileIO(w, 'w') as stdout:
        subprocess.check_call(command_arguments,
            stderr=stdout, stdin=stdin, stdout=stdout)
    with io.FileIO(r, 'r') as fd:
        return fd.read()

def connect(dbname, host=None, port=None, username=None):
    return psycopg2.connect(database=dbname,
        host=host, port=port, user=username)

class Transaction(object):
    def __init__(self, cur):
        self.cur = cur

    def __enter__(self):
        self.cur.execute("BEGIN;")
        return self.cur

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.cur.execute("COMMIT;")
        else:
            self.cur.execute("ROLLBACK;")

class FromFile(unittest2.TestCase):
    address = {
        'host' : os.environ.get('PGHOST', 'localhost'),
        'port' : os.environ.get('PGPORT', 5432),
        'username' : os.environ.get('PGUSER', os.environ['USER']),
    }
    basename = 'test'
    createdb = 'createdb'
    dropdb = 'dropdb'
    pg_dump = 'pg_dump'
    pg_restore = 'pg_restore'
    init_db = dedent("""\
        CREATE TABLE fromfile_table (
            id                  serial,
            fromfile_column     text
        );
        """)
    check_db = dedent("""\
        SELECT  1
        FROM    information_schema.columns
        WHERE
            table_name = 'fromfile_table' AND
            column_name = 'fromfile_column'
        LIMIT 1;
        """)

    def _connect(self):
        self.dbh = connect(self.dbname, **self.address)

    def _close(self):
        self.dbh.close()

    def _initdb(self):
        with Transaction(self.dbh.cursor()) as cur:
            cur.execute(self.init_db)

    def _checkdb(self):
        with Transaction(self.dbh.cursor()) as cur:
            cur.execute(self.check_db)
            if not cur.fetchone():
                self.fail("Database verification failed")

    def _createdb(self):
        run(self.createdb, self.dbname, **self.address)

    def _dropdb(self):
        run(self.dropdb, self.dbname, **self.address)

    def _dumpdb(self, *a, **kw):
        kw = dict(self.address.items() + kw.items())
        a = list(a) + [self.dbname]
        return run(self.pg_dump, *a, **kw)

    def _restoredb(self, *a, **kw):
        kw = dict(self.address.items() +
                  [('dbname', self.dbname)] + kw.items())
        run(restoredb.__file__,
            self.temp.name, *a, **kw)

    def _cleanup(self):
        self.temp.close()
        try:
            os.unlink(self.temp.name)
        except OSError:
            pass
        self._close()
        try:
            self._dropdb()
        except subprocess.CalledProcessError:
            pass

    def setUp(self):
        self.dbname = "%s_%d" % (self.basename, id(self))
        try:
            self._connect()
            self._close()
        except psycopg2.OperationalError:
            pass
        else:
            raise psycopg2.OperationalError(
                "FATAL:  database exists! dsn: " + self.dsn)
        self._createdb()
        self._connect()
        self._initdb()
        with Transaction(self.dbh.cursor()) as cur:
            cur.execute(self.check_db)
            if not cur.fetchone():
                raise Exception("Database verificator broken")
        self.temp = tempfile.NamedTemporaryFile(delete=False)
        self.addCleanup(self._cleanup)
        self.skip_check = False

    def tearDown(self):
        if not self.skip_check:
            self.temp.close()
            self._close()
            self._dropdb()
            self._createdb()
            try:
                self._restoredb()
            except subprocess.CalledProcessError, e:
                self.fail("can not restore dump: %s" % e)
            self._connect()
            self._checkdb()

    @unittest2.expectedFailure
    def test_01_no_one_shall_pass(self):
        self.skip_check = True
        self.fail("let's cancel checks")

    def test_10_sql_dump(self):
        self.temp.write(self._dumpdb(format='plain'))

    def test_10_custom_dump(self):
        self.temp.write(self._dumpdb(format='custom'))

    def test_10_tar_dump(self):
        self.temp.write(self._dumpdb(format='tar'))

    def test_20_sql_dump_in_a_tar(self):
        dump = io.BytesIO(self._dumpdb('-F', 'p'))
        tar = tarfile.open(fileobj=self.temp, mode='w')
        try:
            tarinfo = tarfile.TarInfo('test_file')
            tarinfo.size = len(dump.getvalue())
            tar.addfile(tarinfo, dump)
        finally:
            tar.close()

    def test_20_custom_dump_in_a_tar(self):
        dump = io.BytesIO(self._dumpdb('-F', 'c'))
        tar = tarfile.open(fileobj=self.temp, mode='w')
        try:
            tarinfo = tarfile.TarInfo('test_file')
            tarinfo.size = len(dump.getvalue())
            tar.addfile(tarinfo, dump)
        finally:
            tar.close()

    def test_20_tar_dump_in_a_tar(self):
        dump = io.BytesIO(self._dumpdb('-F', 't'))
        tar = tarfile.open(fileobj=self.temp, mode='w')
        try:
            tarinfo = tarfile.TarInfo('test_file')
            tarinfo.size = len(dump.getvalue())
            tar.addfile(tarinfo, dump)
        finally:
            tar.close()

class FromStdin(FromFile):
    def _restoredb(self, *a, **kw):
        kw = dict(self.address.items() +
                  [('dbname', self.dbname)] +
                  kw.items(), stdin=open(self.temp.name, 'rb'))
        a = ['--debug']
        run(restoredb.__file__, *a, **kw)
