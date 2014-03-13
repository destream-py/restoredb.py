#!/usr/bin/env python2
#
#   restoredb
#
#       This is a small example on how to use StreamDecompressor to make a
#       generic pg_restore-like command.
#
#   TL;DR
#
#       ~/$ restoredb -d dbname my_dump.pgdump.xz
#       # will restore a pgdump compressed in xz
#
#       ~/$ restoredb -d dbname my_dump.sql.bz2
#       # will restore an SQL dump compressed in bz2
#
#       ~/$ restoredb -d dbname my_dump.tar.gz
#       # will restore an tar dump compressed in gzip
#
#       ~/$ restoredb -d dbname my_dump.7z
#       # will restore any dump if the 7z contains only one file
#       # and is a pgdump, SQL dump or tar dump
#
#       ~/$ ssh toto@foo.bar "cat remote_dump.zip.xz" | restoredb -d dbname
#       # will restore any dump if the remote zip file over-compressed in xz
#       # contains only one file and is a pgdump, SQL dump or tar dump
#

import io
import os
import sys
import argparse
import subprocess
import tarfile

import StreamDecompressor

try:
    import pgheader
    import time
except ImportError:
    pgheader = None

__all__ = """
    PostgreSQLDump PostgreSQLTarDump PlainSQL pgdump_guesser open parser run
""".split()


class PostgreSQLDump(StreamDecompressor.ExternalPipe):
    """
    Stream custom dump to pg_restore in order to get SQL
    """
    __command__ = ['pg_restore']
    __compression__ = 'pgdmp_custom'
    __mimes__ = [
        'application/octet-stream',
        'binary',
    ]

    def __init__(self, name, fileobj, toc_pos=0):
        if pgheader:
            self.header = pgheader.ArchiveHandle(
                io.BytesIO(fileobj.peek(16000)[toc_pos:]))
            self.header.ReadHead()
        else:
            self.header = None
        super(PostgreSQLDump, self).__init__(name, fileobj)

    @classmethod
    def __guess__(cls, mime, name, fileobj, toc_pos=0):
        realname = super(PostgreSQLDump, cls).__guess__(mime, name, fileobj)
        magic = fileobj.peek(5)[toc_pos:]
        if not magic[:5] == 'PGDMP':
            raise ValueError("not a postgres custom dump")
        return realname


class PostgreSQLTarDump(PostgreSQLDump):
    """
    Inherit of PostgreSQLDump to add some specific behaviors related to tar.

    Note: this decompressor must run prior to the Untar decompressor
    """
    __compression__ = 'pgdmp_tar'
    __mimes__ = [
        'application/x-tar',
    ]
    __extensions__ = ['tar']

    def __init__(self, name, fileobj):
        super(PostgreSQLTarDump, self)\
            .__init__(name, fileobj, toc_pos=tarfile.BLOCKSIZE)

    @classmethod
    def __guess__(cls, mime, name, fileobj):
        if mime not in cls.__mimes__:
            raise ValueError("not a tar file")
        tarinfo = tarfile.TarInfo.frombuf(
            fileobj.peek(tarfile.BLOCKSIZE)[:tarfile.BLOCKSIZE])
        if not tarinfo.name == 'toc.dat':
            raise ValueError("does not look like a tar dump")
        return super(PostgreSQLTarDump, cls).__guess__(
            mime, name, fileobj, toc_pos=tarfile.BLOCKSIZE)


class PlainSQL(StreamDecompressor.Archive):
    """
    This class only make sure we have text/plain here
    """
    __compression__ = 'sql'
    __mimes__ = [
        'text/plain',
    ]

    def __init__(self, name, fileobj):
        if isinstance(fileobj, PostgreSQLDump):
            self.header = fileobj.header
        else:
            self.header = None
        super(PlainSQL, self).__init__(name, fileobj, fileobj)

    @classmethod
    def __guess__(cls, mime, name, fileobj):
        if type(fileobj) is PlainSQL:
            raise ValueError("prevent looping on text/plain")
        return super(PlainSQL, cls).__guess__(mime, name, fileobj)


pgdump_guesser = StreamDecompressor.Guesser(
    extra_decompressors=[
        (-10, PostgreSQLTarDump),
        (  0, PostgreSQLDump),
        (  0, PlainSQL),
    ],
)


def open(name=None, fileobj=None):
    return pgdump_guesser.open(name=name, fileobj=fileobj)


parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--help', action='store_true')
parser.add_argument('--dbname', '-d', dest='dbname', type=str)
parser.add_argument('--host', '-h',
    help="Specifies the TCP port or the local Unix-domain socket file.")
parser.add_argument('--username', '-U',
    help="Connect to the database as the user username instead "
         "of the default.")
parser.add_argument('--port', '-p', type=int)
parser.add_argument('--no-owner', '-O', action='store_true',
    help="Do not output commands to set ownership of objects to match the "
         "original database.")
parser.add_argument('--no-privileges', '--no-acl', '-x', action='store_true',
    help="Prevent restoration of access privileges (grant/revoke commands).")
parser.add_argument('--clean', '-c', action='store_true',
    help="Clean (drop) database objects before recreating them.")
parser.add_argument('--create', '-C', action='store_true',
    help="Create the database before restoring into it. If --clean is also "
         "specified, drop and recreate the target database before connecting "
         "to it.")
parser.add_argument('--no-header', action='store_true',
    help="Do not print header (when available)")
parser.add_argument('--debug', action='store_true')
parser.add_argument('dump', nargs='?')

def warn(*messages):
    sys.stderr.write(" ".join(map(unicode, messages))+"\n")

def die(*messages):
    warn(*messages)
    sys.exit(1)

def debug(*messages):
    if args.debug:
        warn("debug:", *messages)

def run(args):
    if args.help:
        parser.print_help()
        sys.exit(0)

    if args.no_owner:
        PostgreSQLDump.__command__.append('--no-owner')

    if args.no_privileges:
        PostgreSQLDump.__command__.append('--no-privileges')

    if args.clean:
        PostgreSQLDump.__command__.append('--clean')

    if args.create:
        PostgreSQLDump.__command__.append('--create')

    if args.dump:
        archive = pgdump_guesser.open(name=args.dump)
    else:
        archive = pgdump_guesser.open(fileobj=sys.stdin)
    if not archive.compressions or archive.compressions[-1] != 'sql':
        die("Not a PostgreSQL dump (%s)" % ", ".join(archive.compressions))

    if archive.header and not args.no_header:
        header = dict(archive.header.__dict__,
            createDate=time.ctime(archive.header.createDate),
            format={
                0 : 'UNKNOWN',
                1 : 'CUSTOM',
                3 : 'TAR',
                4 : 'NULL',
                5 : 'DIRECTORY',
            }[archive.header.format])
        sys.stderr.write(
            ";\n"
            "; Archive created at %(createDate)s\n"
            ";     dbname: %(archdbname)s\n"
            ";     TOC Entries:\n"
            ";     Compression: %(compression)s\n"
            ";     Dump Version: %(vmaj)d.%(vmin)d-%(vrev)d\n"
            ";     Format: %(format)s\n"
            ";     Integer: %(intSize)d bytes\n"
            ";     Offset: %(offSize)d bytes\n"
            ";     Dumped from database version: %(archiveDumpVersion)s\n"
            ";     Dumped by pg_dump version: %(archiveRemoteVersion)s\n"
            ";\n"
            % header
        )

    if 'pgdump' in archive.compressions:
        debug("pg_restore arguments:", PostgreSQLDump.__command__)

    if not args.dbname or args.dbname == '-':
        try:
            sys.stdout.writelines(archive)
            sys.exit(0)
        except IOError, e:
            archive.close()
            die(e.args[1])
    else:
        command_args = ['psql', '--dbname', args.dbname]

        # psql arguments
        if args.host:
            command_args += ['--host', args.host]
        if args.port:
            command_args += ['--port', str(args.port)]
        if args.username:
            command_args += ['--username', args.username]

        debug("command arguments:", command_args)
        # NOTE: can't use stdin=archive because it doesn't flush line-by-line
        psql = subprocess.Popen(command_args, stdin=subprocess.PIPE,
            stdout=io.open(os.devnull))
        try:
            psql.stdin.writelines(archive)
            debug("psql: finished writing lines, closing...")
            psql.stdin.close()
        except IOError:
            archive.close()
            die(e.args[1])
        retcode = psql.wait()
        debug("psql exit status:", retcode)
        sys.exit(retcode)

if __name__ == '__main__':
    args = parser.parse_args()
    run(args)
