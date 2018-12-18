restoredb
=========

This is a small example on how to use StreamDecompressor to make a generic
pg_restore-like command.

Installation
------------

```
pip install restoredb
```

TL;DR
-----

```bash
~/$ restoredb -d dbname my_dump.pgdump.xz
# will restore a pgdump compressed in xz

~/$ restoredb -d dbname my_dump.sql.bz2
# will restore an SQL dump compressed in bz2

~/$ restoredb -d dbname my_dump.tar.gz
# will restore an tar dump compressed in gzip

~/$ restoredb -d dbname my_dump.7z
# will restore any dump if the 7z contains only one file
# and is a pgdump, SQL dump or tar dump

~/$ ssh toto@foo.bar "cat remote_dump.zip.xz" | restoredb -d dbname
# will restore any dump if the remote zip file over-compressed in xz
# contains only one file and is a pgdump, SQL dump or tar dump
```
