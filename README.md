db_restore
==========

Database restoration script to automatically decompress any format and pipe to the right restorer.


Synopsis
========

```bash
# Restore many dumps into one database
db_restore -c psql -d test_database backup_part1.sql backup_part2.sql.gz backup_part3.sql.zip ...

# Restore special PostgreSQL dumps (custom & tar formats)
db_restore -c psql -d test_database backup_part1.dump.7z backup_part2.dump.tar backup_part3.sql.lzma ...
# it will automatically use pg_restore if the file detected is a pg dump

# Give special parameters
db_restore -c psql -o psql="-e" pg_restore="-Oe" -d test_database backup_parts.anything.anyorder
```
