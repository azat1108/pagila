#!/bin/sh
createdb -U postgres pagila
pg_restore -U postgres -d pagila /docker-entrypoint-initdb.d/pagila-data-apt-jsonb.backup
pg_restore -U postgres -d pagila /docker-entrypoint-initdb.d/pagila-data-yum-jsonb.backup