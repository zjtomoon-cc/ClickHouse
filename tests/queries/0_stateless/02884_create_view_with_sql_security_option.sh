#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS test_db;"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE test_db;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_db.test_table (s String) ENGINE = MergeTree ORDER BY s;"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS user02884_1, user02884_2, user02884_3";
${CLICKHOUSE_CLIENT} --query "CREATE USER user02884_1, user02884_2, user02884_3";

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_db.* TO user02884_1"

echo "===== StorageView ====="
${CLICKHOUSE_CLIENT} --query "
  CREATE VIEW test_db.test_view_1 (s String)
  AS SELECT * FROM test_db.test_table;
"

${CLICKHOUSE_CLIENT} --query "
  CREATE DEFINER user02884_1 VIEW test_db.test_view_2 (s String)
  AS SELECT * FROM test_db.test_table;
"

${CLICKHOUSE_CLIENT} --query "
  CREATE DEFINER = user02884_1 SQL SECURITY DEFINER VIEW test_db.test_view_3 (s String)
  AS SELECT * FROM test_db.test_table;
"

${CLICKHOUSE_CLIENT} --query "
  CREATE DEFINER = user02884_1 SQL SECURITY INVOKER VIEW test_db.test_view_4 (s String)
  AS SELECT * FROM test_db.test_table;
"

${CLICKHOUSE_CLIENT} --query "
  CREATE SQL SECURITY INVOKER VIEW test_db.test_view_5 (s String)
  AS SELECT * FROM test_db.test_table;
"

${CLICKHOUSE_CLIENT} --query "
  CREATE SQL SECURITY DEFINER VIEW test_db.test_view_6 (s String)
  AS SELECT * FROM test_db.test_table;
"

${CLICKHOUSE_CLIENT} --query "
  CREATE DEFINER CURRENT_USER VIEW test_db.test_view_7 (s String)
  AS SELECT * FROM test_db.test_table;
"

${CLICKHOUSE_CLIENT} --query "
  CREATE DEFINER user02884_3 VIEW test_db.test_view_8 (s String)
  AS SELECT * FROM test_db.test_table;
"

(( $(${CLICKHOUSE_CLIENT} --query "SHOW TABLE test_db.test_view_1" 2>&1 | grep -c "DEFINER") >= 1 )) && echo "UNEXPECTED" || echo "OK"
(( $(${CLICKHOUSE_CLIENT} --query "SHOW TABLE test_db.test_view_2" 2>&1 | grep -c "DEFINER = user02884_1") >= 1 )) && echo "OK" || echo "UNEXPECTED"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_db.test_view_1 TO user02884_2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_db.test_view_2 TO user02884_2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_db.test_view_3 TO user02884_2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_db.test_view_4 TO user02884_2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_db.test_view_5 TO user02884_2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_db.test_view_6 TO user02884_2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_db.test_view_7 TO user02884_2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_db.test_view_8 TO user02884_2"


${CLICKHOUSE_CLIENT} --query "INSERT INTO test_db.test_table VALUES ('foo'), ('bar');"

(( $(${CLICKHOUSE_CLIENT} --user user02884_2 --query "SELECT * FROM test_db.test_view_1" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --user user02884_2 --query "SELECT count() FROM test_db.test_view_2"
${CLICKHOUSE_CLIENT} --user user02884_2 --query "SELECT count() FROM test_db.test_view_3"
(( $(${CLICKHOUSE_CLIENT} --user user02884_2 --query "SELECT * FROM test_db.test_view_4" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
(( $(${CLICKHOUSE_CLIENT} --user user02884_2 --query "SELECT * FROM test_db.test_view_5" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --user user02884_2 --query "SELECT count() FROM test_db.test_view_6"
${CLICKHOUSE_CLIENT} --user user02884_2 --query "SELECT count() FROM test_db.test_view_7"
(( $(${CLICKHOUSE_CLIENT} --user user02884_2 --query "SELECT * FROM test_db.test_view_8" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"


echo "===== MaterializedView ====="
${CLICKHOUSE_CLIENT} --query "
  CREATE DEFINER = user02884_1 SQL SECURITY DEFINER
  MATERIALIZED VIEW test_db.test_mv_1 (s String)
  ENGINE = MergeTree ORDER BY s
  AS SELECT * FROM test_db.test_table;
"

${CLICKHOUSE_CLIENT} --query "
  CREATE SQL SECURITY INVOKER
  MATERIALIZED VIEW test_db.test_mv_2 (s String)
  ENGINE = MergeTree ORDER BY s
  AS SELECT * FROM test_db.test_table;
"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_db.test_mv_1 TO user02884_2"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_db.test_mv_2 TO user02884_2"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON test_db.test_table TO user02884_2"

${CLICKHOUSE_CLIENT} --user user02884_2 --query "SELECT count() FROM test_db.test_mv_1"
${CLICKHOUSE_CLIENT} --user user02884_2 --query "SELECT count() FROM test_db.test_mv_2"

(( $(${CLICKHOUSE_CLIENT} --user user02884_2 --query "INSERT INTO test_db.test_table VALUES ('one'), ('two');" 2>&1 | grep -c "Not enough privileges") >= 1 )) && echo "OK" || echo "UNEXPECTED"
${CLICKHOUSE_CLIENT} --query "DROP VIEW test_db.test_mv_2;"
${CLICKHOUSE_CLIENT} --user user02884_2 --query "INSERT INTO test_db.test_table VALUES ('one'), ('two');"
${CLICKHOUSE_CLIENT} --user user02884_2 --query "SELECT count() FROM test_db.test_mv_1"


#(( $(${CLICKHOUSE_CLIENT} --user user02884_1 --query "
#  CREATE VIEW test_db.test_view (s String)
#  AS SELECT * FROM test_db.test_table;
#  DEFINER user02884_2
#  SQL SECURITY DEFINER
#" 2>&1 | grep -c "Not enough rights") >= 1 )) && echo 1 || echo "NO MATCH"

#{CLICKHOUSE_DATABASE_1:Identifier}