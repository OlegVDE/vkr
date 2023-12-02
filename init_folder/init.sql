CREATE DATABASE IF NOT EXISTS test_db;

CREATE TABLE IF NOT EXISTS test_db.clients_info
(
  client String,
  hostname String,
  alias_list String,
  addres_list Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (client, hostname, alias_list);