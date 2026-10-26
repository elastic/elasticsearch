# ClickHouse data source PoC

This module lets ES|QL read a ClickHouse table through a stored data source and dataset.
It uses the ClickHouse HTTP interface. It does not copy the table into an Elasticsearch index.
The module is enabled in snapshot builds.

```mermaid
flowchart LR
  K[Kibana Discover] --> Q[ES|QL]
  Q --> D[Stored dataset and encrypted source credentials]
  D --> C[ClickHouse connector]
  C --> H[ClickHouse HTTP interface]
  H --> B[Streamed compute blocks]
  B --> Q
  Q --> K
```

## Build and test

Run these commands from the Elasticsearch repository. Use the Java toolchain required by that repository.

```sh
./gradlew :x-pack:plugin:esql-datasource-clickhouse:check
./gradlew :x-pack:plugin:esql:test --tests org.elasticsearch.xpack.esql.datasources.DataSourceModuleLazyLoadingTests
./gradlew :distribution:archives:darwin-aarch64-tar:assemble
```

The distribution includes this module. Use an enterprise or trial license and enable `esql.federation.enabled`.
Configure `cluster.state.encryption.password.poc` and `cluster.state.encryption.active_password_id`
in the Elasticsearch keystore. Set the first to a strong random password and the second to `poc`.
Stored source secrets require cluster state encryption.

## Register a source and table

Use a ClickHouse account with SELECT access to the required table. Send the source request over a secure connection.
The endpoint is an HTTPS origin, such as `https://example.clickhouse.cloud:8443`.
HTTP is accepted only for `localhost`, `127.0.0.1`, or `::1`.
Endpoint paths, query strings, embedded credentials, and redirects are not accepted.

```http
PUT /_query/data_source/clickhouse_demo
{
  "type": "clickhouse",
  "settings": {
    "endpoint": "https://example.clickhouse.cloud:8443",
    "username": "esql_reader",
    "password": "<password>",
    "auth": "basic"
  }
}

PUT /_query/dataset/clickhouse_logs
{
  "data_source": "clickhouse_demo",
  "resource": "clickhouse://default/app_logs"
}
```

The resource identifies a database and table. It is not a network address.
Database and table names must match `[A-Za-z_][A-Za-z0-9_]*`.
The connector reads the schema with `SELECT * ... LIMIT 0`.
Do not supply file format settings or field mappings.

Elasticsearch encrypts the password and redacts it in source responses.
Omit `password` when you update a source to keep the stored password.
The module registers `password` for audit request redaction even when its feature flag is disabled.

```esql
FROM clickhouse_logs
| WHERE service == "checkout" AND level == "error" AND latency_ms >= 1500
| KEEP id, ts, message, latency_ms
| SORT latency_ms DESC, id ASC
| LIMIT 10
```

```esql
FROM clickhouse_logs
| STATS rows = COUNT(*), maximum = MAX(latency_ms) BY service
| SORT service
```

## Copy a table to an index

`_reindex` accepts an ES|QL query as its source:

```http
POST _reindex
{
  "source": { "esql": "FROM clickhouse_logs" },
  "dest": { "index": "clickhouse_logs_copy" }
}
```

The ES|QL plugin streams the query result and writes it with bulk requests. It ignores the result size limits.
`script`, `max_docs`, and `slices` are rejected; use `LIMIT` in the query. `refresh`, `requests_per_second`, and
`timeout` are ignored. The task status does not report progress. Elasticsearch assigns new document IDs.

## Type support

| ClickHouse type | ES|QL type |
| --- | --- |
| Int8/16/32/64, UInt8/16/32 | long |
| UInt64 | unsigned_long |
| Float32/64 | double |
| Bool | boolean |
| Date, Date32, DateTime, DateTime64 | date, at millisecond precision |
| String, FixedString, Enum, UUID, IPv4, IPv6 | keyword |
| Decimal and integers wider than 64 bits | keyword, to preserve the value |

`Nullable` and `LowCardinality` wrappers are supported. Null values remain null.
Array, Map, Tuple, JSON, and other unlisted types cause a schema error.
For such tables, create a ClickHouse view that selects or converts the required columns.

## PoC limits

- Only column selection is sent to ClickHouse. Elasticsearch applies filters, sorting, limits, and aggregations.
- Each query can therefore read the full table. Use a small table or a restricted view for the demo.
- The request sets a maximum of 1,000,000 result rows and 60 seconds of ClickHouse execution time.
- The client uses a 10-second connection timeout and a 30-second socket read timeout.
- Each JSON row is limited to 1,048,576 characters. A schema can have at most 1,024 columns.
- The cursor emits batches of at most 1,024 rows. Compute blocks use the Elasticsearch circuit breaker.
- Cancellation closes the HTTP connection. Immediate cancellation of remote execution is not guaranteed.
- There is one stream per query. There is no split planning, connection pool, automatic retry, or aggregate pushdown.
- This is a development PoC. It has not been validated for production, multiple nodes, or ClickHouse Cloud.

The tests use an HTTP fixture. They cover schema reads, credentials, nulls, UInt64 values,
batching, COUNT cardinality, and errors received after an HTTP 200 response.
