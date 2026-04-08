# EXTERNAL Command — Syntax Design

**Date**: 2026-03-11

---

## Principles

1. **Single syntax for all data sources.** Blob stores, catalogs, SQL databases, streaming sources, APIs — one command shape covers all. The syntax does not change based on what you're querying.

2. **CRUD registration is optional.** A user can run a query with zero setup — just a URI. A user can also register a data source via CRUD API and reference it. Both work. The system operates at any point on the spectrum from zero infrastructure to fully managed.

3. **WITH and CRUD config merge.** A registered data source provides base configuration (endpoint, credentials, region). The WITH clause overrides or extends it. Combined, they form the full connection info. WITH uses the same JSON structure as the CRUD API body. WITH is purely configuration — no routing or identity decisions.

4. **Resource spec is opaque to the planner.** The quoted string — path, glob, table name, SQL query, metric name — is passed to the data source implementation as-is. The planner does not parse, validate, or interpret it. The data source decides what it means.

5. **Data source name is a positional argument.** When a registered data source is used, its name appears as an unquoted identifier in the command, before the resource spec. It is not a WITH option — it is part of the command structure, just as an index name is part of FROM.

6. **Connector type is always specifiable.** Auto-detection from the resource spec (URI scheme, file extension) is the default. When auto-detection is ambiguous — e.g., the same S3 path could be read as raw files or via an Iceberg catalog — the user can specify the connector explicitly. DATASOURCE is a routing decision, not configuration, so it has its own syntax position rather than being buried in WITH. Storage provider and format are also specifiable via `storage` and `format` in WITH.

7. **Type is inferred by default.** When DATASOURCE is not specified and no registered source provides it, the system infers the data source type from the resource spec — URI scheme identifies the storage provider, file extension identifies the format. This covers the common case without requiring the user to state what is already obvious from the URI.

8. **Credentials never appear as raw values.** Both CRUD registration and inline WITH reference secrets via template syntax (`{{SECRET_NAME}}`), resolving against a separate secret store at execution time. Default credential providers (IAM instance roles, Application Default Credentials, managed identity) work without any explicit auth.

9. **WITH is never used for connector routing.** The connector is determined from the command structure only: explicit DATASOURCE keyword, sourceRef registration, or inference from resourceSpec (URI scheme). WITH never influences which connector handles the query. WITH is source-specific configuration — connection parameters, parsing options (including `format` when file extension is ambiguous), query tuning.

10. **Data sources validate arguments early.** When a data source receives its configuration (merged CRUD + WITH) and resource spec, it validates completeness and correctness before execution begins. Missing required fields (e.g., no credentials and no default credential provider), invalid option values, or malformed resource specs produce clear errors at analysis time, not at read time.

---

## Syntax

```
EXTERNAL (DATASOURCE type | sourceRef)? resourceSpec [WITH { ... }]?
```

| Component | Form | Required | Description |
|-----------|------|----------|-------------|
| `DATASOURCE type` | keyword + unquoted identifier | No | Explicit connector type. Valid connectors today: `file` (blob store — composes storage + format), `iceberg` (catalog), `flight` (Arrow Flight). Future: `jdbc`, `promql`. Not a storage provider (s3, gcs) or format (csv, parquet) — those are components within a connector, specified via `storage`/`format` in WITH. Mutually exclusive with sourceRef. |
| `sourceRef` | unquoted identifier | No | Name of a registered data source — analogous to a database or cluster. Provides connectivity and type. Mutually exclusive with DATASOURCE. |
| `resourceSpec` | quoted string | **Yes** | Identifies a tabular dataset within the data source. Opaque to the parser — meaning determined by the data source. Could be a URI, glob pattern, table name, SQL query, metric expression, or anything else that produces rows and columns. |
| `WITH { ... }` | JSON map | No | Configuration overrides. Merged with registered data source config (if any). Same structure as CRUD API body. Purely config — no routing or identity. |

DATASOURCE and sourceRef are **mutually exclusive**. A registered data source always carries its type (type is mandatory in CRUD registration). If you need a different type, create a different registration — don't override at query time.

> **Naming note**: `DATASOURCE` is a placeholder keyword — the name is not final and will change. The semantics (explicit data source type selection, mutually exclusive with sourceRef) are stable; the keyword itself needs a better name.

### Core examples

```sql
-- Everything inferred: S3 scheme → storage, .parquet → format, IAM → auth
EXTERNAL "s3://bucket/logs/*.parquet"

-- Registered source: type, connectivity, credentials all from registration
EXTERNAL prod_s3 "logs/2026-03/*.parquet"

-- Registered source + config override: base from registration, delimiter from WITH
EXTERNAL prod_s3 "logs/*.csv" WITH { "delimiter": ";" }

-- Explicit connector + storage/format: no URI scheme, no known extension
EXTERNAL DATASOURCE file "app/*.log" WITH {
  "storage": "s3",
  "format": "ndjson",
  "secrets": { "access_key": "{{AWS_KEY}}", "secret_key": "{{AWS_SECRET}}" }
}

-- Explicit type, inline connectivity: no registration, everything in command + WITH
EXTERNAL DATASOURCE jdbc "SELECT * FROM users" WITH {
  "url": "postgresql://host:5432/db",
  "secrets": { "user": "{{PG_USER}}", "password": "{{PG_PASS}}" }
}
```

### Resolution order for DATASOURCE

1. **DATASOURCE explicit** in the command → use it. No sourceRef.
2. **sourceRef present** → type from registration. No DATASOURCE keyword.
3. **Neither** → infer from resourceSpec (URI scheme + file extension via `canHandle()`).

### Config merge

Registered data source config (from CRUD) is the base. WITH overrides on top. The combined result is the full connection configuration passed to the data source implementation.

When no sourceRef is present, WITH alone is the full config. When no WITH is provided, the registered source config alone is the full config.

---

## Syntax nuances

### Data source vs resource spec

A registered data source is analogous to a database or a cluster — it identifies a system that contains data, not the data itself. It holds connectivity: endpoint, credentials, region, catalog URI. It answers "where do I connect?"

The resource spec identifies a tabular dataset within that system. A glob pattern, a table name, a SQL query, a metric — whatever produces rows and columns. It answers "what do I read?"

This separation mirrors every level of Elasticsearch: a remote cluster is not an index. A database is not a table. A bucket is not a file. The EXTERNAL command always requires the resource spec — the data source just tells it where to look.

```sql
-- "prod_s3" is the system (bucket + credentials). "logs/*.parquet" is the dataset.
EXTERNAL prod_s3 "logs/*.parquet"

-- "prod_pg" is the system (database connection, type=jdbc). The SQL query identifies the dataset.
EXTERNAL prod_pg "SELECT * FROM users WHERE active = true"

-- "prod_iceberg" is the system (catalog + warehouse, type=iceberg). "analytics.web_events" is the dataset.
EXTERNAL prod_iceberg "analytics.web_events"
```

There is no `EXTERNAL prod_s3` without a resource spec. The system alone is not queryable — you must name what you want to read.

### DATASOURCE selects the data source implementation

DATASOURCE identifies which data source implementation (`ExternalSourceFactory`) handles the query. In the codebase today, the registered connectors are:

| Connector | `type()` | What it does | How it's composed |
|-----------|----------|-------------|-------------------|
| **`file`** | `"file"` | Reads files from blob stores. | `FileSourceFactory` composes a `StorageProvider` (s3, gcs, azure, http) with a `FormatReader` (parquet, csv, ndjson, orc) and optional `DecompressionCodec` (gzip, zstd, bzip2). |
| **`iceberg`** | `"iceberg"` | Reads tables from Iceberg catalogs. | `LazyTableCatalogWrapper` — manages catalog metadata, delegates storage/format internally. |
| **`flight`** | `"flight"` / `"grpc"` | Queries via Arrow Flight / gRPC. | `LazyConnectorFactory` — registered for `flight://` and `grpc://` schemes. |
| *(future)* `jdbc` | `"jdbc"` | Queries SQL databases. | Would register for `jdbc:` scheme. |

**S3, GCS, Azure, HTTP are not connectors** — they are storage providers, components within the `file` connector. **CSV, Parquet, NDJSON, ORC are not connectors** — they are format readers, also components within the `file` connector.

When `DATASOURCE` is not specified, the system infers it:
1. URI has a known storage scheme (`s3://`, `gs://`, `wasbs://`, `http://`) + file extension → **`file`** connector.
2. URI has a connector scheme (`flight://`, `grpc://`) → that connector.
3. No scheme match → iterate `canHandle()` on all registered factories.

Storage provider and format within the `file` connector are also inferred by default (scheme → storage, extension → format), but can be overridden via `storage` and `format` in WITH.

```sql
-- Inferred: s3:// → file connector, s3 storage, .parquet → parquet format
EXTERNAL "s3://bucket/logs/*.parquet"

-- Override format: .log extension doesn't identify format, specify in WITH
EXTERNAL "s3://bucket/app/*.log" WITH { "format": "ndjson" }

-- Iceberg: sourceRef carries the connector type (type=iceberg in registration)
EXTERNAL prod_iceberg "warehouse.events"
```

### DATASOURCE and sourceRef are mutually exclusive

A registered source always carries its type — type is mandatory in CRUD registration. If you use a sourceRef, the type comes from the registration. If you specify DATASOURCE, you are going inline (no sourceRef). You never combine them.

```sql
-- sourceRef: type comes from registration
EXTERNAL prod_s3 "logs/*.parquet"

-- DATASOURCE: explicit connector, components via WITH
EXTERNAL DATASOURCE file "app/*.log" WITH { "storage": "s3", "format": "ndjson" }

-- NOT valid: EXTERNAL DATASOURCE iceberg prod_s3 "..."
```

If you need to access the same storage with a different connector, create a separate registration with the appropriate type.

### Inline without sourceRef or WITH

When the resource spec is a fully qualified URI and the environment provides default credentials (IAM, ADC, managed identity), nothing else is needed:

```sql
EXTERNAL "s3://bucket/logs/*.parquet"
EXTERNAL "gs://bucket/data/*.csv"
EXTERNAL "wasbs://container@account.blob.core.windows.net/data/*.parquet"
EXTERNAL "https://data.example.com/employees.csv"
```

These work because: the URI scheme identifies the storage provider, the file extension identifies the format, and the platform's default credential chain provides authentication. This is the zero-setup path.

### Validation

The data source implementation validates its configuration eagerly. Examples of early validation:

- S3 source: URI must have a bucket. If no credentials and no default provider detected, error before execution.
- JDBC source: connection URL must be present. If no `"url"` in WITH or registration, error.
- Iceberg source: catalog URI must be present. Table identifier must be non-empty.
- CSV format: `delimiter` must be a single character. `error_mode` must be one of the recognized values.

Validation happens during analysis, not during data reading. The user gets a clear error before any I/O begins.

---

## Examples

### Cloud object stores

```sql
-- S3 with IAM instance role (DefaultCredentialsProvider, no explicit auth needed)
EXTERNAL "s3://my-bucket/logs/2026-03/*.parquet"

-- S3 with registered source providing connectivity
EXTERNAL prod_s3 "logs/2026-03/*.parquet"

-- S3 fully inline with secret references
EXTERNAL "s3://my-bucket/logs/*.parquet" WITH {
  "region": "us-east-1",
  "secrets": { "access_key": "{{AWS_KEY}}", "secret_key": "{{AWS_SECRET}}" }
}

-- S3 with registered source + config overrides
EXTERNAL prod_s3 "logs/*.csv" WITH {
  "region": "eu-west-1",
  "delimiter": "\t",
  "error_mode": "skip_row"
}

-- GCS with Application Default Credentials (on GCE/GKE)
EXTERNAL "gs://my-bucket/events/2026-03/*.parquet"

-- GCS with registered source
EXTERNAL prod_gcs "events/*.ndjson"

-- Azure Blob with managed identity (on Azure VMs/AKS)
EXTERNAL "wasbs://logs@mystorageaccount.blob.core.windows.net/2026-03/*.parquet"

-- Azure with registered source
EXTERNAL prod_azure "2026-03/*.parquet"
```

### Public data (HTTP)

```sql
-- Public CSV, no auth
EXTERNAL "https://data.gov/datasets/population.csv"

-- Public Parquet
EXTERNAL "https://datasets.example.com/nyc-taxi/2024.parquet"

-- Authenticated HTTP endpoint
EXTERNAL "https://api.example.com/exports/report.csv" WITH {
  "secrets": { "bearer_token": "{{API_TOKEN}}" }
}
```

### Catalogs (Iceberg, Delta Lake)

```sql
-- Iceberg table via registered catalog (type=iceberg in registration)
EXTERNAL prod_iceberg "analytics.web_events"

-- Iceberg with snapshot-based time travel
EXTERNAL prod_iceberg "analytics.web_events" WITH {
  "snapshot_id": "389745023"
}

-- Iceberg with timestamp-based time travel
EXTERNAL prod_iceberg "analytics.web_events" WITH {
  "as_of": "2026-03-01T00:00:00Z"
}

-- Iceberg fully inline (DATASOURCE explicit, catalog URI + warehouse in WITH)
EXTERNAL DATASOURCE iceberg "analytics.web_events" WITH {
  "catalog_uri": "https://catalog.example.com",
  "warehouse": "s3://warehouse-bucket",
  "secrets": { "token": "{{ICEBERG_TOKEN}}" }
}

-- Delta Lake table (type=delta in registration)
EXTERNAL prod_delta "default.clickstream"

-- Delta with version
EXTERNAL prod_delta "default.clickstream" WITH { "version": 42 }
```

### SQL databases

```sql
-- PostgreSQL query via registered connection (type=jdbc in registration)
EXTERNAL prod_pg "SELECT emp_no, salary FROM employees WHERE salary > 50000"

-- MySQL via registered connection (type=jdbc in registration)
EXTERNAL prod_mysql "SELECT order_id, amount FROM orders WHERE created_at > '2026-01-01'"

-- JDBC fully inline
EXTERNAL DATASOURCE jdbc "SELECT id, name FROM users" WITH {
  "url": "postgresql://db.example.com:5432/analytics",
  "secrets": { "user": "{{PG_USER}}", "password": "{{PG_PASS}}" }
}

-- JDBC with fetch size override (type=jdbc in registration)
EXTERNAL prod_pg "SELECT * FROM large_table" WITH {
  "fetch_size": 10000
}
```

### Arrow Flight / gRPC

```sql
-- Flight connector via registered endpoint (type=flight in registration)
EXTERNAL analytics_flight "catalog.dataset"

-- Flight fully inline
EXTERNAL DATASOURCE flight "catalog.dataset" WITH {
  "endpoint": "grpc://flight.example.com:8815",
  "secrets": { "token": "{{FLIGHT_TOKEN}}" }
}
```

### Metrics and monitoring

```sql
-- Prometheus via registered connection (type=prometheus in registration)
EXTERNAL prod_prom "http_requests_total"

-- CloudWatch metrics (type=cloudwatch in registration, future)
EXTERNAL prod_aws "AWS/EC2/CPUUtilization" WITH {
  "period": 300,
  "stat": "Average"
}
```

### AWS services

```sql
-- CloudTrail logs in S3 (just blob store — no special connector needed)
EXTERNAL prod_aws "s3://my-trail/AWSLogs/*/CloudTrail/us-east-1/2026/03/**/*.json.gz"

-- CloudTrail Lake (type=cloudtrail in registration, future)
EXTERNAL prod_cloudtrail "my-event-store" WITH {
  "event_category": "Management"
}
```

### Mixed format scenarios

```sql
-- Same S3 path, different access methods
-- Raw Parquet files (auto-detected from extension)
EXTERNAL prod_s3 "warehouse/events/*.parquet"

-- Same data via Iceberg (separate registration with type=iceberg)
EXTERNAL prod_iceberg "warehouse.events"

-- .log files that are actually NDJSON (format in WITH resolves ambiguous extension)
EXTERNAL prod_s3 "app/*.log" WITH { "format": "ndjson" }

-- TSV file (tab-separated, .tsv extension auto-detects)
EXTERNAL prod_s3 "data/export.tsv"

-- .txt file that is actually tab-separated CSV (format + delimiter in WITH)
EXTERNAL prod_s3 "data/export.txt" WITH { "format": "csv", "delimiter": "\t" }
```

### Compressed files

```sql
-- Gzipped NDJSON (compression auto-detected from .gz extension)
EXTERNAL prod_s3 "logs/2026-03-11.ndjson.gz"

-- Zstandard-compressed Parquet
EXTERNAL prod_s3 "data/events.parquet.zst"

-- Bzip2-compressed CSV
EXTERNAL prod_s3 "archives/data.csv.bz2"
```

### Complexity spectrum

```sql
-- 1. Bare minimum — self-describing URI, default credentials
EXTERNAL "s3://bucket/logs/*.parquet"

-- 2. Registered source — connectivity from CRUD
EXTERNAL prod_s3 "logs/*.parquet"

-- 3. Registered source + format options
EXTERNAL prod_s3 "logs/*.csv" WITH { "delimiter": ";", "error_mode": "skip_row" }

-- 4. Ambiguous extension — format specified in WITH
EXTERNAL "s3://bucket/app/*.log" WITH { "format": "ndjson" }

-- 5. Everything inline — zero CRUD, full WITH
EXTERNAL DATASOURCE jdbc "SELECT * FROM users" WITH {
  "url": "postgresql://host:5432/db",
  "schema": "public",
  "fetch_size": 5000,
  "secrets": { "user": "{{PG_USER}}", "password": "{{PG_PASS}}" }
}
```

---

## CRUD API [draft]

External data sources are registered via a CRUD API. Registration is optional — it provides reusable connectivity configuration so queries don't repeat endpoints, credentials, and base settings.

A registered data source is analogous to registering a cluster or a database — it stores connectivity, not data. The resource (what to read) is always specified in the EXTERNAL command.

### Create / Update

```
PUT _esql/external_sources/{name}
```

```json
{
  "type": "file",
  "config": {
    "storage": "s3",
    "region": "us-east-1",
    "bucket": "my-analytics-bucket",
    "secrets": {
      "access_key": "{{AWS_KEY}}",
      "secret_key": "{{AWS_SECRET}}"
    }
  }
}
```

```json
{
  "type": "jdbc",
  "config": {
    "url": "postgresql://db.example.com:5432/analytics",
    "schema": "public",
    "secrets": {
      "user": "{{PG_USER}}",
      "password": "{{PG_PASS}}"
    }
  }
}
```

```json
{
  "type": "iceberg",
  "config": {
    "catalog_uri": "https://catalog.example.com",
    "warehouse": "s3://warehouse-bucket",
    "secrets": {
      "token": "{{ICEBERG_TOKEN}}"
    }
  }
}
```

`type` is at the root because it is a routing decision — it determines which data source implementation handles this registration. Everything else is nested under `config` because it is implementation-specific and opaque to the framework. The framework reads `type`, dispatches to the right implementation, and hands it `config` to validate and interpret.

### Get / List / Delete

```
GET _esql/external_sources/{name}
GET _esql/external_sources
DELETE _esql/external_sources/{name}
```

### Naming

Data source names must be valid unquoted identifiers (letters, digits, dashes, underscores). Names that require quoting are rejected at registration time.

---

## WITH options

WITH merges into the `config` section of the CRUD registration. Any key settable in `config` at registration time can be overridden in WITH. WITH is purely configuration — it contains no routing or identity decisions. Note: `type` cannot appear in WITH — it is a root-level routing decision set at registration time or via the DATASOURCE keyword.

### Universal options

| Option | Type | Description |
|--------|------|-------------|
| `storage` | string | Storage provider override (`"s3"`, `"gcs"`, `"azure"`, `"http"`). Only meaningful for the `file` connector. Default: inferred from URI scheme. |
| `format` | string | Document format override (`"csv"`, `"ndjson"`, `"parquet"`, `"orc"`, `"tsv"`). Only meaningful for the `file` connector. Default: inferred from file extension. |
| `error_mode` | string | `"fail_fast"`, `"skip_row"`, `"null_field"` |
| `max_errors` | long | Max error count before failing |
| `max_error_ratio` | double | Max error ratio (0.0–1.0) |
| `secrets` | object | Secret template references (`{{NAME}}` resolved at execution time) |

### Source-specific options

Passed through to the data source implementation. Examples:

- **S3**: `region`, `endpoint`
- **GCS**: `project_id`, `endpoint`
- **Azure**: `account`, `endpoint`
- **CSV**: `delimiter`, `quote`, `escape`, `comment`, `null_value`, `encoding`, `datetime_format`, `max_field_size`, `multi_value_syntax`
- **JDBC**: `url`, `schema`, `fetch_size`
- **Iceberg**: `catalog_uri`, `warehouse`, `snapshot_id`, `as_of`
- **Flight**: `endpoint`

---

## Design decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Command name | `EXTERNAL` | Separate from FROM. Avoids METADATA/NULLIFY clause conflicts. FROM table functions possible later. |
| Resource spec | Single quoted string, mandatory, opaque | Data source interprets. Parser does not inspect. Simplest grammar. |
| Source ref | Positional argument, unquoted identifier | Not in WITH. Identity/routing belongs in command structure, not config. Analogous to index name in FROM. |
| DATASOURCE position | Own syntax position, not in WITH | DATASOURCE is routing, not config. Validates early. |
| DATASOURCE values | High-level connectors only (`file`, `iceberg`, `flight`, `jdbc`) | Storage providers (s3, gcs) and formats (csv, parquet) are components within `file`, specified via `storage`/`format` in WITH. |
| DATASOURCE default | Inferred from sourceRef or resourceSpec | Auto-detection via registration or `canHandle()`. DATASOURCE explicit only when ambiguous. |
| Config merge | WITH overrides CRUD registration | Same JSON structure. User can override any registered setting per-query. |
| WITH scope | Purely configuration | No routing, no identity. Source name and type are positional in the command. |
| Credentials | Template refs `{{NAME}}` in secrets | Never raw values. Both CRUD and WITH use the same mechanism. |
| CCS colon syntax | Not used | `:` reserved for cluster prefix, `::` for selectors. No namespace separation possible. |
| Validation | Eager, at analysis time | Data source validates config completeness before any I/O. Clear errors, not runtime failures. |
| FROM table functions | Post-MVP | Desirable for mixing internal + external sources. Not needed now. |

---

## Grammar

```antlr
externalCommand
    : EXTERNAL (DATASOURCE UNQUOTED_SOURCE | UNQUOTED_SOURCE)? stringOrParameter commandNamedParameters
    ;
```

DATASOURCE and sourceRef are mutually exclusive — the grammar enforces this with an alternation. The parser sees exactly one of three shapes:

| Input | What the parser sees |
|-------|---------------------|
| `EXTERNAL "s3://bucket/*.parquet"` | No identifier. DATASOURCE inferred from resourceSpec. |
| `EXTERNAL prod_s3 "logs/*.parquet"` | Identifier without DATASOURCE keyword = sourceRef. Type from registration. |
| `EXTERNAL DATASOURCE file "app/*.log"` | DATASOURCE keyword + identifier = explicit connector. No sourceRef. |

No ambiguity at parse time. No guessing. Grammar cost: minimal — one keyword (`DATASOURCE`) + alternation on one `UNQUOTED_SOURCE`.

---

## Open for later

- FROM table functions (mixing internal + external in one query)
- EXTERNAL as binding command (`EXTERNAL name "path" | FROM name, index`)
- CRUD endpoint naming (`_esql/external_sources` vs `_query/external_sources`)
- Secrets storage mechanism (keystore vs index)
- Data source name quoting (backticks if needed)
