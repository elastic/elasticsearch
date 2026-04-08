# ES|QL|DS CRUD Design

## Requirements

ES|QL Data Sources extends ES|QL to query data stored outside Elasticsearch — in cloud object stores (S3, GCS, Azure), data catalogs (Iceberg), databases (JDBC), and streaming endpoints (Flight). Users need a way to register these external systems and their credentials, define what data to query from them, and control who can do what.

- **Manage connections** — provision, update, and delete connections including credentials
- **Query by name** — named references to external data; queries never contain credentials or paths
- **Reuse connections** — one connection serves many references; credential rotation in one place
- **Minimize boilerplate** — a short name is sufficient to identify what to query
- **Multiple system types** — S3, GCS, Azure, Iceberg, JDBC, Flight; new types addable without framework changes
- **Multi-level access control** — separate privileges for provisioning connections, defining references, and querying
- **Credentials encrypted at rest** — never stored in plaintext; never returned on GET
- **Consistent with ES conventions** — follows established ES patterns for CRUD, naming, privileges, and storage

## Mental Model

A two-tier CRUD model is proposed: **datasources** and **datasets**.

A **datasource** represents a connection to an external system — credentials, endpoint, authentication. Every datasource has an explicit `type` (`s3`, `gcs`, `azure_blob`, `iceberg`, `jdbc`, `flight`). It holds only how to connect, never what data to query. Authentication patterns include long-lived credentials, identity-based authentication (e.g., AWS AssumeRole with External ID, GCS Workload Identity Federation, Azure federated credentials), or no authentication for public resources. Identity-based flows may require additional fields (role ARN, external ID, service account email); some of these can be encrypted at rest for a stronger security posture.

A **dataset** is a new **index abstraction** that references a datasource and specifies what to query — a resource path, format settings, partitioning config. It never holds credentials.

The split keeps connectivity separate from data definition. One datasource serves many datasets. Credentials rotate without touching datasets. This is consistent with Trino, ClickHouse, Cribl, and Snowflake (which uses a three-tier model: storage integration, stage, external table).

Each datasource type is implemented as a plugin. The CRUD layer handles routing, encryption, storage, and the REST layer; the plugin validates type-specific schemas. Adding a new type requires no framework changes.

## CRUD API

Both datasources and datasets are managed under the `_query/` namespace, following the same pattern as views:

| Operation | Datasources | Datasets |
|-----------|-------------|----------|
| Create/Replace | `PUT _query/datasource/{id}` | `PUT _query/dataset/{id}` |
| Get | `GET _query/datasource/{id}` | `GET _query/dataset/{id}` |
| List | `GET _query/datasource` | `GET _query/dataset` |
| Delete | `DELETE _query/datasource/{id}` | `DELETE _query/dataset/{id}` |

IDs are user-provided names (e.g., `prod_s3_logs`, `sales_iceberg`, `prod_postgres_orders`) — the standard ES pattern used by inference endpoints, transforms, ML jobs, and 30+ other APIs. Dynamic ID generation via POST may be added in the future if needed.

The `type` field selects the datasource implementation, which validates all fields and determines which to encrypt. Dataset IDs share the same namespace as indices.

```
PUT _query/datasource/{id}
{
  "type": "<s3|gcs|azure_blob|iceberg|jdbc|flight>",
  "description": "...",
  "settings": {
    <datasource-specific fields>
  }
}
```

```
PUT _query/dataset/{id}
{
  "datasource": "<datasource_id>",
  "resource": "...",  // datasource-specific: URI, glob pattern, table name, SQL query, etc.
  "description": "...",
  "settings": {
    <dataset-specific fields>
  }
}
```

The two-layer envelope keeps `type`, `description`, and `datasource`/`resource` at the top level. All type-specific properties go under `settings`, following the snapshot repository pattern (`PUT _snapshot/repo { "type": "s3", "settings": { ... } }`). This is more client-friendly — clients can parse the envelope without knowing the type-specific schema. Datasources validate the payload schema but do not validate the connection itself (connection testing can be handled via Kibana).

### Example: S3 + Parquet

```json
PUT _query/datasource/prod_s3_logs
{
  "type": "s3",
  "description": "Production S3 logs bucket, us-east-1",
  "settings": {
    "region": "us-east-1",
    "access_key": "AKIA...",
    "secret_key": "wJal..."
  }
}

PUT _query/dataset/access_logs
{
  "datasource": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "settings": {
    "partition_detection": "hive"
  }
}
```

Additional examples (Iceberg, JDBC, GCS, Azure, Flight) in [Appendix A](#appendix-a-examples).

### Alternatives considered

**POST with server-generated IDs.** Considered but deferred — PUT with user-provided names is consistent with 30+ ES APIs.

**Flat payload.** All type-specific fields at the top level alongside `type` and `description`, without a `settings` wrapper. Simpler to validate and slightly less verbose, but harder for clients to parse generically. See [Appendix E](#appendix-e-alternative-payload-structures) for side-by-side examples.

## Storage

Both datasources and datasets are stored in **cluster state** (as `Metadata.ProjectCustom`), following the same pattern as index templates, aliases, and views. Secret fields are encrypted in cluster state — never stored in plaintext (more on this in [Security](#security)). To keep cluster state healthy, configurable limits on the number of datasource and dataset definitions should be enforced — similar to `cluster.max_indices_per_project` for indices.

### Alternatives considered

**System index storage.** Storing datasources and datasets in system indices (`.esql-datasources`, `.esql-datasource-secrets`) instead of cluster state was considered. This was decided against — system indices require I/O during query planning, which should be avoided. Cluster state is in-memory and requires no I/O at read time. Migration to system indices is straightforward if scale requires it.

## Security

For the full security design, see [ES|QL - Datasources - Security](https://docs.google.com/document/d/1P9BFEwBgliunLSPDdFmX5TNuI9Z02HpFyKoEtKuACtQ/). For the encryption design, see [Encrypted Cluster State Secrets](https://docs.google.com/document/d/1bqDmBQaeyEC_EAvjpuSOvLc_kbLyw3CygfU_4VWX6Is/).

### Authorization

#### Datasources

Datasources are not index abstractions — they hold credentials and connection details, and do not participate in the index namespace. Two options for datasource authorization:

**Option 1: Cluster-wide privileges.** Two privileges — `manage_datasources` (create, update, delete any datasource) and `read_datasources` (reference any datasource when creating a dataset). This is all-or-nothing: a user can manage all datasources or none. This is the same pattern used by inference endpoints (`manage_inference`), snapshot repositories (`create_snapshot`), transforms (`manage_transform`), and connectors (`manage_connector`).

**Option 2: Per-datasource global privileges.** Scoped to name patterns (e.g., `prod_s3_*`), with granular privileges: `create`, `delete`, `read_metadata`, `manage`, and `read`. This allows admins to grant access to specific datasources — for example, granting a team `read` on `prod_s3_*` without exposing `staging_*` datasources.

Option 1 is sufficient for Tech Preview and possibly GA. Option 2 can be added later as a non-breaking change — Elasticsearch's authorization model uses OR semantics across cluster and global privileges, so existing roles continue to work. The recommendation is to target Option 2 as the final shape, but it is acceptable to deliver Option 1 at GA if needed.

#### Datasets

Datasets are index abstractions, following the same precedent as views. They use custom index privileges for management — `create_dataset`, `delete_dataset`, `read_dataset_metadata` — and the standard `read` privilege for querying, all scoped to name patterns.

The control point is dataset creation, not query execution. Creating a dataset requires both `create_dataset` on the dataset name and a datasource privilege (cluster `read_datasources` in Option 1, or scoped `read` in Option 2) on the referenced datasource. This ensures that only authorized users can wire up new datasets to credential stores. Once a dataset exists, anyone with `read` privilege on it can query through the datasource's credentials.

Note: DLS/FLS is not enforceable on datasets (no Lucene index) and will fail at planning time if defined.

### Encryption and Credential Handling

Credentials are encrypted in cluster state using the [Encrypted Cluster State Secrets](https://docs.google.com/document/d/1bqDmBQaeyEC_EAvjpuSOvLc_kbLyw3CygfU_4VWX6Is/) design — a master key strengthened via KDF and used with authenticated encryption. Key rotation is supported through an explicit API. Credentials are write-only — GET responses mask secret fields, never returning actual values. At query time, credentials are decrypted in memory and passed to the plugin. Audit logging for credential operations (create, update, delete, decrypt) should be included.

## Querying

Today, `EXTERNAL` requires inline URIs and credentials in every query:

```
EXTERNAL "s3://logs-bucket/access/**/*.parquet"
  WITH { "access_key": "AKIA...", "secret_key": "wJal...", "region": "us-east-1" }
| WHERE status_code >= 400
| STATS error_count = COUNT(*) BY service_name
```

With CRUD in place, all connection details and credentials are stored in the datasource, and the dataset provides a named reference. The query simplifies to just `EXTERNAL access_logs` — or, since datasets are index abstractions sharing the same namespace as indices, aliases, and views, potentially just `FROM`:

```
EXTERNAL access_logs
| WHERE status_code >= 400
| STATS error_count = COUNT(*) BY service_name
```

The residual value of `EXTERNAL` is quite limited once CRUD is in place. Moving to `FROM` eliminates the distinction between internal and external data at the query level. `EXTERNAL` could be retired to a dev/migration path.

The recommendation is to target `FROM` for GA — once `EXTERNAL` ships, removing it is impossible due to backward compatibility. CRUD should be developed and integrated with `EXTERNAL` first, then the move to `FROM` should follow before GA.

## Lifecycle

**Creating a datasource**: the CRUD layer routes the request to the plugin based on `type`. The plugin validates fields and identifies secrets. The CRUD layer encrypts secrets and stores the definition in cluster state.

**Creating a dataset**: the CRUD layer resolves the referenced datasource, checks privileges, verifies no name collision with existing indices/aliases/views, and delegates resource validation to the plugin. The dataset is stored in cluster state as a new index abstraction. Resource validation also prevents injection and SSRF.

**Querying**: the planner resolves the dataset and datasource, decrypts credentials in memory, and passes them to the plugin to establish the connection. The query engine executes normally. 
See [Appendix F](#appendix-f-interaction-diagrams) for sequence diagrams.

---

## Appendix A: Examples

### S3 + Parquet

```json
PUT _query/datasource/prod_s3_logs
{
  "type": "s3",
  "description": "Production S3 logs bucket, us-east-1",
  "settings": {
    "region": "us-east-1",
    "access_key": "AKIA...",
    "secret_key": "wJal..."
  }
}

PUT _query/dataset/access_logs
{
  "datasource": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "settings": {
    "partition_detection": "hive"
  }
}
```

### S3 + NDJSON (same datasource, different dataset)

```json
PUT _query/dataset/raw_events
{
  "datasource": "prod_s3_logs",
  "resource": "s3://events-bucket/raw/**/*.ndjson.gz",
  "settings": {
    "error_mode": "skip_row",
    "max_error_ratio": 0.01
  }
}
```

### S3 + CSV with custom format (same datasource)

```json
PUT _query/dataset/legacy_export
{
  "datasource": "prod_s3_logs",
  "resource": "s3://exports-bucket/legacy/**/*.csv",
  "settings": {
    "delimiter": ";",
    "encoding": "ISO-8859-1",
    "datetime_format": "dd/MM/yyyy HH:mm:ss",
    "null_value": "N/A",
    "multi_value_syntax": "none"
  }
}
```

### GCS + ORC

```json
PUT _query/datasource/analytics_gcs_clickstream
{
  "type": "gcs",
  "description": "Analytics GCS clickstream bucket",
  "settings": {
    "project_id": "my-analytics-project",
    "credentials": "{\"type\":\"service_account\",...}"
  }
}

PUT _query/dataset/clickstream
{
  "datasource": "analytics_gcs_clickstream",
  "resource": "gs://clickstream-bucket/2026/**/*.orc",
  "settings": {
    "partition_detection": "hive"
  }
}
```

### Azure Blob + Parquet

```json
PUT _query/datasource/prod_azure_telemetry
{
  "type": "azure_blob",
  "description": "Production Azure telemetry, West Europe",
  "settings": {
    "connection_string": "DefaultEndpointsProtocol=https;AccountName=..."
  }
}

PUT _query/dataset/telemetry
{
  "datasource": "prod_azure_telemetry",
  "resource": "wasbs://telemetry-container/events/**/*.parquet"
}
```

### Iceberg on Snowflake REST catalog + S3 storage

```json
PUT _query/datasource/sales_iceberg
{
  "type": "iceberg",
  "description": "Snowflake-managed Iceberg catalog, S3 storage",
  "settings": {
    "catalog_type": "rest",
    "catalog_uri": "https://org-account.snowflakecomputing.com",
    "warehouse": "s3://my-warehouse/iceberg/",
    "region": "us-east-1",
    "catalog_token": "eyJ...",
    "access_key": "AKIA...",
    "secret_key": "wJal..."
  }
}

PUT _query/dataset/customers
{
  "datasource": "sales_iceberg",
  "resource": "db.schema.customers"
}
```

### JDBC

```json
PUT _query/datasource/prod_postgres_orders
{
  "type": "jdbc",
  "description": "Production PostgreSQL orders database",
  "settings": {
    "host": "analytics-db.internal",
    "port": 5432,
    "database": "analytics",
    "ssl": true,
    "username": "readonly",
    "password": "..."
  }
}

PUT _query/dataset/orders
{
  "datasource": "prod_postgres_orders",
  "resource": "public.orders"
}
```

### Flight

```json
PUT _query/datasource/metrics_flight
{
  "type": "flight",
  "description": "Metrics Arrow Flight endpoint",
  "settings": {
    "host": "flight.internal",
    "port": 47470
  }
}

PUT _query/dataset/realtime_metrics
{
  "datasource": "metrics_flight",
  "resource": "metrics_stream"
}
```

## Appendix B: Datasource Type Schemas

Each datasource type defines its fields, which are required, and which are secrets (encrypted, never returned on GET). Credentials are optional for all cloud storage types — when omitted, the default credential chain applies (IAM roles, environment variables, instance profiles, managed identity, Application Default Credentials).

### S3

| Field | Required | Secret | Description |
|-------|----------|--------|-------------|
| `region` | No | No | AWS region (default: `us-east-1`) |
| `endpoint` | No | No | Custom S3 endpoint (MinIO, etc.) |
| `access_key` | No | Yes | AWS access key ID |
| `secret_key` | No | Yes | AWS secret access key |
| `auth` | No | No | `"none"` for anonymous access to public buckets |

### GCS

| Field | Required | Secret | Description |
|-------|----------|--------|-------------|
| `project_id` | No | No | GCP project |
| `endpoint` | No | No | Custom GCS endpoint |
| `token_uri` | No | No | Override OAuth2 token URI |
| `credentials` | No | Yes | Service account JSON (full content) |
| `auth` | No | No | `"none"` for anonymous access to public buckets |

### Azure Blob

| Field | Required | Secret | Description |
|-------|----------|--------|-------------|
| `endpoint` | No | No | Custom endpoint |
| `account` | No | No | Storage account name |
| `connection_string` | No | Yes | Full Azure connection string |
| `key` | No | Yes | Shared access key (use with `account`) |
| `sas_token` | No | Yes | SAS token |
| `auth` | No | No | `"none"` for anonymous access to public containers |

Multiple auth methods — provide one of `connection_string`, `account`+`key`, or `sas_token`. When all omitted, uses `DefaultAzureCredential`.

### Iceberg

Currently supports S3-backed Hadoop catalogs only. Additional fields for REST/Glue/Hive catalogs are planned.

| Field | Required | Secret | Description |
|-------|----------|--------|-------------|
| `region` | No | No | AWS region |
| `endpoint` | No | No | Custom S3 endpoint |
| `access_key` | No | Yes | Storage credentials |
| `secret_key` | No | Yes | Storage credentials |

Planned fields for REST catalog support: `catalog_type`, `catalog_uri`, `warehouse`, `catalog_token`.

### JDBC

| Field | Required | Secret | Description |
|-------|----------|--------|-------------|
| `host` | Yes | No | Database host |
| `port` | Yes | No | Database port |
| `database` | Yes | No | Database name |
| `ssl` | No | No | Use TLS (default: false) |
| `username` | No | Yes | Database username |
| `password` | No | Yes | Database password |

### Flight

| Field | Required | Secret | Description |
|-------|----------|--------|-------------|
| `host` | Yes | No | gRPC host |
| `port` | No | No | gRPC port (default: 47470) |

## Appendix C: Dataset Fields and Settings


| Field | Required | Description |
|-------|----------|-------------|
| `datasource` | Yes | Reference to a datasource by ID |
| `resource` | Yes | What to query — URI, glob, table name, or SQL query. Opaque to framework, validated by the datasource plugin. |
| `description` | No | Human-readable |

All remaining fields are optional and validated by the datasource plugin:

**Framework fields (all file-based datasources):**

| Field | Default | Description |
|-------|---------|-------------|
| `format` | auto-detect from extension | Override format detection (`"parquet"`, `"csv"`, `"ndjson"`, `"orc"`) |
| `error_mode` | `fail_fast` | Error handling: `"fail_fast"`, `"skip_row"`, `"null_field"` |
| `max_errors` | ∞ (when skip_row) | Max error count before abort |
| `max_error_ratio` | 0.0 | Max fraction of rows (0.0-1.0) that may error |
| `partition_detection` | `auto` | `"auto"`, `"hive"`, `"template"`, `"none"` |
| `partition_path` | null | Template for template-based partition extraction |
| `hive_partitioning` | `true` | Enable/disable Hive-style partition detection |

**Parquet**: No user-configurable dataset fields. Row group coalescing and filter pushdown are automatic.

**NDJSON fields:**

| Field | Default | Description |
|-------|---------|-------------|
| `datetime_format` | `strict_date_optional_time` | Custom date/time pattern for parsing date strings. |

**CSV/TSV fields:**

| Field | Default (CSV / TSV) | Description |
|-------|---------------------|-------------|
| `delimiter` | `,` / `\t` | Field separator character |
| `quote` | `"` | Quoting character |
| `escape` | `\` | Escape character inside quoted fields |
| `comment` | `//` | Line comment prefix |
| `null_value` | `""` (empty) | String representation of null |
| `encoding` | `UTF-8` | Character encoding |
| `datetime_format` | null (ISO-8601/epoch) | Custom date/time pattern |
| `max_field_size` | 10485760 (10 MB) | Max bytes per field (OOM protection) |
| `multi_value_syntax` | `brackets` | `"brackets"` (`[a,b,c]`) or `"none"` |

**ORC**: No user-configurable dataset fields. Filter pushdown and type conversion are automatic.

## Appendix D: GET Response Shapes

Secret fields are masked in all GET responses — actual values are never returned.

### GET `_query/datasource/prod_s3_logs`

```json
{
  "datasource_id": "prod_s3_logs",
  "type": "s3",
  "description": "Production S3 logs bucket, us-east-1",
  "settings": {
    "region": "us-east-1"
  }
}
```

Secret fields are masked in all GET responses — actual values are never returned.

### GET `_query/datasource` (list)

```json
{
  "datasources": [
    { "datasource_id": "prod_s3_logs", "type": "s3", "description": "Production S3 logs" },
    { "datasource_id": "sales_iceberg", "type": "iceberg", "description": "Sales Iceberg catalog" }
  ]
}
```

### GET `_query/dataset/access_logs`

```json
{
  "dataset_id": "access_logs",
  "datasource": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "description": "Production access logs",
  "settings": {
    "partition_detection": "hive"
  }
}
```

### GET `_query/dataset` (list)

```json
{
  "datasets": [
    { "dataset_id": "access_logs", "datasource": "prod_s3_logs", "resource": "s3://logs-bucket/access/**/*.parquet" },
    { "dataset_id": "customers", "datasource": "sales_iceberg", "resource": "db.schema.customers" }
  ]
}
```

### PUT response

`201 Created` if new, `200 OK` if updated (following ent-search connector precedent). Response body returns the created/updated resource (with secrets omitted for datasources).

## Appendix E: Alternative Payload Structures

Three payload layouts were considered. **Option 2: Settings bag** was chosen — it follows the snapshot repository pattern and is more client-friendly.

### Option 1: Flat

All fields at the top level. The `type` field determines which fields are valid and which are secrets. Simpler but harder for clients to parse generically.

**S3 + Parquet**

```json
PUT _query/datasource/prod_s3_logs
{
  "type": "s3",
  "description": "Production S3 logs bucket, us-east-1",
  "region": "us-east-1",
  "access_key": "AKIA...",
  "secret_key": "wJal..."
}

PUT _query/dataset/access_logs
{
  "datasource": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "partition_detection": "hive"
}
```

**Iceberg**

```json
PUT _query/datasource/sales_iceberg
{
  "type": "iceberg",
  "description": "Snowflake-managed Iceberg catalog, S3 storage",
  "catalog_type": "rest",
  "catalog_uri": "https://org-account.snowflakecomputing.com",
  "warehouse": "s3://my-warehouse/iceberg/",
  "region": "us-east-1",
  "catalog_token": "eyJ...",
  "access_key": "AKIA...",
  "secret_key": "wJal..."
}

PUT _query/dataset/customers
{
  "datasource": "sales_iceberg",
  "resource": "db.schema.customers"
}
```

### Option 2: Settings bag (chosen)

`type` and `description` at the top level, everything type-specific nested inside `settings`. Follows the snapshot repository pattern (`PUT _snapshot/repo { "type": "s3", "settings": { ... } }`). More client-friendly — clients can parse the envelope without knowing the type-specific schema.

**S3 + Parquet**

```json
PUT _query/datasource/prod_s3_logs
{
  "type": "s3",
  "description": "Production S3 logs bucket, us-east-1",
  "settings": {
    "region": "us-east-1",
    "access_key": "AKIA...",
    "secret_key": "wJal..."
  }
}

PUT _query/dataset/access_logs
{
  "datasource": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "settings": {
    "partition_detection": "hive"
  }
}
```

**Iceberg**

```json
PUT _query/datasource/sales_iceberg
{
  "type": "iceberg",
  "description": "Snowflake-managed Iceberg catalog, S3 storage",
  "settings": {
    "catalog_type": "rest",
    "catalog_uri": "https://org-account.snowflakecomputing.com",
    "warehouse": "s3://my-warehouse/iceberg/",
    "region": "us-east-1",
    "catalog_token": "eyJ...",
    "access_key": "AKIA...",
    "secret_key": "wJal..."
  }
}

PUT _query/dataset/customers
{
  "datasource": "sales_iceberg",
  "resource": "db.schema.customers"
}
```

### Option 3: Grouped by category

Type-specific fields grouped into meaningful sections — for example, `connection` for endpoint/region, `authentication` for credentials, `catalog` for catalog-specific settings. The grouping is driven by usability: related fields are visually together, making payloads easier to read and fill out.

**S3 + Parquet**

```json
PUT _query/datasource/prod_s3_logs
{
  "type": "s3",
  "description": "Production S3 logs bucket, us-east-1",
  "connection": {
    "region": "us-east-1"
  },
  "authentication": {
    "access_key": "AKIA...",
    "secret_key": "wJal..."
  }
}

PUT _query/dataset/access_logs
{
  "datasource": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "format": {
    "partition_detection": "hive"
  }
}
```

**Iceberg**

```json
PUT _query/datasource/sales_iceberg
{
  "type": "iceberg",
  "description": "Snowflake-managed Iceberg catalog, S3 storage",
  "catalog": {
    "catalog_type": "rest",
    "catalog_uri": "https://org-account.snowflakecomputing.com",
    "catalog_token": "eyJ..."
  },
  "connection": {
    "warehouse": "s3://my-warehouse/iceberg/",
    "region": "us-east-1"
  },
  "authentication": {
    "access_key": "AKIA...",
    "secret_key": "wJal..."
  }
}

PUT _query/dataset/customers
{
  "datasource": "sales_iceberg",
  "resource": "db.schema.customers"
}
```

## Appendix F: Interaction Diagrams

### Creating a Datasource

```
  User                CRUD                Plugin              State
   |                   |                   |                   |
   |--- PUT ---------->|                   |                   |
   |                   |--- validate ----->|                   |
   |                   |<-- fields ok -----|                   |
   |                   |<-- secret ids ----|                   |
   |                   |--- encrypt + store ------------------>|
   |<-- 201 -----------|                   |                   |
```

### Creating a Dataset

```
  User                CRUD                Plugin              State
   |                   |                   |                   |
   |--- PUT ---------->|                   |                   |
   |                   |--- resolve datasource --------------->|
   |                   |    check privileges                   |
   |                   |    check name collision               |
   |                   |--- validate resource->|               |
   |                   |<-- resource ok -------|               |
   |                   |--- store ---------------------------->|
   |<-- 201 -----------|                   |                   |
```

### Querying

```
  User         Planner             CRUD              Plugin          Engine
   |               |                |                   |               |
   |--- query ---->|                |                   |               |
   |               |--- resolve --->|                   |               |
   |               |<-- dataset + datasource            |               |
   |               |--- decrypt --->|                   |               |
   |               |<-- decrypted creds                 |               |
   |               |--- build ----->|------------------>|               |
   |               |                |<-- connection -----|               |
   |               |--- execute --->|------------------>|-------------->|
   |<-- results ---|                |                   |               |
```

## Future Work

- **`FROM` integration** — if datasets land in `FROM`, retire `EXTERNAL` to a dev/migration path
- **Per-datasource global privileges** (Option 2) — scoped authorization for multi-tenant deployments
- **`_update` endpoint** — partial updates to datasource/dataset definitions (currently PUT-only)
- **POST with server-generated IDs** — if demand arises for dynamic ID generation

## References

* [1] ES|QL Datasources — MVP Roadmap: https://github.com/elastic/esql-planning/issues/189
* [2] ES|QL Datasources — CRUD API Implementation: https://github.com/elastic/esql-planning/issues/287
* [3] ES|QL - Datasources - Security (Johannes Fredén): https://docs.google.com/document/d/1P9BFEwBgliunLSPDdFmX5TNuI9Z02HpFyKoEtKuACtQ
* [4] Encrypted Cluster State Secrets (Johannes Fredén): https://docs.google.com/document/d/1bqDmBQaeyEC_EAvjpuSOvLc_kbLyw3CygfU_4VWX6Is
* [5] ES|QL Datasources — Main PR: https://github.com/elastic/elasticsearch/pull/141678
* [6] Plain text secrets in indices: https://docs.google.com/document/d/1p_IS-5y4T2eZE_dkPp5HMS647DO580Uo1s0LkEGMwjM
* [7] Manage Roles Privilege design: https://docs.google.com/document/d/1VN73C2KpmvvOW85-XGUqMmnMwXrfK4aoxRtG8tPqk7Y
* [8] Inference secrets storage PR: https://github.com/elastic/elasticsearch/pull/100148
* [9] AWS cross-account access — External ID: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
* [10] GCS Workload Identity Federation: https://cloud.google.com/iam/docs/workload-identity-federation
* [11] Azure Federated Identity Credentials: https://learn.microsoft.com/en-us/entra/workload-id/workload-identity-federation
```