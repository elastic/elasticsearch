# ES|QL Data Sources CRUD API Design

## Two-Tier Model

| Object | What it represents | ES analog |
|--------|-------------------|-----------|
| **Datasource** | All credentials and connectivity — how to authenticate and connect | Inference endpoint, ent-search connector |
| **Dataset** | What data to query — index-like abstraction, references a datasource | Index, data stream, alias, view |

A single datasource can serve many datasets. A dataset always references exactly one datasource. All credentials and connectivity information lives on the datasource — nothing on the dataset.

## Datasource Types

Every datasource has an explicit `type`. Each type has a predefined schema with known fields. The server knows which fields are secrets based on the type definition — no separate `credentials` section needed.

| Type | What it connects to | Secret fields (encrypted, never returned on GET) | Config fields (readable) |
|------|--------------------|-------------------------------------------------|--------------------------|
| `s3` | Amazon S3 | `access_key`, `secret_key` | `region`, `endpoint` |
| `gcs` | Google Cloud Storage | `service_account_json` | `project_id`, `endpoint` |
| `azure` | Azure Blob Storage | `connection_string` or `account`+`key` or `sas_token` | `endpoint` |
| `iceberg` | Iceberg catalog | `catalog_token`, `access_key`, `secret_key` | `catalog_type`, `catalog_uri`, `warehouse`, `region` |
| `jdbc` | Database (future) | `username`, `password` | `host`, `port`, `database`, `ssl` |
| `flight` | Arrow Flight (future) | (future: tokens) | `host`, `port` |

All types also support:
- `description` — human-readable text
- `auth: "none"` — anonymous / default credential chain (where applicable)

## Dataset Properties

| Field | Required | Description |
|-------|----------|-------------|
| `datasource` | Yes | Reference to datasource by ID |
| `resource` | Yes | URI, glob, table name, or SQL query — opaque to framework |
| `description` | No | Human-readable text |
| `settings` | No | Read-time settings (see below) |

Settings (all optional):

| Category | Properties |
|----------|-----------|
| Format | `format` (auto-detected from extension, or explicit override) |
| Format-specific (CSV) | `delimiter`, `quote`, `escape`, `comment`, `null_value`, `encoding`, `datetime_format`, `max_field_size`, `multi_value_syntax` |
| Error handling | `error_mode`, `max_errors`, `max_error_ratio` |
| Partitioning | `partition_detection`, `partition_path`, `hive_partitioning` |

Parquet, ORC, and NDJSON have zero user-configurable format properties today.

## API Design

### Datasources

| Operation | Method | Path | Semantics | Transport Action |
|-----------|--------|------|-----------|-----------------|
| Create | `PUT` | `_query/datasource/{id}` | Full create/replace | `cluster:admin/xpack/datasource/put` |
| Get one | `GET` | `_query/datasource/{id}` | Secret fields omitted | `cluster:admin/xpack/datasource/get` |
| List all | `GET` | `_query/datasource` | Summary list | `cluster:admin/xpack/datasource/list` |
| Partial update | `PUT` | `_query/datasource/{id}/_update` | Merge — absent secrets preserved | `cluster:admin/xpack/datasource/update` |
| Delete | `DELETE` | `_query/datasource/{id}` | Fails if datasets reference it | `cluster:admin/xpack/datasource/delete` |

PUT = full replace (following ingest pipelines, inference precedent). `_update` = merge (following inference, transforms, ML precedent). Absent secret fields on `_update` means "keep existing" — allows updating config without re-supplying credentials.

### Datasets

| Operation | Method | Path | Semantics | Transport Action |
|-----------|--------|------|-----------|-----------------|
| Create | `PUT` | `_query/dataset/{id}` | Full create/replace | `cluster:admin/xpack/dataset/put` |
| Get one | `GET` | `_query/dataset/{id}` | Full definition | `cluster:admin/xpack/dataset/get` |
| List all | `GET` | `_query/dataset` | Summary list | `cluster:admin/xpack/dataset/list` |
| Partial update | `PUT` | `_query/dataset/{id}/_update` | Merge | `cluster:admin/xpack/dataset/update` |
| Delete | `DELETE` | `_query/dataset/{id}` | | `cluster:admin/xpack/dataset/delete` |

Dataset IDs share namespace with indices — creation fails if an index/alias/data stream/view with the same name exists. PUT returns `201 Created` if new, `200 OK` if updated (following ent-search connector precedent).

### Alternative: POST with server-generated IDs

As an alternative to PUT with user-provided names, we can also support POST with auto-generated IDs (following the connectors pattern which supports both):

```
POST _query/datasource
{ "type": "s3", ... }
→ 201 Created, response includes generated "datasource_id": "abc123"
```

PUT with user-provided names is the primary path (consistent with 32+ ES APIs — inference, transforms, ML, pipelines, templates, etc.). POST with auto-ID is optional.

## Examples

### S3 + Parquet

```json
PUT _query/datasource/prod_aws
{
  "type": "s3",
  "description": "Production AWS, us-east-1",
  "region": "us-east-1",
  "access_key": "AKIA...",
  "secret_key": "wJal..."
}

PUT _query/dataset/access_logs
{
  "datasource": "prod_aws",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "settings": { "partition_detection": "hive" }
}
```

Query: `FROM access_logs | WHERE status >= 400 | STATS count() BY region`

### Same S3 datasource → NDJSON dataset

```json
PUT _query/dataset/raw_events
{
  "datasource": "prod_aws",
  "resource": "s3://events-bucket/raw/**/*.ndjson.gz",
  "settings": { "error_mode": "skip_row", "max_error_ratio": 0.01 }
}
```

### Same S3 datasource → CSV with custom format

```json
PUT _query/dataset/legacy_export
{
  "datasource": "prod_aws",
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
PUT _query/datasource/analytics_gcs
{
  "type": "gcs",
  "description": "Analytics GCS project",
  "project_id": "my-analytics-project",
  "service_account_json": "{\"type\":\"service_account\",...}"
}

PUT _query/dataset/clickstream
{
  "datasource": "analytics_gcs",
  "resource": "gs://clickstream-bucket/2026/**/*.orc",
  "settings": { "partition_detection": "hive" }
}
```

### Azure + Parquet

```json
PUT _query/datasource/azure_datalake
{
  "type": "azure",
  "description": "Azure Data Lake, West Europe",
  "connection_string": "DefaultEndpointsProtocol=https;AccountName=..."
}

PUT _query/dataset/telemetry
{
  "datasource": "azure_datalake",
  "resource": "wasbs://telemetry-container/events/**/*.parquet"
}
```

### Iceberg on Snowflake REST catalog + S3 storage

```json
PUT _query/datasource/iceberg_warehouse
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
  "datasource": "iceberg_warehouse",
  "resource": "db.schema.customers"
}
```

Query: `FROM customers | WHERE country == "US" | LIMIT 1000`

### JDBC (future)

```json
PUT _query/datasource/postgres_analytics
{
  "type": "jdbc",
  "description": "Analytics PostgreSQL",
  "host": "analytics-db.internal",
  "port": 5432,
  "database": "analytics",
  "ssl": true,
  "username": "readonly",
  "password": "..."
}

PUT _query/dataset/orders
{
  "datasource": "postgres_analytics",
  "resource": "public.orders"
}
```

Query: `FROM orders | WHERE total > 100 | STATS sum(total) BY customer_id`

### Flight (future)

```json
PUT _query/datasource/flight_server
{
  "type": "flight",
  "description": "Internal Arrow Flight server",
  "host": "flight.internal",
  "port": 47470
}

PUT _query/dataset/realtime_metrics
{
  "datasource": "flight_server",
  "resource": "metrics_stream"
}
```

## GET Responses

### GET `_query/datasource/{id}`

Secret fields are omitted — not masked, not redacted, simply absent:

```json
{
  "datasource_id": "prod_aws",
  "type": "s3",
  "description": "Production AWS, us-east-1",
  "region": "us-east-1",
  "credentials_configured": true
}
```

### GET `_query/datasource` (list)

```json
{
  "datasources": [
    { "datasource_id": "prod_aws", "type": "s3", "description": "Production AWS", "credentials_configured": true },
    { "datasource_id": "iceberg_warehouse", "type": "iceberg", "description": "Iceberg warehouse", "credentials_configured": true }
  ]
}
```

### GET `_query/dataset/{id}`

```json
{
  "dataset_id": "access_logs",
  "datasource": "prod_aws",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "description": "Production access logs",
  "settings": { "partition_detection": "hive" }
}
```

### Partial update without re-supplying secrets

```json
PUT _query/datasource/prod_aws/_update
{
  "region": "eu-west-1"
}
```

Region changes. Existing encrypted credentials preserved.

## Querying

Since datasets are index abstractions, they naturally land in `FROM`:

```
FROM access_logs | WHERE status >= 400 | STATS count() BY region
```

`FROM my_dataset` works exactly like `FROM my_index`. With CRUD in place, `EXTERNAL` would become just `EXTERNAL my_dataset` (no inline URIs or credentials), which begs the question of whether it's still useful. We should consider going straight to `FROM`, retiring `EXTERNAL` to a dev/migration path.

## Privilege Model

### Datasource privileges (global/configurable cluster privileges)

Datasource access is controlled via **global privileges** — configurable cluster privileges scoped to datasource name patterns. This follows the same mechanism as `ManageRolesPrivilege` and provides per-datasource access control from day one.

```json
PUT /_security/role/dataset_creator
{
  "global": {
    "datasource": {
      "names": ["prod_aws*"],
      "privileges": ["read"]
    }
  },
  "indices": [{
    "names": ["logs-*"],
    "privileges": ["manage"]
  }]
}
```

| Privilege | What it grants |
|-----------|---------------|
| `create` | Create a new datasource matching the name pattern |
| `delete` | Delete a datasource matching the name pattern |
| `read_metadata` | View datasource config (secret fields omitted) |
| `manage` | Full CRUD on matching datasources, including credentials |
| `read` | Reference the datasource when creating a dataset |

### Dataset privileges (index-level)

Datasets are index abstractions — they use existing index privileges:

| Privilege | What it grants |
|-----------|---------------|
| `read` (existing) | Query via `FROM dataset_name` |
| `view_index_metadata` (existing) | See dataset exists, view settings |
| `manage` (existing) | Create/update/delete the dataset |

### Cross-resource authorization

Creating a dataset requires both:
1. Index `manage` on the dataset name pattern (to create the index abstraction)
2. Global datasource `read` on the referenced datasource name (to verify the datasource exists and the user is allowed to use it)

### Built-in roles

| Role | Global (datasource) | Index | Use case |
|------|---------------------|-------|----------|
| `esql_datasource_admin` | `manage` on `*` | `manage` on `*` | Full admin |
| `esql_datasource_user` | `read_metadata` on `*` | `read` on dataset patterns | Query only |
| `esql_dataset_manager` | `read` on specific patterns | `manage` on specific patterns | Create/manage datasets using allowed datasources |

### DLS/FLS on datasets

Not enforceable (no Lucene index). Query **fails** at planning time if a role defines DLS/FLS on a dataset name pattern:

> "Document-level security and field-level security are not supported on external datasets."

## Storage Architecture

### Option A: Everything in cluster state (preferred)

```
Cluster State (Metadata.ProjectCustom)
  ├── DatasetMetadata
  │     └── dataset_id → { datasource_ref, resource, settings, description }
  │
  └── DatasourceMetadata
        └── datasource_id → { type, fields... }
            Secret fields encrypted (AES-256-GCM) before storage.
            Cluster state holds ciphertext. At query time, decrypted
            into SecureString (char[], zeroed on close()).
```

**Why this works:** Cluster state holds encrypted ciphertext — useless without the key. At query time, decrypt into `SecureString` → use → zero. `SecureString` implements `Releasable` — works with try-with-resources.

**Advantages:** Synchronous access, simpler implementation, atomic updates, same pattern as ingest pipelines/templates/aliases.

**Disadvantages:** Encrypted credentials on all nodes (~1KB/datasource — negligible at expected scale).

### Option B: Hybrid (alternative)

Datasets in cluster state, datasources + secrets in system indices. More complex, async lookups. Better if scale exceeds hundreds of datasources. API surface doesn't change — migration from A to B is transparent.

### Recommendation: Option A

## Cribl Comparison

| Aspect | Cribl | Our design |
|--------|-------|-----------|
| Provider endpoint | `POST /search/dataset-providers` | `PUT _query/datasource/{id}` |
| Dataset endpoint | `POST /search/datasets` | `PUT _query/dataset/{id}` |
| Type | Always required (dropdown) | Always required (`s3`, `gcs`, `azure`, `iceberg`, `jdbc`, `flight`) |
| Schema | Predefined fields per type | Predefined fields per type |
| Credentials | Inline, not encrypted | Flat fields, encrypted at rest |
| ID | In body, server can auto-generate | In URL (PUT) or auto-generated (POST) |
| Update | PATCH (full replace despite name) | `_update` (true merge, secrets preserved) |
| One datasource → many datasets | Yes | Yes |
| Ad-hoc query | No (must pre-register) | `FROM` resolves through CRUD |
| Query syntax | `dataset="access_logs"` | `FROM access_logs` |

## Decisions Log

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| 1 | URL namespace | `_query/datasource` + `_query/dataset` | Broader than `_esql` — future-proofs for SQL |
| 2 | Type on datasource | Always required. `s3`, `gcs`, `azure`, `iceberg`, `jdbc`, `flight`. | Per Alex's feedback — explicit, no inference magic |
| 3 | Field structure | Flat predefined fields per type. Server knows which are secrets from type schema. | Per Alex's feedback — "proper objects with predefined fields" |
| 4 | Secrets in API | Flat in PUT body, server detects and encrypts based on type. GET omits secret fields. | No opaque `config`/`credentials` bags |
| 5 | Storage | Cluster state (Option A). Encrypted credentials via AES-256-GCM, SecureString at query time. | Simpler, synchronous, safe at expected scale |
| 6 | Dataset privileges | Existing index-level (`read`/`manage`) | Datasets are index abstractions |
| 7 | Datasource privileges | Global/configurable cluster privileges, scoped to name patterns | Per Johannes' proposal. Same mechanism as `ManageRolesPrivilege`. |
| 8 | EXTERNAL vs FROM | Both kept, both resolve through CRUD. Consider retiring EXTERNAL. | No inline credentials. Syntax decision deferred. |
| 9 | ID generation | PUT with user-provided name (primary). POST with auto-ID (alternative). | PUT consistent with 32+ ES APIs. POST available for convenience. |
| 10 | Update semantics | `PUT` = full replace, `_update` = merge (secrets preserved if absent) | Follows inference precedent |
| 11 | Composable datasources | Long-term goal, not MVP | Iceberg referencing cloud storage datasource for S3 creds |
| 12 | Credential scope | All credentials + connectivity on datasource. Nothing on dataset. | |

## Open Questions

1. **IAM role assumption**: Should `auth_mode: "assume_role"` with `role_arn` be a datasource auth option? Eliminates stored credentials for cloud storage. Post-MVP candidate.

2. **Schema caching on datasets**: Should the dataset store a cached schema to avoid re-inferring on every query? Tracked as #286.

3. **Datasource deletion guard**: Prevent deleting a datasource that has datasets referencing it? Recommendation: prevent, require explicit dataset cleanup first.

4. **Discovery endpoint**: `GET _query/datasource/_types` listing available types and their field schemas. Useful for Kibana dynamic forms. Parked for later.
