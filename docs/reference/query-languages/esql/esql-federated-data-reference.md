---
navigation_title: "Reference"
description: "API reference, settings, schema discovery, and limits for {{esql}} federated data sources and datasets."
applies_to:
  stack: preview =9.5
  serverless: preview
products:
  - id: elasticsearch
---

# {{esql}} federated data reference

This page covers the full API surface, settings, and operational details for {{esql}} federated data sources and datasets. For an introduction to these concepts, refer to [{{esql}} federated data](esql-federated-data.md). For a quickstart guide, refer to [](esql-federated-data-quickstart.md).

## Mental model

Federated data has four layers:

- **Data source:** where and how {{es}} connects. Data source settings configure the external system, endpoint, region, and authentication.
- **Dataset:** what to read. Dataset definitions choose a data source and identify the resource, such as an S3 path or glob.
- **Compression:** how bytes are unwrapped. For supported text formats, compression is inferred from the resource extension before parsing.
- **Format reader:** how records are parsed. Each reader, such as Parquet, CSV, TSV, or NDJSON, has format-specific behavior and settings.

The API sections below follow this model: data source APIs manage connections, dataset APIs manage queryable resources, and dataset settings configure compression and format-reader behavior.

## Supported data source types

The following data source types are supported:

:::{include} _snippets/federated-data/supported-data-source-types.md
:::

## Supported file formats

Federated data sources can read the following file formats:

:::{include} _snippets/federated-data/supported-file-formats.md
:::

The format is detected automatically from the file extension. You can override this in the [dataset settings](#common-settings).

### Text formats

| Format | Recognized extensions |
|---|---|
| CSV | `.csv` |
| TSV | `.tsv` |
| NDJSON | `.ndjson`, `.jsonl`, `.json` |

### Compression for text formats

A text resource is read uncompressed, or compressed with a codec identified from a trailing extension: `clicks.csv`, `clicks.csv.gz`, `clicks.csv.zst`.

| Codec | Extensions |
|---|---|
| uncompressed | — |
| gzip | `.gz`, `.gzip` |
| zstd | `.zst`, `.zstd` |

### Parquet

Parquet declares its compression internally, per column chunk, so Parquet resources are not externally compressed. They are recognized by the `.parquet` and `.parq` extensions. Whether a file can be read depends on the codec its writer used for its column chunks:

| Parquet codec | Status |
|---|---|
| UNCOMPRESSED | Read |
| SNAPPY | Read |
| ZSTD | Read |
| GZIP | Read |

SNAPPY, ZSTD, and GZIP account for the overwhelming majority of Parquet in practice, so the supported set covers nearly all real files.

<!-- TODO: Once the data source and dataset APIs are defined in elasticsearch-specification,
replace the inline examples below with a summary table linking to the generated
API reference at https://www.elastic.co/docs/api/doc/elasticsearch/ -->

## Data source API

Data sources are managed under the `/_query/data_source` endpoint. All data source operations require the cluster `manage` privilege or a `global.data_source` privilege (refer to [security](#security)).

| Operation | Endpoint |
|---|---|
| [Create or update](#create-or-update-a-data-source) | `PUT /_query/data_source/{name}` |
| [Get](#get-a-data-source) | `GET /_query/data_source/{name}` |
| [List all](#list-all-data-sources) | `GET /_query/data_source` |
| [Delete](#delete-a-data-source) | `DELETE /_query/data_source/{name}` |

### Create or update a data source

`PUT` creates a new data source or replaces an existing one entirely. The create call does not validate connectivity to the external system. To verify that credentials and endpoint are correct, create a dataset that references the data source and query it.

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
PUT /_query/data_source/prod_s3_logs
{
  "type": "s3",
  "description": "Production S3 logs bucket, us-east-1",
  "settings": {
    "region": "us-east-1",
    "access_key": "<AWS_ACCESS_KEY_ID>",
    "secret_key": "<AWS_SECRET_ACCESS_KEY>"
  }
}
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/data_source/prod_s3_logs" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "type": "s3",
  "description": "Production S3 logs bucket, us-east-1",
  "settings": {
    "region": "us-east-1",
    "access_key": "<AWS_ACCESS_KEY_ID>",
    "secret_key": "<AWS_SECRET_ACCESS_KEY>"
  }
}'
```
:::

::::

### Get a data source

Retrieves a data source by name. Credential values are replaced by `::es_redacted::` in the response.

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
GET /_query/data_source/prod_s3_logs
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/data_source/prod_s3_logs" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

### List all data sources

Returns all registered data sources.

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
GET /_query/data_source
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/data_source" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

### Delete a data source

Deletes a data source by name.

:::{important}
A data source cannot be deleted while datasets still reference it. Delete the dependent datasets first, or the request returns a `409 Conflict` error.
:::

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
DELETE /_query/data_source/prod_s3_logs
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X DELETE "${ELASTICSEARCH_URL}/_query/data_source/prod_s3_logs" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

## Data source settings

### S3

The following settings are available for `s3` data sources:

**Connection settings:**

| Setting | Required | Description |
|---|---|---|
| `region` | No | The bucket region. |
| `endpoint` | No | An explicit endpoint, for an S3-compatible store. |

**Authentication settings:**

| Setting | Required | Description |
|---|---|---|
| `access_key` | No | AWS access key ID. |
| `secret_key` | No | AWS secret access key. |
| `session_token` | No | Session token for temporary STS credentials. Use with `access_key` and `secret_key`. |
| `auth` | No | Set to `"none"` for anonymous access to public buckets, or `"workload_identity"` to use the node's cloud identity. |

<!-- NOTE: The scope doc uses "workload_identity" but the code shipped "ambient".
     Reconcile with eng before GA. The cluster setting is esql.datasource.ambient_credentials.enabled. -->

## Authentication

A data source authenticates to its store with one of the models below. The models are mutually exclusive on a data source.

| Model | Description |
|---|---|
| Static credentials | A fixed access key and secret key, supplied in the data source settings. The common form for a service account. |
| Anonymous | `auth: "none"`. For public data that needs no credentials. |
| Workload identity | `auth: "workload_identity"`. Keyless, using the node's own cloud identity. For single-cloud, single-tenant deployments. Requires `esql.datasource.workload_identity.enabled: true` (operator-only). Not available in serverless. |

When `access_key` and `secret_key` are omitted and `auth` is not set, {{es}} uses the default AWS credential chain: IAM roles, environment variables, or instance profiles.

## Dataset API

Datasets are managed under the `/_query/dataset` endpoint. All dataset operations require the index `manage` privilege on the dataset name, or a fine-grained dataset privilege (refer to [security](#security)).

| Operation | Endpoint |
|---|---|
| [Create or update](#create-or-update-a-dataset) | `PUT /_query/dataset/{name}` |
| [Get](#get-a-dataset) | `GET /_query/dataset/{name}` |
| [List all](#list-all-datasets) | `GET /_query/dataset` |
| [Delete](#delete-a-dataset) | `DELETE /_query/dataset/{name}` |

### Create or update a dataset

`PUT` creates a new dataset or replaces an existing one entirely.

:::{important}
A dataset cannot have the same name as an existing index, alias, or view, because dataset names share the same namespace. Dataset names must be lowercase and cannot begin with `-`, `_`, or `+`.
:::

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
PUT /_query/dataset/access_logs
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "description": "Production access logs",
  "settings": {
    "partition_detection": "hive"
  }
}
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/_query/dataset/access_logs" \
  -H "Authorization: ApiKey ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "description": "Production access logs",
  "settings": {
    "partition_detection": "hive"
  }
}'
```
:::

::::

### Get a dataset

Retrieves a dataset by name.

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
GET /_query/dataset/access_logs
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/dataset/access_logs" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

### List all datasets

Returns all registered datasets.

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
GET /_query/dataset
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X GET "${ELASTICSEARCH_URL}/_query/dataset" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

### Delete a dataset

Deletes a dataset by name.

::::{tab-set}
:group: api-ref

:::{tab-item} Console
:sync: console
```console
DELETE /_query/dataset/access_logs
```
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X DELETE "${ELASTICSEARCH_URL}/_query/dataset/access_logs" \
  -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

## Dataset settings

Dataset settings configure how a resource's format is read. They are specified in the `settings` object of a dataset definition. They divide into settings users commonly change and advanced settings with sensible defaults.

### Common settings

The following settings apply to all file-based data sources:

| Setting | Default | Description |
|---|---|---|
| `format` | Auto-detect from extension | Override format detection. Valid values: `"parquet"`, `"csv"`, `"tsv"`, `"ndjson"`. |
| `partition_detection` | `auto` | Partition detection mode. Valid values: `"auto"`, `"hive"`, `"none"`. |

### CSV and TSV settings

**Commonly changed:**

| Setting | Default (CSV / TSV) | Description |
|---|---|---|
| `delimiter` | `,` / `\t` | The field separator. |
| `mode` | `quoted` / `plain` | A preset bundling quoting and escaping into one choice. Valid values: `"quoted"`, `"escaped"`, `"plain"`. |
| `header_row` | `true` | Whether the first row names the columns. |
| `null_value` | `""` (empty) | The token read as null (for example `NULL`, `NA`, `\N`). |
| `encoding` | `UTF-8` | The file's character encoding. |

**Advanced:**

| Setting | Default (CSV / TSV) | Description |
|---|---|---|
| `quote` | `"` / `"` | The quote character. Subsumed by `mode`. |
| `escape` | `\` / `\` | The escape character. Subsumed by `mode`. |
| `comment` | `//` | Lines beginning with this prefix are skipped. |
| `column_prefix` | `col` | Prefix for generated column names when `header_row` is `false`. |
| `schema_sample_size` | `20000` | Rows sampled to infer column types. |
| `datetime_format` | ISO-8601 | The pattern used to parse date and time values. |
| `multi_value_syntax` | `none` | Whether bracketed multi-values are recognized. Valid values: `"none"`, `"brackets"`. |
| `max_field_size` | `10485760` (10 MB) | The maximum size of a single field. `0` is unlimited. |
| `error_mode` | `fail_fast` | How a malformed row is handled. Valid values: `"fail_fast"`, `"skip_row"`, `"null_field"`. |
| `max_errors` | unbounded | Bad rows tolerated. Not valid with `fail_fast`. |
| `max_error_ratio` | `0.0` | Fraction of bad rows tolerated (0.0–1.0). Not valid with `fail_fast`. |

### NDJSON settings

**Commonly changed:**

| Setting | Default | Description |
|---|---|---|
| `schema_sample_size` | `20000` | Lines sampled to infer the schema. Determines whether sparse or late-appearing fields get a column. |

**Advanced:**

| Setting | Default | Description |
|---|---|---|
| `segment_size` | `4mb` | The unit a file is divided into for parallel reading. Minimum 64 KiB. |

### Parquet

Parquet is self-describing and is read with no settings in the common case. Its two settings are read-performance toggles, defaulted on.

| Setting | Default | Description |
|---|---|---|
| `optimized_reader` | `true` | A read-path optimization. |
| `late_materialization` | `true` | A read-path optimization. |

## Querying federated data

A dataset is a read source for the standard {{esql}} pipeline. Every processing command operates on it as on an index. Filters and limits are applied during the file scan.

```esql
FROM sales | WHERE region == "EMEA" | STATS revenue = SUM(amount) BY product | SORT revenue DESC
```

A single query may read more than one source:

* **Several datasets** — `FROM sales, returns` reads both and combines the results.
* **An index together with a dataset** — `FROM orders, sales` combines data held in {{es}} with data held in object storage.

### Metadata columns

Metadata columns are available through `METADATA`:

| Column | Returned for a dataset |
|---|---|
| `_index` | The dataset name. |
| `_id` | A stable per-row identifier. |
| `_version` | The source file's modification time. |
| `_source` | The row as a JSON object. |
| `_file.path`, `_file.name`, `_file.directory`, `_file.size`, `_file.modified` | The object each row was read from. |
| `_score` | null |
| `_ignored` | null |

```esql
FROM access_logs METADATA _file.path, _file.name, _file.size
| KEEP _file.path, _file.name, _file.size, status_code
| LIMIT 10
```

### Functionality that does not work, or works differently, on datasets

A dataset is a file, not an {{es}} index, so the operations below are not available. Each fails with a clear error rather than wrong results.

| Operation | Reason | Error |
|---|---|---|
| LOOKUP JOIN, with a dataset as the lookup target | A dataset works as the left (source) side of the join. The lookup target on the right must be an {{es}} index. | `LOOKUP JOIN against a dataset is not supported` |
| TS (time series) | A time-series source must be an {{es}} index. | `TS command is not supported for datasets` |
| LOGSDB and other non-standard index modes | These index modes apply only to {{es}} indices. | `LOGSDB index mode on FROM <dataset> is not supported` |
| MATCH, MATCH_PHRASE, KNN | These resolve a field from an index mapping, which a dataset does not have. | `… cannot operate on [<field>], which is not a field from an index mapping` |
| KQL, QSTR | These query an {{es}} index. | `… cannot be used after [FROM <dataset>]` |
| Document-level security (DLS) and field-level security (FLS) | Queries that apply DLS or FLS to a dataset fail at planning time. | |
| Snapshot and restore | Data sources and datasets cannot be snapshotted or restored. | |

Complex Parquet types MAP and nested LIST are not currently supported and return null. STRUCT is supported and flattened to dot-notation column names (for example, `address.city`).

## Schema discovery

<!-- TODO: Confirm whether the schema discovery API (GET /_query/data_source/{name}/_schema) is public.
     Tracked in https://github.com/elastic/esql-planning/issues/288 -->

Because federated data does not live in {{es}}, the system discovers schemas before queries can run. How this works depends on the file format.

### Schema sources by format

For **Parquet**, schemas are read from file headers. These formats also provide metadata like column statistics and bloom filters that the engine uses to skip irrelevant data.

For **CSV and NDJSON**, schemas are inferred by sampling rows from the data files.

### Schema merge strategies

When a dataset spans multiple files, the files may have different schemas. Two merge strategies are available:

- **First file wins.** The first file alphabetically defines the schema. All other files are assumed to match.
- **Union by name.** Schemas from all files are merged. Lossless type widening is applied where possible. Lossy promotions such as integer to keyword cause an error.

## Cluster settings

The data sources feature adds the cluster settings below. All are node-scoped.

### Object limits

| Setting | Default | Description |
|---|---|---|
| `esql.data_sources.max_count` | 100 | Maximum number of data sources that can be defined. Range 0–1000. |
| `esql.datasets.max_count` | 1000 | Maximum number of datasets that can be defined. Range 0–10,000. |

### Request concurrency and throttling

| Setting | Default | Description |
|---|---|---|
| `esql.external.max_concurrent_requests` | 50 | Maximum concurrent cloud API requests per storage scheme, per node. 0 disables limiting. Range 0–500. |
| `esql.external.throttle_max_retry_duration` | 30s | Maximum total time spent retrying throttled cloud API requests before failing the query. 0 disables the budget. Range 0–300. |

### Glob and file-discovery limits

| Setting | Default | Description |
|---|---|---|
| `esql.external.max_discovered_files` | 10,000 | Hard cap on files collected by glob expansion before the query aborts. Protects against degenerate globs. Range 1–1,000,000. |
| `esql.external.max_glob_expansion` | 100 | Cap on concrete paths generated by brace expansion before falling back to listing. Range 1–10,000. |

### Authentication

| Setting | Default | Description |
|---|---|---|
| `esql.datasource.workload_identity.enabled` | false | Enables `auth: "workload_identity"` (the node's own cloud identity via IMDS / metadata server). Operator-only. Not available in serverless. |

### Caching

| Setting | Default | Description |
|---|---|---|
| `esql.source.cache.enabled` | true | Enables the external-source cache (inferred schemas and file listings). Dynamic. |
| `esql.source.cache.size` | 0.4% of heap | Memory budget for the cache. Applied at node startup only. |
| `esql.source.cache.schema.ttl` | 5m | How long an inferred schema is cached. Applied at node startup only. |
| `esql.source.cache.listing.ttl` | 30s | How long a file-listing result is cached. Applied at node startup only. |

## Security

### Privileges

An administrator defines and manages data sources and datasets. An analyst queries datasets. Dataset operations are authorized by the standard {{es}} index privileges, so a role that already administers or reads the matching index names covers datasets with no additional grant.

| Operation | Privilege | Type |
|---|---|---|
| Query a dataset | `read` | Index, on the dataset name |
| Create, read, or delete a dataset | `manage` or `all` | Index, on the dataset name |
| Dataset administration granted on its own | `create_dataset`, `read_dataset_metadata`, `delete_dataset`, `manage_dataset` | Index, on the dataset name |
| Create or replace a data source | `global.data_source` `create` / `cluster.manage` | Global (fine-grained) / Cluster (global) |
| Read a data source definition | `global.data_source` `read_metadata` / `cluster.manage` | Global (fine-grained) / Cluster (global) |
| Delete a data source | `global.data_source` `delete` / `cluster.manage` | Global (fine-grained) / Cluster (global) |
| All data source operations | `global.data_source` `manage` / `cluster.manage` | Global (fine-grained) / Cluster (global) |

`superuser` has full access to data sources and datasets. Data source management is reached only through `superuser` or a role explicitly granted `global.data_source`.

A role configures these privileges as follows. The example grants querying `sales` and `clicks`, dataset administration over `acme_*`, and management of the `acme_*` data sources:

```json
{
  "indices": [
    { "names": ["sales", "clicks"], "privileges": ["read"] },
    { "names": ["acme_*"], "privileges": ["manage"] }
  ],
  "global": {
    "data_source": [
      { "names": ["acme_*"], "privileges": ["manage"] }
    ]
  }
}
```

A data source's credentials are masked when its definition is read back, and at query time the store is accessed using the data source's stored credentials.

### Credential encryption

When a data source includes credentials, {{es}} encrypts them before storing them in the cluster state. This encryption is configured automatically in {{ech}} and {{serverless-short}} deployments. For self-managed, {{ece}}, and {{eck}} deployments, you must configure encryption manually.

If encryption is not configured, any `PUT /_query/data_source` request that includes credentials returns a `503` error. The two required keystore settings are `cluster.state.encryption.active_password_id` and `cluster.state.encryption.password.<id>`.

::::{applies-switch}
:::{applies-item} self:
Use the [elasticsearch-keystore](../../elasticsearch/command-line-tools/elasticsearch-keystore.md) tool to add a password and set the active password ID on every node:

```bash
bin/elasticsearch-keystore add cluster.state.encryption.password.1
bin/elasticsearch-keystore add cluster.state.encryption.active_password_id
```

When prompted for the active password ID, enter the ID that matches the password setting suffix (in this example, `1`). Then call the [reload secure settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-reload-secure-settings) to apply the new settings without a full restart:

```console
POST /_nodes/reload_secure_settings
```
:::
:::{applies-item} eck:
Add the encryption password and active password ID as [secure settings](docs-content://deploy-manage/security/k8s-secure-settings.md) through Kubernetes secrets referenced in `spec.secureSettings`.
:::
:::{applies-item} ece:
Add the encryption password and active password ID as [secure settings](docs-content://deploy-manage/security/secure-settings.md) using the Cloud UI or the [RESTful API](cloud://reference/cloud-enterprise/restful-api.md).
:::
::::

To allow plaintext credential storage without encryption, set `cluster.state.encryption.required` to `false`. This is not recommended for production use.

### Credential masking

All credential values are replaced by `::es_redacted::` in GET responses. Credentials are never returned in API responses.
