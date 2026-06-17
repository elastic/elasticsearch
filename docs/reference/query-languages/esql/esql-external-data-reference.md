---
navigation_title: "Reference"
description: "API reference, settings, schema discovery, and limits for {{esql}} external data sources and datasets."
applies_to:
  stack: preview =9.5
  serverless: preview
products:
  - id: elasticsearch
---

# {{esql}} external data sources reference

This page covers the full API surface, settings, and operational details for {{esql}} external data sources and datasets. For an introduction to these concepts, refer to [query external data with {{esql}}](esql-external-data.md). For a quickstart guide, refer to [](esql-external-data-quickstart.md).

## Supported data source types

The following data source types are supported:

:::{include} _snippets/external-data/supported-data-source-types.md
:::

## Supported file formats

External data sources can read the following file formats:

:::{include} _snippets/external-data/supported-file-formats.md
:::

The format is detected automatically from the file extension. You can override this in the [dataset settings](#common-settings).

<!-- TODO: Once the data source and dataset APIs are defined in elasticsearch-specification,
replace the inline examples below with a summary table linking to the generated
API reference at https://www.elastic.co/docs/api/doc/elasticsearch/ -->

## Data source API

Data sources are managed under the `/_query/data_source` endpoint. All data source operations require the cluster `manage` privilege.

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
    "access_key": "AKIA...",
    "secret_key": "wJal..."
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
    "access_key": "AKIA...",
    "secret_key": "wJal..."
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

## Data source settings by type

Each data source type has its own settings. Credentials are optional for all types. When omitted, {{es}} uses the default credential chain for the storage system.

### S3

The following settings are available for `s3` data sources:

| Field | Required | Description |
|---|---|---|
| `region` | No | AWS region. Defaults to `us-east-1`. |
| `endpoint` | No | Custom S3-compatible endpoint for MinIO or similar services. |
| `access_key` | No | AWS access key ID. |
| `secret_key` | No | AWS secret access key. |
| `auth` | No | Set to `"none"` for anonymous access to public buckets. |

When `access_key` and `secret_key` are omitted, {{es}} uses the default AWS credential chain: IAM roles, environment variables, or instance profiles.

### GCS

The following settings are available for `gcs` data sources:

| Field | Required | Description |
|---|---|---|
| `project_id` | No | GCP project ID. |
| `endpoint` | No | Custom GCS endpoint. |
| `token_uri` | No | Override OAuth2 token URI. |
| `credentials` | No | Service account JSON key file content. |
| `auth` | No | Set to `"none"` for anonymous access to public buckets. |

When `credentials` is omitted, {{es}} uses Application Default Credentials.

### Azure

The following settings are available for `azure` data sources:

| Field | Required | Description |
|---|---|---|
| `endpoint` | No | Custom endpoint. |
| `account` | No | Storage account name. |
| `connection_string` | No | Full Azure connection string. |
| `key` | No | Shared access key. Use with `account`. |
| `sas_token` | No | SAS token. |
| `auth` | No | Set to `"none"` for anonymous access to public containers. |

Multiple authentication methods are available. Provide one of the following:

- `connection_string`
- `account` + `key`
- `sas_token`

When all are omitted, {{es}} uses `DefaultAzureCredential`.

## Dataset API

Datasets are managed under the `/_query/dataset` endpoint. All dataset operations require the index `manage` privilege on the dataset name.

| Operation | Endpoint |
|---|---|
| [Create or update](#create-or-update-a-dataset) | `PUT /_query/dataset/{name}` |
| [Get](#get-a-dataset) | `GET /_query/dataset/{name}` |
| [List all](#list-all-datasets) | `GET /_query/dataset` |
| [Delete](#delete-a-dataset) | `DELETE /_query/dataset/{name}` |

### Create or update a dataset

`PUT` creates a new dataset or replaces an existing one entirely.

:::{important}
A dataset cannot have the same name as an existing index, alias, or view, because dataset names share the same namespace.
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

Dataset settings control how data is read and interpreted. They are specified in the `settings` object of a dataset definition.

### Common settings

The following settings apply to all file-based data sources:

| Field | Default | Description |
|---|---|---|
| `format` | Auto-detect from extension | Override format detection. Valid values: `"parquet"`, `"csv"`, `"ndjson"`, `"orc"`. |
| `error_mode` | `fail_fast` | Error handling mode. Valid values: `"fail_fast"`, `"skip_row"`, `"null_field"`. |
| `max_errors` | Unlimited when `skip_row` | Maximum number of row errors before aborting. |
| `max_error_ratio` | `0.0` | Maximum fraction of rows that may error, from 0.0 to 1.0. |
| `partition_detection` | `auto` | Partition detection mode. Valid values: `"auto"`, `"hive"`, `"template"`, `"none"`. |
| `partition_path` | null | Template for template-based partition extraction. |
| `hive_partitioning` | `true` | Enable or disable Hive-style partition detection. |

### CSV and TSV settings

The following additional settings apply when reading CSV or TSV files:

| Field | Default | Description |
|---|---|---|
| `delimiter` | `,` for CSV, `\t` for TSV | Field separator character. |
| `quote` | `"` | Quoting character. |
| `escape` | `\` | Escape character inside quoted fields. |
| `comment` | `//` | Line comment prefix. |
| `null_value` | `""` | String representation of null. |
| `encoding` | `UTF-8` | Character encoding. |
| `datetime_format` | null | Custom date and time pattern. When null, ISO-8601 and epoch formats are detected automatically. |
| `max_field_size` | 10485760 | Maximum bytes per field. Protects against out-of-memory errors from very large fields. |
| `multi_value_syntax` | `brackets` | Multi-value parsing mode. `"brackets"` parses `[a,b,c]` as a multi-value field. `"none"` disables multi-value parsing. |

### NDJSON settings

The following additional setting applies when reading NDJSON files:

| Field | Default | Description |
|---|---|---|
| `datetime_format` | `strict_date_optional_time` | Custom date and time pattern for parsing date strings. |

### Parquet and ORC

Parquet and ORC files have no user-configurable dataset settings. Row group coalescing, filter pushdown, and type conversion are handled automatically.

## Schema discovery

Because external data does not live in {{es}}, the system discovers schemas before queries can run. How this works depends on the file format.

### Schema sources by format

For **Parquet and ORC**, schemas are read from file headers. These formats also provide metadata like column statistics and bloom filters that the engine uses to skip irrelevant data.

For **CSV and NDJSON**, schemas are inferred by sampling rows from the data files.

### Schema merge strategies

When a dataset spans multiple files, the files may have different schemas. Two merge strategies are available:

- **First file wins.** The first file alphabetically defines the schema. All other files are assumed to match.
- **Union by name.** Schemas from all files are merged. Lossless type widening is applied where possible. Lossy promotions such as integer to keyword cause an error.

### Virtual partition fields

The engine exposes metadata for each file as queryable fields. These can be used in `WHERE` clauses for filtering:

- File name
- File size
- Modification time

## Limits

The following cluster settings control the maximum number of data sources and datasets:

| Setting | Default | Maximum |
|---|---|---|
| `esql.data_sources.max_count` | 100 | 1,000 |
| `esql.datasets.max_count` | 1,000 | 10,000 |

## Security

### Privileges

External data source operations use existing {{es}} privileges. No additional privileges are required.

- **Data source operations** require the cluster `manage` privilege. Unlike index-level privileges, cluster `manage` implies `read` for data sources, so a user who can create and update data sources can also retrieve their metadata.
- **Dataset operations** require the index `manage` privilege scoped to the dataset name pattern.
- **Querying a dataset** requires the `read` privilege on the dataset name. To verify that a data source is configured correctly, a user must create a dataset that references it and query that dataset.

More granular per-entity privileges are planned for a future release.

### Credential encryption

When a data source includes credentials, {{es}} encrypts them before storing them in the cluster state. This encryption is configured automatically in {{ech}} and {{serverless-short}} deployments. For self-managed, {{ece}}, and {{eck}} deployments, you must configure encryption manually.

If encryption is not configured, any `PUT /_query/data_source` request that includes credentials returns a `503` error. The two required keystore settings are `cluster.state.encryption.active_password_id` and `cluster.state.encryption.password.<id>`.

::::{applies-switch}
:::{applies-item} { deployment.self: ga }
Use the [elasticsearch-keystore](../../../elasticsearch/command-line-tools/elasticsearch-keystore.md) tool to add a password and set the active password ID on every node:

```bash
bin/elasticsearch-keystore add cluster.state.encryption.password.1
bin/elasticsearch-keystore add cluster.state.encryption.active_password_id
```

When prompted for the active password ID, enter the ID that matches the password setting suffix (in this example, `1`). Then call the [reload secure settings API](../../../elasticsearch/rest-apis/cluster-apis/nodes-reload-secure-settings.md) to apply the new settings without a full restart:

```console
POST /_nodes/reload_secure_settings
```
:::
:::{applies-item} { deployment.eck: ga }
Add the encryption password and active password ID as [secure settings](../../../../deploy-manage/security/k8s-secure-settings.md) through Kubernetes secrets referenced in `spec.secureSettings`.
:::
:::{applies-item} { deployment.ece: ga }
Add the encryption password and active password ID through the [ECE keystore API](../../../../reference/cloud/cloud-enterprise/ece-restful-api-examples-configuring-keystore.md).
:::
::::

To allow plaintext credential storage without encryption, set `cluster.state.encryption.required` to `false`. This is not recommended for production use.

### Credential masking

All credential values are replaced by `::es_redacted::` in GET responses. Credentials are never returned in API responses.

## Capabilities and limitations

Most {{esql}} processing commands and functions work on external datasets. The execution engine is the same one used for native indices.

### Supported capabilities

The following capabilities work on external datasets:

- All processing commands: `WHERE`, `EVAL`, `STATS`, `SORT`, `LIMIT`, `KEEP`, `DROP`, and others
- All scalar functions
- `ENRICH`
- `LOOKUP JOIN` with external data on the left side
- `INLINESTATS`

### Limitations

The following capabilities are not currently supported on external datasets:

- Full-text search: `MATCH`, `QSTR`, `KQL`, `KNN`, and scoring
- `LOOKUP JOIN` with external data on the right side
- Time series and `METRICS` commands
- Document-level security and field-level security on datasets. Queries that apply DLS or FLS to a dataset will fail at planning time.
- Snapshot and restore of data sources and datasets
