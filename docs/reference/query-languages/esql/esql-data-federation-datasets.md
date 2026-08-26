---
navigation_title: "Add datasets"
description: "Create ES|QL Data Federation datasets to query files in external storage. Choose file formats, adjust Parquet and CSV parsing, and control schema inference."
applies_to:
  stack: experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Select external datasets for {{esql}} Data Federation

Datasets share the same namespace as indices, data streams, aliases, and [{{esql}} views](esql-views.md). A dataset cannot have the same name as any of them.

:::{include} _snippets/data-federation/experimental-warning.md
:::

## Supported file formats

Federated data sources can read the following file formats:

:::{include} _snippets/data-federation/supported-file-formats.md
:::

The format is detected automatically from the file extension. You can override this in the [dataset settings](#common-settings).

### Text formats

The following text formats are recognized by file extension:

| Format | Recognized extensions |
|---|---|
| CSV | `.csv` |
| TSV | `.tsv` |
| NDJSON | `.ndjson`, `.jsonl`, `.json` |

### Compression for text formats

A text resource is read uncompressed, or compressed with a codec identified from a trailing extension: `clicks.csv`, `clicks.csv.gz`, `clicks.csv.zst`.

| Codec | Extensions |
|---|---|
| uncompressed | none |
| gzip | `.gz`, `.gzip` |
| zstd | `.zst`, `.zstd` |

### Parquet

Parquet declares its compression internally, per column chunk, so Parquet resources are not externally compressed. They are recognized by the `.parquet` and `.parq` extensions. The following internal codecs are supported:

- `UNCOMPRESSED`
- `SNAPPY`
- `ZSTD`
- `GZIP`
- `LZ4_RAW`
- `LZ4` (legacy Hadoop-framed format, supported for reading only)

## Manage datasets in the UI

In {{kib}}, you create and manage datasets from the **Datasets** tab under **Data management** > **{{esql}} Data Federation**.

The **Datasets** tab lists each dataset including:
- its data source and data source type
- its resource
- its description

From this tab you can search your datasets, filter by data source, add a new one, and edit or delete an existing one.

### Add a new dataset

Click **Add dataset** to open a flyout where you define the dataset:

- **Data source**: the connected data source to read through.
- **Name**: a unique name for use in queries. Names must be lowercase and cannot begin with `-`, `_`, or `+`. A dataset cannot share a name with any existing index, data stream, alias, or view.
- **Description**: an optional description.
- **Resource**: the URI and glob pattern that selects the files to read.
- **Format**: the file format. This selection is required in the {{kib}} UI. The API can omit `settings.format` to auto-detect it from the file extension. Refer to [supported file formats](#supported-file-formats).

To configure how the format is read, expand **Advanced settings**. Refer to [dataset settings](#dataset-settings).

To customize the inferred schema, rename columns, or override field types, use the [dataset mappings API](#declare-a-dataset-mapping). Schema customization is not available in the UI.

## Manage datasets using the API

Datasets are managed under the `/_query/dataset` endpoint. All dataset operations require the index `manage` privilege on the dataset name, or a fine-grained dataset privilege. Refer to [manage credentials and privileges](esql-data-federation-security.md) for details.

| Operation | Endpoint | API reference |
|---|---|---|
| [Create or update](#create-or-update-a-dataset) | `PUT /_query/dataset/{name}` | [Create or update an ES\|QL dataset](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-put-dataset) |
| [Get](#get-a-dataset) | `GET /_query/dataset/{name}` | [Get ES\|QL datasets](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-get-dataset) |
| [List all](#list-all-datasets) | `GET /_query/dataset` | [Get ES\|QL datasets](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-get-dataset) |
| [Delete](#delete-a-dataset) | `DELETE /_query/dataset/{name}` | [Delete ES\|QL datasets](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-delete-dataset) |

### Create or update a dataset

[`PUT /_query/dataset/{name}`](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-put-dataset) creates a new dataset or replaces an existing one entirely.

:::{important}
A dataset cannot have the same name as an existing index, data stream, alias, or view, because dataset names share the same namespace. Dataset names must be lowercase and cannot begin with `-`, `_`, or `+`.
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

:::{tip}
After creating a dataset, you can check the field mappings that {{es}} inferred from your files. Refer to [check field mappings](esql-data-federation-quickstart.md#check-field-mappings) in the quickstart for a hands-on example.
:::

### Declare a dataset mapping

By default, {{es}} infers a dataset's schema from its files. You can instead add an optional `mappings` block to the create or update request to control column names and types. Dataset mappings are currently available only through the API. The {{kib}} **Add dataset** flyout does not expose them.

The following example declares the complete schema, renames the physical `event_time` column to `@timestamp`, supplies its date format, and uses `request_id` as the row's `_id`:

```console
PUT /_query/dataset/access_logs
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.csv",
  "mappings": {
    "dynamic": "false",
    "properties": {
      "@timestamp": {
        "type": "date",
        "path": "event_time",
        "format": "yyyy-MM-dd HH:mm:ss"
      },
      "request_id": { "type": "keyword" },
      "service": { "type": "keyword" },
      "status_code": { "type": "integer" }
    },
    "_id": {
      "path": "request_id"
    }
  }
}
```

The `mappings` block supports the following properties:

- `properties`: Columns keyed by their logical name. Each column requires a `type`.
  - `path`: Optional physical column name. Use it to expose a file column under a different logical name, including renaming a timestamp column to `@timestamp`.
  - `format`: Optional date parsing pattern for a column with type `date`.
- `_id.path`: Optional source column whose value becomes the row's `_id`.
- `dynamic`: Controls undeclared columns. The default, `true`, overlays the declared columns on the inferred schema. Set it to `false` to treat the declaration as the complete schema, skip schema inference for text formats, and leave undeclared columns unavailable to queries.

:::{note}
With `dynamic: false`, declared columns bind to file columns by name. In CSV and TSV files with a header row, each declared column binds to the header column of the same name (or the name given in `path`). A declared column absent from a file reads as null with a warning, not an error. Headerless files bind by position. 

For self-describing columnar formats such as Parquet, names bind to the file schema the same way, and a declared type is accepted when the file's type can be coerced to it. Only incompatible type pairs are rejected.
:::

### Get a dataset

[`GET /_query/dataset/{name}`](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-get-dataset) retrieves a dataset by name.

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

[`GET /_query/dataset`](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-get-dataset) returns all registered datasets.

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

[`DELETE /_query/dataset/{name}`](https://www.elastic.co/docs/api/doc/elasticsearch/v9/operation/operation-esql-delete-dataset) deletes a dataset by name.

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
| `schema_resolution` | `union_by_name` | How schemas are reconciled across multiple files. Valid values: `"first_file_wins"`, `"strict"`, `"union_by_name"`. Refer to [schema merge strategies](#schema-merge-strategies). |
| `error_mode` | `fail_fast` | How malformed rows are handled. Valid values: `"fail_fast"`, `"skip_row"`, `"null_field"`. For Parquet, `skip_row` fills affected columns with null instead of skipping the entire row. For CSV, TSV, and NDJSON, `null_field` fills only individual value failures with null. Rows whose structure cannot be parsed (for example, an unparsable JSON line or a malformed CSV row) are still dropped. |
| `max_errors` | unbounded | Maximum malformed rows allowed before the query fails. Ignored when `error_mode` is `fail_fast`. |
| `max_error_ratio` | `0.0` | Fraction of malformed rows allowed (0.0–1.0). Ignored when `error_mode` is `fail_fast`. |

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
| `schema_sample_size` | `20000` | Rows sampled to infer the schema. Determines whether sparse or late-appearing fields get a column. |
| `quote` | `"` / none | The quote character, or `"none"` to turn quoting off. An explicit value overrides the `mode` preset. |
| `escape` | `\` / none | The escape character, or `"none"` to turn escaping off. An explicit value overrides the `mode` preset. |
| `comment` | `//` | Lines beginning with this prefix are skipped. |
| `column_prefix` | `col` | Prefix for generated column names when `header_row` is `false`. |
| `datetime_format` | ISO-8601 | The pattern used to parse date and time values. |
| `trim_spaces` | `false` | Whether to remove surrounding ASCII whitespace from string field values. |
| `multi_value_syntax` | `none` | Whether bracketed multi-values are recognized. Valid values: `"none"`, `"brackets"`. |
| `max_field_size` | `10485760` (10 MB) | The maximum size of a single field. `0` is unlimited. |

### NDJSON settings

**Commonly changed:**

| Setting | Default | Description |
|---|---|---|
| `schema_sample_size` | `20000` | Lines sampled to infer the schema. Determines whether sparse or late-appearing fields get a column. |

**Advanced:**

| Setting | Default | Description |
|---|---|---|
| `segment_size` | `4mb` | The unit a file is divided into for parallel reading. Minimum 64 KiB. |
| `datetime_format` | `strict_date_optional_time` | The pattern used to infer and parse date and time values. |

### Parquet

Parquet is self-describing and is read with no settings in the common case. Its two settings are read-performance toggles, defaulted on.

| Setting | Default | Description |
|---|---|---|
| `optimized_reader` | `true` | Uses vectorized decoding, page skipping, and I/O prefetch for the next row group. Leave enabled for normal scans. Disable it only to troubleshoot a suspected optimized-reader issue by using the baseline read path. |
| `late_materialization` | `true` | When a filter can be pushed to the reader, reads predicate columns first and materializes other projected columns only for surviving rows. This is most useful for selective queries over wide files. Leave enabled unless you are troubleshooting filter or read-path behavior. |

## How schemas are inferred

Because federated data does not live in {{es}}, the system discovers schemas before queries can run. How this works depends on the file format:

- Parquet reads its schema from file metadata, which also provides column statistics and bloom filters that the engine uses to skip irrelevant data.
- For CSV, TSV, and NDJSON, schemas are inferred by sampling rows from the data files.

### Schema merge strategies

When a dataset spans multiple files, the files might have different schemas. Set `schema_resolution` in the dataset's `settings` object to choose a strategy:

- `union_by_name` (default): Merges schemas from all files by column name. Types are widened where possible: when two files disagree with no common type, the column falls back to `keyword` and the response carries a warning suggesting `strict` if you want the conflict to fail instead. `union_by_name` never fails on a type conflict. This is safer when files can vary, at the cost of reading and merging more file metadata.
- `first_file_wins`: Uses the first file alphabetically to define the schema and assumes later files match it. This is typically faster, but schema differences in later files can cause query errors or values to be read under the wrong assumptions.
- `strict`: Requires every file to have the same schema, apart from nullability, and returns an error when they differ. Use this when schema drift must fail explicitly.

## Next steps

- [Query your datasets](esql-data-federation-querying.md) to learn how partition pruning, filter pushdown, and column selection reduce the amount of data read from storage.
- If queries return unexpected types or missing values, check the [schema merge strategies](#schema-merge-strategies) or declare column types explicitly with [dataset mappings](#declare-a-dataset-mapping).
- [Tune cluster settings](esql-data-federation-cluster-settings.md) to adjust file-discovery limits, caching TTLs, and request concurrency for your workload.
