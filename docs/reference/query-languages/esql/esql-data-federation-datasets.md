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
- **Resource**: the URI and glob pattern that selects the files to read. Refer to [resource patterns](esql-data-federation-patterns.md) for the pattern language.
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
| `target_split_size` | `64mb` | The target size of each unit of work a file is divided into to be read in parallel across nodes. Files larger than the target are cut into several splits. Files smaller than the target are read as a single split. Lower the split size for more parallelism over a few large files. Raise it to reduce planning work over a very large number of bytes. |
| `split_probe_window` | `256kb` | The number of bytes a search for a record boundary can read while dividing files into splits. A dataset with records that are larger than this value is cut into fewer splits than the `target_split_size`. In this case, reads occur with less parallelism because a search that does not reach the end of a record finds no boundary to split at. Raise the value for a dataset with long records, within the budget in the note that follows. The setting applies to NDJSON and to CSV and TSV without quoting or escaping. Quoted or escaped records cannot be searched at a fixed offset, so those files are scanned sequentially and bounded by `external_max_record_size`, as it is dividing a split further across the threads of one node. Values below about `136kb` are read in full by every search, since finishing a window that small costs less than opening another connection. |
| `max_split_probes` | `1000` | The maximum number of record-boundary searches a query can perform, which bounds how many splits its files are cut into. A searched file yields one split more than the searches spent on it. A file too small to search is read as a single whole-file split. A scan asking for more splits than this setting value is read at a wider split size than `target_split_size` requests. Raise it to get the requested split size on a very large scan. The highest accepted value is `10000`. |
| `file_exclusions` {applies_to}`stack: experimental 9.6+` | `["**/_*", "**/.*", "**/_temporary/**", "**/_delta_log/**"]` | Patterns naming objects to drop from wildcard discovery, written in the same [pattern language](esql-data-federation-patterns.md) as `resource` and matched against the object's path relative to the listing prefix. The default skips file names beginning with `_` or `.` and the contents of `_temporary` and `_delta_log` directories. Refer to [excluding non-data objects](#excluding-non-data-objects). |
| `file_sort_by` {applies_to}`stack: experimental 9.6+` | `list` (when `first_file_wins`) | What to order files by before taking the first-file-wins schema. Valid values: `"list"`, `"name"`, `"mtime"`. Only valid with `"schema_resolution": "first_file_wins"`. Refer to [first-file-wins file order](#first-file-wins-file-order). |
| `file_order` {applies_to}`stack: experimental 9.6+` | `asc` (when `first_file_wins`) | Sort direction for `file_sort_by`. Valid values: `"asc"`, `"desc"`. Always applied; `"list"` + `"desc"` reverses declaration or listing order. Only valid with `"schema_resolution": "first_file_wins"`. |

:::{note}
`max_split_probes` and `split_probe_window` are independent. The first defines how many record-boundary searches a query runs. The second defines how many bytes each one reads. Their product is the bytes a query can read while searching, which cannot exceed 4 GB. With the default values, it is 1000 searches of `256kb`, or around 250 MB. Size the window from the dataset's longest record and the count from the number of splits the scan needs. Lower one of them if the pair is rejected. The budget covers searches at fixed offsets: a sequentially scanned file (quoted or escaped CSV and TSV) is bounded by `external_max_record_size` rather than by either key.
:::

### Excluding non-data objects

```{applies_to}
stack: experimental 9.6+
```

Object-store prefixes rarely hold only data. A Spark `_SUCCESS` marker, `.crc` sidecars, a `_temporary/`
or `_delta_log/` subtree, or a folder placeholder the S3 console created all sit next to the files you want
to read, and any object no reader can claim fails the whole query.

By default a dataset skips them. The default exclusion list is:

```
["**/_*", "**/.*", "**/_temporary/**", "**/_delta_log/**"]
```

The first two entries follow the convention Spark, Hive, and Trino use: a file whose name begins with `_` or
`.` is not data, at any depth. Because `*` never crosses a `/`, these entries match only the last path
segment. They exclude files by name and cannot touch a directory, so partition directories are never
affected: `_dept=alpha/part-0.parquet` and `_foo/part-0.parquet` are read under every partition detection
mode. The last two entries name the two well-known directories whose contents look like data but are not: a
failed Spark job's leftovers under `_temporary`, and a Delta Lake transaction log under `_delta_log`.

Each entry is an ordinary pattern in the same [pattern language](esql-data-federation-patterns.md) as
`resource`, matched against the same string: the object's path relative to the resource's listing prefix. An
object whose relative path matches any entry is dropped from the listing. Setting `file_exclusions` replaces
the default list entirely.

Note that the `resource` pattern is your first filter: a dataset on `**/*.parquet` never sees a `README.md` at
all, so there is nothing to exclude. This setting is for objects the `resource` pattern *does* match, most
often data-shaped files in a directory you do not want read. A retired partition kept alongside the live ones
is the common case:

```
access/year=2024/part-0.parquet      <- read
access/year=2025/part-0.parquet      <- read
access/backup_2024/part-0.parquet    <- also matches **/*.parquet, but is not current data
```

Restate the default and add the directory:

```console
PUT /_query/dataset/access_logs
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.parquet",
  "settings": {
    "file_exclusions": ["**/_*", "**/.*", "**/_temporary/**", "**/_delta_log/**", "backup_2024/**"]
  }
}
```

The added entry is matched against paths relative to the listing prefix `s3://logs-bucket/access/`, so
`backup_2024/**` drops everything under that one directory. To drop directories of that name at any depth,
write `**/backup_2024/**` instead.

Whenever exclusion drops something, the response carries a warning saying how many of the objects your
`resource` selected were excluded, naming one of them and the entry that matched it:

```
2 of 4 objects matching the resource under [s3://logs-bucket/access/] were excluded by the
[file_exclusions] dataset setting, for example [_SUCCESS] which matched entry [**/_*]
```

The warning is emitted for the default list as well as for one you set, because a dataset that never
configured exclusion is exactly the one where a missing file is hardest to explain. It is a single warning per
listing however many objects were dropped, so it does not grow with the size of the prefix.

To turn exclusion off entirely, set `"file_exclusions": []`. Directory placeholder keys are still skipped
(see below), so this reads every object the resource pattern matches except those.

Two rules are worth knowing. A malformed entry is rejected when you register the dataset, with an error
naming the [invalid pattern](esql-data-federation-patterns.md#invalid-patterns). And exclusion applies to
wildcard discovery only. An object you name explicitly in `resource` is always read, because naming it is a
request to read it, whether the resource carries no wildcard at all, names the object as one entry of a
comma-separated resource, or names it through a finite brace pattern such as `data/{a,b}.csv`.

Objects whose key ends in `/` are directory placeholders rather than files, the empty markers an object-store
console creates for a folder. They are skipped before any pattern is consulted, so no
setting can bring them back.

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

% schema_sample_size (default 20000) hidden until https://github.com/elastic/elasticsearch/issues/155636 is resolved

| Setting | Default (CSV / TSV) | Description |
|---|---|---|
| `quote` | `"` / none | The quote character, or `"none"` to turn quoting off. An explicit value overrides the `mode` preset. |
| `escape` | `\` / none | The escape character, or `"none"` to turn escaping off. An explicit value overrides the `mode` preset. |
| `comment` | `//` | Lines beginning with this prefix are skipped. |
| `column_prefix` | `col` | Prefix for generated column names when `header_row` is `false`. |
| `datetime_format` | ISO-8601 | The pattern used to parse date and time values. |
| `trim_spaces` | `false` | Whether to remove surrounding ASCII whitespace from string field values. |
| `multi_value_syntax` | `none` | Whether bracketed multi-values are recognized. Valid values: `"none"`, `"brackets"`. |
| `max_field_size` | `10485760` (10 MB) | The maximum size of a single field. `0` is unlimited. |

### NDJSON settings

% **Commonly changed:** table hidden — schema_sample_size was the only row.
% Restore when https://github.com/elastic/elasticsearch/issues/155636 is resolved:
% | Setting | Default | Description |
% |---|---|---|
% | `schema_sample_size` | `20000` | Lines sampled to infer the schema. Determines whether sparse or late-appearing fields get a column. |

| Setting | Default | Description |
|---|---|---|
| `segment_size` | `4mb` | The unit a file is divided into for parallel reading. Minimum 64 KiB. |
| `datetime_format` | `strict_date_optional_time` | The pattern used to infer and parse date and time values. |

### Parquet

Parquet is self-describing and has no format-specific dataset settings.

## How schemas are inferred

Because federated data does not live in {{es}}, the system discovers schemas before queries can run. How this works depends on the file format:

- Parquet reads its schema from file metadata, which also provides column statistics and bloom filters that the engine uses to skip irrelevant data.
- For CSV, TSV, and NDJSON, schemas are inferred by sampling rows from the data files.

### Schema merge strategies

When a dataset spans multiple files, the files might have different schemas. Set `schema_resolution` in the dataset's `settings` object to choose a strategy:

- `union_by_name` (default): Merges schemas from all files by column name. Types are widened where possible: when two files disagree with no common type, the column falls back to `keyword` and the response carries a warning suggesting `strict` if you want the conflict to fail instead. `union_by_name` never fails on a type conflict. This is safer when files can vary, at the cost of reading and merging more file metadata.
- `first_file_wins`: After files are discovered, they are ordered and the schema is taken from **the first file in that order**. Later files are assumed to match. This is typically faster, but schema differences in later files can cause query errors or values to be read under the wrong assumptions. Use [`file_sort_by`](#first-file-wins-file-order) and [`file_order`](#first-file-wins-file-order) to choose that first file. Those settings are rejected on `union_by_name` and `strict`.
- `strict`: Requires every file to have the same schema, apart from nullability, and returns an error when they differ. Use this when schema drift must fail explicitly.

### First-file-wins file order

```{applies_to}
stack: experimental 9.6+
```

`file_sort_by` and `file_order` apply only when `schema_resolution` is `first_file_wins`. The dataset API and query `WITH` clause reject them on `union_by_name` and `strict`.

After files are discovered (glob, comma list, or mix), they are ordered, then the schema is taken from **the first file in that order**.

| Setting | Default (when FFW) | Values | Meaning |
| --- | --- | --- | --- |
| `file_sort_by` | `list` | `list`, `name`, `mtime` | What to order files by. |
| `file_order` | `asc` | `asc`, `desc` | Direction. Always applied. `list` + `desc` reverses declaration / listing order. |

Default is `list` + `asc`: keep discovery / **declaration** order, then take the first file. A comma-separated `resource` therefore uses the first name you wrote. On S3 (and Azure/GCS) a glob's LIST is already lexicographic by key, so the default matches historical `name` + `asc` for those globs. It does **not** re-sort a comma list; that is the difference from previous FFW.

`mtime` is the object-store LastModified (or local file mtime). Files with no mtime sort as oldest. Equal mtimes break ties by `name` ascending. There is no created-time key; S3 listings do not provide one.

This is not query row order. `LIMIT` does not mean "rows from the first file."

#### First / last declared file (`file_sort_by: list`)

Comma-separated `resource` keeps your list order under the default (`list` + `asc`). Use that to pin a schema file without scanning every footer.

**First named file is the schema** (wide file first). Knobs may be omitted; they default to `list` + `asc`:

```console
PUT /_query/dataset/logs
{
  "data_source": "prod_s3",
  "resource": "s3://logs/_schema.parquet,s3://logs/events/**/*.parquet",
  "settings": {
    "schema_resolution": "first_file_wins"
  }
}
```

`_schema.parquet` may be empty of rows as long as its footer (or CSV header) has the full column set. Later files are read as that schema.

**Last named file is the schema** (append the wide file):

```console
PUT /_query/dataset/logs
{
  "data_source": "prod_s3",
  "resource": "s3://logs/events/**/*.parquet,s3://logs/_schema.parquet",
  "settings": {
    "schema_resolution": "first_file_wins",
    "file_sort_by": "list",
    "file_order": "desc"
  }
}
```

On a **glob only**, omitted knobs (`list` + `asc`) mean provider order. S3 ListObjects is already lexicographic by key, so this matches `name` + `asc`. A local directory is not sorted; for a stable donor there set `file_sort_by` to `name`.

```console
PUT /_query/dataset/logs
{
  "data_source": "prod_s3",
  "resource": "s3://logs/events/**/*.parquet",
  "settings": {
    "schema_resolution": "first_file_wins"
  }
}
```

#### Lexicographic path (`file_sort_by: name`)

Client-sort by path. Same donor as the default on an S3 glob; **required** when a comma list must ignore declaration order, or when a local glob must not follow `readdir`.

```console
PUT /_query/dataset/logs
{
  "data_source": "prod_s3",
  "resource": "s3://logs/events/**/*.parquet",
  "settings": {
    "schema_resolution": "first_file_wins",
    "file_sort_by": "name",
    "file_order": "asc"
  }
}
```

Newest ISO date in a Hive path (`dt=2024-01-01` ... `dt=2026-09-04`), without trusting mtime:

```console
PUT /_query/dataset/logs
{
  "data_source": "prod_s3",
  "resource": "s3://logs/events/dt=*/*.parquet",
  "settings": {
    "schema_resolution": "first_file_wins",
    "file_sort_by": "name",
    "file_order": "desc"
  }
}
```

#### Newest / oldest object (`file_sort_by: mtime`)

```console
PUT /_query/dataset/logs
{
  "data_source": "prod_s3",
  "resource": "s3://logs/events/**/*.parquet",
  "settings": {
    "schema_resolution": "first_file_wins",
    "file_sort_by": "mtime",
    "file_order": "desc"
  }
}
```

Use when the newest **overwrite** should win. A copy into another prefix gets a new LastModified; prefer `name` + `desc` when the path already encodes time.

#### Rejected combinations

```console
PUT /_query/dataset/logs
{
  "data_source": "prod_s3",
  "resource": "s3://logs/events/**/*.parquet",
  "settings": {
    "schema_resolution": "union_by_name",
    "file_sort_by": "list"
  }
}
```

Fails. `file_sort_by` and `file_order` are not used with `union_by_name` or `strict`. Union-by-name already merges every file; file order does not change the column set or types. Dedicated types/renames stay on the dataset `mappings` block.

## Next steps

- [Query your datasets](esql-data-federation-querying.md) to learn how partition pruning, filter pushdown, and column selection reduce the amount of data read from storage.
- If queries return unexpected types or missing values, check the [schema merge strategies](#schema-merge-strategies) or declare column types explicitly with [dataset mappings](#declare-a-dataset-mapping).
- [Tune cluster settings](esql-data-federation-cluster-settings.md) to adjust file-discovery limits, caching TTLs, and request concurrency for your workload.
