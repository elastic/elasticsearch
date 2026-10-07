---
navigation_title: "Schema inference"
description: "Learn how ES|QL Data Federation infers, merges, and selects schemas for datasets that span files in external storage."
applies_to:
  stack: experimental 9.5+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Schema inference for datasets in {{esql}} Data Federation

A dataset's schema defines the columns and data types that queries can use. A dataset can get its schema in three ways:

- **Infer every column:** Columns and types are discovered from the dataset's files. Each file's [format](#schema-sources-by-file-format) determines how its schema is found, and the [schema resolution strategy](#choose-a-schema-resolution-strategy) combines schemas from multiple files.
- **Declare every column:** Only the columns you [declare](#declare-a-schema-explicitly) are available, and nothing is inferred.
- **Declare some columns and infer the rest:** Declared columns override what's inferred for them, and every other column is still inferred.

:::{include} _snippets/data-federation/experimental-warning.md
:::

## Schema sources by file format

The following table shows where each [supported file format](esql-data-federation-file-formats.md) gets its schema and how it names columns:

| Format | Schema source | Column names |
|---|---|---|
| Parquet | File metadata | Field names. Nested fields use dotted names, such as `user.id`. |
| CSV and TSV with a header row | Sampled rows | Header names |
| CSV and TSV without a header row | Sampled rows | `col0`, `col1`, and so on, by position. The [`column_prefix`](esql-data-federation-dataset-settings.md#csv-column-prefix) setting controls the prefix. |
| NDJSON | Sampled rows | JSON keys. Nested objects use dotted names, such as `user.id`. |

Parquet metadata can also contain column statistics and bloom filters that let queries skip irrelevant data. For text formats, use `schema_sample_size` for [CSV and TSV](esql-data-federation-dataset-settings.md#csv-schema-sample-size) or [NDJSON](esql-data-federation-dataset-settings.md#ndjson-schema-sample-size) to control how many rows or lines are sampled.

## Choose a schema resolution strategy

When a dataset spans multiple files, [`schema_resolution`](esql-data-federation-dataset-settings.md#schema-resolution) controls how differences between their schemas are reconciled.

{applies_to}`stack: experimental 9.6+` The default is `first_file_wins`. Datasets created before `first_file_wins` became the default keep using `union_by_name` when they have no stored `schema_resolution` value.

{applies_to}`stack: experimental =9.5` The default is `union_by_name`.

The following table compares the available strategies:

| Strategy | Behavior | Use when |
|---|---|---|
| `first_file_wins` | Reads the schema from the first file after [file ordering](#control-which-file-supplies-the-schema), and reads later files with that schema. Columns that exist only in later files aren't included. Only one file's schema is inspected. | Files share a schema, and you want the least schema-discovery work. |
| `union_by_name` | Inspects every file and merges columns by name. Missing columns contain null values. Compatible types are widened, and incompatible types become `keyword`. | Files can gain or lose columns, and those differences shouldn't fail the query. |
| `strict` | Inspects every file and requires the same schema, apart from nullability. | Schema drift should fail the query. |

### How `first_file_wins` handles type mismatches [first-file-wins-type-mismatches]

A type mismatch in a later Parquet file doesn't fail the query. If a column's type can't be read as the type from the first file, that column contains null values for the file, and the response includes a warning. The [`error_mode`](esql-data-federation-dataset-settings.md#error-mode) setting doesn't control this case.

{applies_to}`stack: experimental 9.6+` If the column is [declared in `mappings`](#declare-a-schema-explicitly), `error_mode` decides instead, and only when a query reads the column:

- `fail_fast`: The query fails.
- `null_field`: The column contains null values for that file, and the response includes a warning.
- `skip_row`: All rows of that file are skipped. Each counts as a malformed row against [`max_errors`](esql-data-federation-dataset-settings.md#max-errors) and [`max_error_ratio`](esql-data-federation-dataset-settings.md#max-error-ratio), so the file exceeds any `max_error_ratio` below `1.0`, and a large file can exceed a small `max_errors`.

## Control which file supplies the schema
```{applies_to}
stack: experimental 9.6+
```

When [`schema_resolution`](esql-data-federation-dataset-settings.md#schema-resolution) is `first_file_wins`, the schema comes from the first file after the discovered files are ordered. Files are ordered after any partition filters prune the listing. Use [`file_sort_by`](esql-data-federation-dataset-settings.md#file-sort-by) to choose how files are ordered, and [`file_order`](esql-data-federation-dataset-settings.md#file-order) to take the first or last file. The other strategies reject these settings, because they inspect every file.

The following table shows which `file_sort_by` value to use:

| Schema source | `file_sort_by` value |
|---|---|
| First or last file in declaration or listing order | [`list`](#use-resource-declaration-order) (default) |
| First or last file by path | [`name`](#sort-by-object-path) |
| Oldest or newest file | [`mtime`](#sort-by-modification-time) |

File order controls schema selection, not query row order. For example, `LIMIT` does not restrict a query to rows from the file that supplied the schema.

### Use resource declaration order

The default `list` and `asc` combination preserves the order of files in a comma-separated `resource`. Put a dedicated schema file first to select it without reading every file footer:

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

The schema file can contain no rows as long as its Parquet footer or CSV header contains the complete column set. Set `file_order` to `desc` to use the last declared file instead.

For a resource that contains only a glob, `list` preserves the storage provider's listing order. Amazon S3, Azure Blob Storage, and Google Cloud Storage return keys in lexicographic order. A local directory is not necessarily sorted, so use `file_sort_by: name` when local schema selection must be stable.

### Sort by object path

Set `file_sort_by` to `name` to sort by path independently of declaration or provider order. For example, the following dataset uses the lexicographically greatest date path as its schema source:

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

### Sort by modification time

Set `file_sort_by` to `mtime` to select a schema based on object modification time. For example, the following dataset uses the most recently modified object:

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

Files without a modification time sort as the oldest. Equal modification times use the path in ascending order as a tie-breaker. Object stores expose modification time, not creation time. Copying an object to another prefix gives the copy a new modification time. Prefer `name` when the object path already encodes the relevant time.

## Declare a schema explicitly

By default, a dataset's schema is inferred from its files. To control column names and types, add an optional `mappings` block to the [create or update request](esql-data-federation-manage-datasets.md#create-or-update-a-dataset).

{applies_to}`stack: experimental 9.6+` You can also declare columns in the **Mapping** step when you add a dataset in {{kib}}.

The following example declares the complete schema, renames the physical `event_time` column to `@timestamp`, and supplies its date format:

```console
PUT /_query/dataset/access_logs
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs-bucket/access/**/*.csv",
  "mappings": {
    "dynamic": false,
    "properties": {
      "@timestamp": {
        "type": "date",
        "path": "event_time",
        "format": "yyyy-MM-dd HH:mm:ss"
      },
      "request_id": { "type": "keyword" },
      "service": { "type": "keyword" },
      "status_code": { "type": "integer" }
    }
  }
}
```

The `mappings` block supports the following properties:

- `properties`: Columns keyed by their logical name. Each column requires a `type`.
  - `path`: Optional physical column name. Use it to expose a file column under a different logical name, including renaming a timestamp column to `@timestamp`.
    - {applies_to}`stack: experimental 9.6` To keep a file column whose name matches a metadata name, rename it here before requesting that name through `METADATA`.
  - `format`: Optional date parsing pattern for a column with type `date` or `date_nanos`. Without a `format`, a plain number in a `date` column is read as epoch milliseconds. Set `format` to `epoch_second` for epoch seconds.
    - {applies_to}`stack: experimental 9.6+` A plain number in a `date_nanos` column without a `format` is also read as epoch milliseconds. A value before 1970 or after 2262, such as an epoch-nanoseconds count, can't be represented and is handled according to [`error_mode`](esql-data-federation-dataset-settings.md#error-mode). There is no `format` for epoch nanoseconds: to read epoch-nanoseconds values, declare the column as `long` and convert it with `TO_DATE_NANOS` in the query.
    - {applies_to}`stack: experimental =9.5` A plain number in a `date_nanos` column without a `format` is read as epoch nanoseconds.
- `dynamic`: Controls undeclared columns. The default, `true`, overlays the declared columns on the inferred schema. Set it to `false` to treat the declaration as the complete schema, skip schema inference, and leave undeclared columns unavailable to queries.

{applies_to}`stack: experimental 9.6+` A `mappings` block can't include `_id`. A create or update request that contains one is rejected.

### How declared columns match file columns

Each declared column is read from one file column:

- **With `path`:** The file column named by `path`.
- **Without `path`:** The file column with the same name as the declared column.

Matching is exact and case-sensitive, and it works the same way whether `dynamic` is `true` or `false`. File columns are named as described in [Schema sources by file format](#schema-sources-by-file-format). Two formats need extra care:

- **CSV and TSV without a header row:** Columns are named by position. To read the third field as `status_code`, set its `path` to `col2`.
- **NDJSON:** A dotted name such as `user.id` matches either a nested key or a flat key with that name.

When a declared column can't be found, it reads as null for each file that lacks it, and the response includes a warning. This applies whether `dynamic` is `true` or `false`.

For Parquet, a declared type must match the file's type or be one it can be converted to. An incompatible type makes the query fail. With `dynamic: false`, this check uses one file. With `dynamic: true`, it uses the merged schema.

{applies_to}`stack: experimental 9.6+` If another file has a type that can't be read as the declared type, [`error_mode`](esql-data-federation-dataset-settings.md#error-mode) decides, as described in [How `first_file_wins` handles type mismatches](#first-file-wins-type-mismatches). With `dynamic: true` and `schema_resolution` set to `union_by_name`, the types of every file are known when the query is planned, so `fail_fast` fails the query then, whether or not the query reads the column.
