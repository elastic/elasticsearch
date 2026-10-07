---
navigation_title: "Dataset settings"
description: "Reference for ES|QL Data Federation dataset settings, including parsing, file discovery, error handling, and schema resolution."
applies_to:
  stack: experimental 9.5+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Dataset settings reference for {{esql}} Data Federation

Dataset settings control how the files in a dataset are discovered, parsed, and reconciled. Add settings to the `settings` object when you [create or update a dataset](esql-data-federation-manage-datasets.md#create-or-update-a-dataset).

:::{include} _snippets/data-federation/experimental-warning.md
:::

## Common settings

The following settings apply to every file-based data source, unless an entry lists specific formats.

% hive_partitioning intentionally omitted. It is being deprecated to a warn-only no-op in https://github.com/elastic/esql-planning/issues/1881

### Format and discovery

These settings select the format reader and filter the objects that wildcard discovery returns.

$$$format$$$

`format`
:   The file format reader used for the dataset.

    - **Default:** Inferred when the resource pattern implies exactly one format. Required otherwise.
    - **Valid values:** `parquet`, `csv`, `tsv`, `ndjson`, or `auto` to infer the format from the resource pattern

    Set `format` for extensionless resources and for patterns that match more than one format. An explicit format sends objects with unrecognized extensions through this reader, but objects that map to a different registered format are rejected.

    :::{dropdown} Compressed files with an explicit format
    An explicit `format` selects the reader but keeps compression detection. For example, `"format": "csv"` over `hits.csv.gz` still decompresses the file before reading it as CSV.

    :::

$$$file-exclusions$$$

`file_exclusions` {applies_to}`stack: experimental 9.6+`
:   Patterns that name objects to drop from wildcard discovery.

    - **Default:** `["**/_*", "**/.*", "**/_temporary/**", "**/_delta_log/**"]`
    - **Valid values:** An array of patterns in the [resource pattern language](esql-data-federation-patterns.md), or `[]` to turn off exclusion
    - **Related:** `resource`

    Setting `file_exclusions` replaces the default list. To keep the defaults, include them in your list. For matching rules and examples, refer to [exclude non-data objects](esql-data-federation-patterns.md#exclude-non-data-objects).

### Partitions

These settings control how partition columns are derived from object paths.

$$$partition-detection$$$

`partition_detection`
:   How partition columns are derived from directory names.

    - **Default:** `auto`
    - **Valid values:**
      - `auto`: Reads Hive `key=value` directory names. When `partition_path` is set, uses that template for paths that don't use `key=value`.
      - `hive`: Reads Hive `key=value` directory names only.
      - `template`: Names partition columns from `partition_path`.
      - `none`: Turns off partition detection.
    - **Requires:** `partition_path` when set to `template`
    - **Conflicts with:**
      - `partition_path` when set to `hive` or `none`
      - {applies_to}`stack: experimental 9.6+` `partition_spec` when set to `none`
    - **Related:** `partition_path`, `partition_spec`, `partition_sample_size`

$$$partition-path$$$

`partition_path`
:   A template that names partition columns for paths that don't use `key=value` directories.

    - **Default:** None
    - **Valid values:** A path template that uses `{column}` placeholders, for example `{year}/{month}`
    - **Conflicts with:** `partition_detection` set to `hive` or `none`
    - **Related:** `partition_detection`, `partition_spec`

    Each placeholder labels one path segment. For example, `{year}/{month}` extracts `year` and `month` columns from a two-level path. The default `partition_detection` of `auto` reads Hive directory names first and uses the template for other paths. For placeholder syntax, refer to [define partition paths](esql-data-federation-patterns.md#define-partition-paths).

$$$partition-spec$$$

`partition_spec` {applies_to}`stack: experimental 9.6+`
:   Maps file columns to partition keys, so that filters on those columns can skip folders.

    - **Default:** None
    - **Valid values:** A comma-separated list of bindings, each in one of these forms:
      - `[key=]transform(column[, unit])`: A temporal or identity transform. `transform` is `identity`, `year`, `month`, `day`, or `hour`. `unit` is `epoch_second` or `epoch_millis`, and applies only to temporal transforms. The default unit is `epoch_millis`. Unit names follow the [date format](/reference/elasticsearch/mapping-reference/mapping-date-format.md) names.
      - `key=column`: Maps a column to a differently named key.
      - `column`: Maps a column to the key with the same name.
    - **Requires:** Each key to be a `{name}` placeholder in `partition_path`, when `partition_path` is set
    - **Conflicts with:** `partition_detection` set to `none`
    - **Related:** `partition_detection`, `partition_path`

    For syntax, examples, and how folders are skipped, refer to [Skip folders with file column filters](esql-data-federation-partition-spec.md).

    :::{dropdown} Behavior at query time
    A binding whose key isn't detected in the folder paths is ignored, and the query returns a warning.<br><br>Transform and unit names are case-insensitive. Keys and column names are case-sensitive.

    :::

$$$partition-sample-size$$$

`partition_sample_size` {applies_to}`stack: experimental 9.6+`
:   The number of file paths read to infer partition columns and their types.

    - **Default:** `1000`
    - **Valid values:** An integer from `1` through `10000000`
    - **Related:** `partition_detection`, `schema_resolution`

    The sample is the first paths in listing order, not a random selection. Raise the value when some partition values first appear later in the listing, so that those values get a column.

    The sample applies only to a query that reads no rows, and only when the listing covers the whole dataset in the store's own order. In the following cases, every file is listed and the sample size has no effect:

    - The query reads rows.
    - The query filters on a partition column or on `_file.*`.
    - The dataset sets `file_sort_by` or `file_order` to a value other than the default.
    - The dataset uses `union_by_name` or `strict` schema resolution.

### Schema resolution [schema-resolution-settings]

These settings control how schemas are combined when a dataset spans multiple files. For concepts and examples, refer to [schema inference](esql-data-federation-schema.md).

$$$schema-resolution$$$

`schema_resolution`
:   The strategy for reconciling schemas across multiple files.

    - **Default:**
      - {applies_to}`stack: experimental 9.6+` `first_file_wins`
      - {applies_to}`stack: experimental =9.5` `union_by_name`
    - **Valid values:** `first_file_wins`, `union_by_name`, `strict`
    - **Related:** `file_sort_by`, `file_order`

    To compare strategies and learn how existing datasets resolve a missing value, refer to [choose a schema resolution strategy](esql-data-federation-schema.md#choose-a-schema-resolution-strategy).

$$$file-sort-by$$$

`file_sort_by` {applies_to}`stack: experimental 9.6+`
:   The value used to order files when choosing which file supplies the schema.

    - **Default:** `list`
    - **Valid values:** `list`, `name`, `mtime`
    - **Requires:** An effective `schema_resolution` of `first_file_wins`
    - **Related:** `file_order`

    For how each value orders files, refer to [control which file supplies the schema](esql-data-federation-schema.md#control-which-file-supplies-the-schema).

$$$file-order$$$

`file_order` {applies_to}`stack: experimental 9.6+`
:   The sort direction for `file_sort_by`.

    - **Default:** `asc`
    - **Valid values:** `asc`, `desc`
    - **Requires:** An effective `schema_resolution` of `first_file_wins`
    - **Related:** `file_sort_by`

    The direction also applies when `file_sort_by` is `list`. In that case, `desc` reverses the declaration or listing order.

### Error handling

These settings control how malformed rows are handled and how many are tolerated before the query fails.

$$$error-mode$$$

`error_mode`
:   How malformed rows are handled.

    - **Default:** `fail_fast`
    - **Valid values:**
      - `fail_fast`: Fails the query at the first malformed row.
      - `skip_row`: Drops each malformed row.
      - `null_field`: Replaces a value that fails to parse with null and keeps the row.
    - **Related:** `max_errors`, `max_error_ratio`

    {applies_to}`stack: experimental 9.6+` Under `null_field`, a multi-valued cell loses only the values that fail to parse, and is null only when none of its values can be read.

    :::{dropdown} When `null_field` drops rows
    `null_field` keeps a row only when the failure can be attributed to a single value. This applies to every format, including Parquet. When a failure affects the row's structure, `null_field` drops the row, as `skip_row` does. For example, an NDJSON line that isn't valid JSON is dropped, and so is a CSV row that can't be split into fields.

    :::

$$$max-errors$$$

`max_errors`
:   The maximum number of malformed rows allowed before the query fails.

    - **Default:** Unlimited
    - **Valid values:** A non-negative integer
    - **Requires:** {applies_to}`stack: experimental 9.6+` An explicit `error_mode` of `skip_row` or `null_field`
    - **Conflicts with:** `error_mode` set to `fail_fast`
    - **Related:** `max_error_ratio`

$$$max-error-ratio$$$

`max_error_ratio`
:   The maximum fraction of malformed rows allowed before the query fails.

    - **Default:** `0.0`, which applies no ratio limit
    - **Valid values:** A number from `0.0` through `1.0`
    - **Requires:** {applies_to}`stack: experimental 9.6+` An explicit `error_mode` of `skip_row` or `null_field`
    - **Conflicts with:** `error_mode` set to `fail_fast`
    - **Related:** `max_errors`

$$$error-limits-without-error-mode$$$

:::{dropdown} Error limits without an explicit error mode
:applies_to: stack: experimental 9.6+
Creating or updating a dataset with `max_errors` or `max_error_ratio` but no `error_mode` is rejected. A dataset stored with an error limit and no `error_mode` before this requirement took effect continues to read as `skip_row`. So does a `FROM EXTERNAL` query that sets an error limit without `error_mode`. In both cases, the response includes a `Warning` header that identifies the inferred mode.
:::

### Splitting and parallelism

These settings control how files are divided into splits that nodes read in parallel.

$$$target-split-size$$$

`target_split_size`
:   The target size of each unit of work that a file is divided into for parallel reading across nodes.

    - **Default:** 64 MiB (`64mb`)
    - **Valid values:** A positive byte size, for example `32mb`
    - **Related:** `split_probe_window`, `max_split_probes`

    Files larger than the target are cut into several splits. Files smaller than the target are read as a single split. Lower the value for more parallelism over a few large files. Raise it to reduce planning work when a dataset contains a very large number of bytes.

$$$split-probe-window$$$

`split_probe_window` {applies_to}`stack: experimental 9.6+`
:   The number of bytes that each record-boundary search can read while files are split.

    - **Formats:** NDJSON, and CSV and TSV without quoting or escaping
    - **Default:** 256 KiB (`256kb`)
    - **Valid values:** A positive byte size. The product of `max_split_probes` and `split_probe_window` can't exceed 4 GiB.
    - **Related:** `max_split_probes`, `target_split_size`

    If a dataset's records are longer than this value, its files are cut into fewer splits than `target_split_size` requests, which reduces parallelism. Raise the value for datasets with long records. If the pair is rejected, lower one of the two probe settings.

    :::{dropdown} How record-boundary searches work
    A search that doesn't reach the end of a record finds no boundary, so the file can't be split at that offset.<br><br>`max_split_probes` sets how many searches a query runs, and `split_probe_window` sets how many bytes each search reads. Their product is the number of bytes a query can read while searching. With the default values, that is 1000 searches of 256 KiB, or about 250 MiB. Size the window from the dataset's longest record, and size `max_split_probes` from the number of splits the scan needs.<br><br>Values below about 136 KiB are read in full by every search, because finishing a small window costs less than opening another connection. Lowering the value below that point doesn't reduce the bytes each search reads.<br><br>Quoted or escaped CSV and TSV can't be searched at a fixed offset, so record boundaries in those files are found by reading them sequentially. That sequential read is bounded by its own convergence behavior and by the `external_max_record_size` query pragma, not by `split_probe_window` or `max_split_probes`.

    :::

$$$max-split-probes$$$

`max_split_probes` {applies_to}`stack: experimental 9.6+`
:   The maximum number of record-boundary searches that a query can perform, which limits how many splits its files are cut into.

    - **Formats:** NDJSON, and CSV and TSV without quoting or escaping
    - **Default:** `1000`
    - **Valid values:** An integer from `1` through `10000`. The product of `max_split_probes` and `split_probe_window` can't exceed 4 GiB.
    - **Related:** `split_probe_window`, `target_split_size`

    When a scan needs more splits than this value allows, the scan uses a larger split size than `target_split_size` requests. Raise the value to get the requested split size on a very large scan.

    :::{dropdown} How searches map to splits
    Each searched file yields one more split than the number of searches spent on it. A file too small to search is read as a single whole-file split and uses no searches.

    :::

## Storage-specific settings

The following settings apply only to data sources that use a specific storage provider.

### Amazon S3

These settings apply to datasets whose data source uses Amazon S3 or an S3-compatible store.

$$$amazon-s3-region$$$

`region` {applies_to}`stack: experimental 9.6+`
:   The AWS region used for the S3 client, for example `eu-central-1`.

    - **Default:** Auto-detected
    - **Valid values:** A non-empty AWS region name
    - **Related:** `endpoint` and `sts_region` on the [data source](esql-data-federation-sources.md)

    Omit `region` for standard AWS S3. Set it when the data source uses a custom `endpoint`, such as MinIO or Scaleway, to skip region discovery on the first request.

    :::{dropdown} Region discovery and federated identity
    Without an `endpoint`, the AWS SDK redirects requests to the bucket's region. With an `endpoint`, a `HeadBucket` request on first access discovers the region, and the result is cached for the lifetime of the data source. An explicit `region` is always used as set. A wrong value returns an error instead of redirecting.<br><br>With `auth: federated_identity`, `region` also selects the STS regional endpoint for role assumption, unless the data source sets `sts_region`. When neither is set, STS uses `us-east-1`. This works for standard commercial AWS but can fail for buckets in other AWS partitions, such as GovCloud or China.

    :::

## CSV and TSV settings

The following settings apply to CSV and TSV files.

### Commonly changed CSV and TSV settings

These settings cover the field separator, quoting style, header handling, and null tokens that most CSV and TSV files need.

$$$csv-delimiter$$$

`delimiter`
:   The field separator.

    - **Default:** `,` for CSV, `\t` for TSV
    - **Valid values:**
      - A single ASCII character other than a line feed or carriage return. Write a tab as `\t` and a backslash as `\\`.
      - {applies_to}`stack: experimental 9.6+` Multi-character values are rejected when you create or update the dataset.
    - **Conflicts with:** The `quote` character when quoting is on, and the `escape` character when escaping is on
    - **Related:** `quote`, `escape`

$$$csv-mode$$$

`mode`
:   A preset that sets quoting and escaping together.

    - **Default:** `quoted` for CSV, `plain` for TSV
    - **Valid values:**
      - `quoted`: Fields can be wrapped in quotes. An embedded quote is doubled, and a backslash escapes characters inside a quoted field.
      - `escaped`: No quoting. A backslash escapes special characters, and `\N` reads as null.
      - `plain`: No quoting or escaping. Every byte is literal, so a field can't contain the delimiter or a newline.
    - **Conflicts with:** {applies_to}`stack: experimental 9.6+` An explicit `quote` when set to `escaped`
    - **Related:** `quote`, `escape`

    An explicit `quote` or `escape` value overrides the preset.

    :::{dropdown} Why `escaped` with `quote` is rejected
    Setting `quote` turns quoting on, and the quoting parser doesn't decode escape sequences. The combination therefore reads neither as escaped nor as quoted data, so it's rejected when you create or update the dataset. A dataset stored with this combination before the check took effect continues to read.

    :::

$$$csv-header-row$$$

`header_row`
:   Whether the first record that isn't blank or a comment names the columns.

    - **Default:** `true`
    - **Valid values:** `true`, `false`
    - **Related:** `skip_rows`, `column_prefix`

    `header_row` is applied after `skip_rows`.

$$$csv-skip-rows$$$

`skip_rows` {applies_to}`stack: experimental 9.6+`
:   The number of leading content records to discard from each file.

    - **Default:** `0`
    - **Valid values:** An integer from `0` through `1000`
    - **Related:** `header_row`, `comment`

    Records are discarded after decompression and before `header_row` is applied. Blank lines and comment lines don't count toward `skip_rows`. For example, read a file that starts with two prose lines and then the header `state,ip,user_agent` with `"skip_rows": 2` and `"header_row": true`. A preamble of comment lines is skipped through `comment` without setting `skip_rows`.

$$$csv-null-value$$$

`null_value`
:   The token that reads as null.

    - **Default:** None. No token reads as null.
    - **Valid values:** A string, for example `NULL`, `NA`, or `\N`.

    {applies_to}`stack: experimental 9.6+` An empty string `""` makes empty fields read as null.

$$$csv-encoding$$$

`encoding`
:   The file's character encoding.

    - **Default:** `UTF-8`
    - **Valid values:** A character set name, for example `ISO-8859-1`

### Advanced CSV and TSV settings

These settings tune schema sampling, quoting characters, column naming, value parsing, and field size limits.

$$$csv-schema-sample-size$$$

`schema_sample_size`
:   The number of rows sampled to infer the schema.

    - **Default:** `20000`
    - **Valid values:**
      - {applies_to}`stack: experimental 9.6+` An integer from `1` through `20000`
      - {applies_to}`stack: experimental =9.5` An integer from `1` through `1000`

    The sample determines whether sparse or late-appearing fields get a column. To learn how schemas are inferred, refer to [schema inference](esql-data-federation-schema.md).

$$$csv-quote$$$

`quote`
:   The quote character.

    - **Default:** `"` for CSV. Quoting is off for TSV.
    - **Valid values:**
      - A single ASCII character other than a line feed or carriage return. Write a tab as `\t` and a backslash as `\\`.
      - `none` to turn off quoting
      - {applies_to}`stack: experimental 9.6+` Multi-character values are rejected when you create or update the dataset.
    - **Conflicts with:**
      - The `delimiter` character
      - The `escape` character, when escaping is on
      - {applies_to}`stack: experimental 9.6+` `mode` set to `escaped`
    - **Related:** `mode`, `escape`, `delimiter`

    An explicit value overrides the `mode` preset.

$$$csv-escape$$$

`escape`
:   The escape character.

    - **Default:** `\` for CSV. Escaping is off for TSV.
    - **Valid values:**
      - A single ASCII character other than a line feed or carriage return. Write a tab as `\t` and a backslash as `\\`.
      - `none` to turn off escaping
      - {applies_to}`stack: experimental 9.6+` Multi-character values are rejected when you create or update the dataset.
    - **Conflicts with:**
      - The `delimiter` character
      - The `quote` character, when quoting is on
    - **Related:** `mode`, `quote`, `delimiter`

    An explicit value overrides the `mode` preset.

$$$csv-comment$$$

`comment`
:   The prefix that marks a line as a comment to skip.

    - **Default:** `//`
    - **Valid values:** A string

$$$csv-column-prefix$$$

`column_prefix`
:   The prefix for generated column names when `header_row` is `false`.

    - **Default:** `col`
    - **Valid values:** A string
    - **Related:** `header_row`

    Each name ends with a counter that starts at `0`, for example `col0`, `col1`, `col2`. An empty prefix produces numeric column names, which must be quoted with backticks in {{esql}}.

$$$csv-datetime-format$$$

`datetime_format`
:   The pattern used to parse date and time values.

    - **Default:** ISO 8601 or epoch milliseconds
    - **Valid values:** A [date format](/reference/elasticsearch/mapping-reference/mapping-date-format.md) pattern or built-in format name. Combine formats with `||`.

$$$csv-trim-spaces$$$

`trim_spaces`
:   Whether to remove surrounding ASCII whitespace from string field values.

    - **Default:** `false`
    - **Valid values:** `true`, `false`

    Typed values, such as numbers and dates, tolerate surrounding whitespace regardless of this setting.

$$$csv-multi-value-syntax$$$

`multi_value_syntax`
:   Whether bracketed multi-values are recognized.

    - **Default:** `none`
    - **Valid values:**
      - `none`: Reads brackets as literal characters.
      - `brackets`: Reads a field such as `[a,b,c]` as a multi-value.
    - **Requires:** Quoting when set to `brackets`. Without a `mode`, `brackets` selects `quoted`.
    - **Conflicts with:** `mode` set to `escaped` or `plain`, or `quote` set to `none`, when set to `brackets`

$$$csv-max-field-size$$$

`max_field_size`
:   The maximum size of a single field, in bytes.

    - **Default:** 10 MiB (`10485760`)
    - **Valid values:** An integer number of bytes. `0` removes the limit.

## NDJSON settings

The following settings apply to NDJSON files.

### Commonly changed NDJSON settings

This setting controls how much of each file is sampled to infer the schema.

$$$ndjson-schema-sample-size$$$

`schema_sample_size`
:   The number of lines sampled to infer the schema.

    - **Default:** `20000`
    - **Valid values:**
      - {applies_to}`stack: experimental 9.6+` An integer from `1` through `20000`
      - {applies_to}`stack: experimental =9.5` An integer from `1` through `1000`

    The sample determines whether sparse or late-appearing fields get a column. To learn how schemas are inferred, refer to [schema inference](esql-data-federation-schema.md).

    {applies_to}`stack: experimental 9.6+` NDJSON inference skips malformed lines, including lines that repeat a key in the same object, for example `{"a":1,"a":2}`. A malformed line contributes no columns, even for fields it names before parsing fails, and doesn't count toward `schema_sample_size` or `schema_max_fields`. A column that appears only on malformed lines is absent from the schema. When the file is read, those lines are handled according to the dataset's [`error_mode`](#error-mode).

### Advanced NDJSON settings

These settings tune parallel reading, date parsing, and schema size limits for NDJSON files.

$$$ndjson-segment-size$$$

`segment_size`
:   The unit that a file is divided into for parallel reading. The effective segment is a few bytes under the value you set, so that each segment buffer, including its JVM array header, fits within the configured size.

    - **Default:** 4 MiB (`4mb`)
    - **Valid values:** A byte size of at least 64 KiB (`64kb`)

$$$ndjson-datetime-format$$$

`datetime_format`
:   The pattern used to infer and parse date and time values.

    - **Default:** `strict_date_optional_time`
    - **Valid values:** A [date format](/reference/elasticsearch/mapping-reference/mapping-date-format.md) pattern or built-in format name. Combine formats with `||`.

$$$ndjson-schema-max-fields$$$

`schema_max_fields` {applies_to}`stack: experimental 9.6+`
:   The maximum number of fields that schema inference can create from a file.

    - **Default:** `1000`, or the value of the `esql.external.schema_max_fields` [cluster setting](esql-data-federation-cluster-settings.md)
    - **Valid values:** An integer from `1` through `100000`

    Objects count as fields, as well as leaf fields, and each segment of a dotted key counts as a field. If a file's inferred schema exceeds the limit, the query fails.

## Parquet settings

Parquet is self-describing and has no format-specific dataset settings.
