---
navigation_title: "Skip folders by filtering files"
description: "Use partition_spec to skip folders when filtering on columns inside your files, reducing I/O for time-partitioned datasets."
applies_to:
  stack: experimental 9.6+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Skip folders with file column filters in {{esql}} Data Federation

`partition_spec` binds a file column to your folder structure so that filters on the column skip
non-matching folders. Filtering on a partition key such as `WHERE year == 2024` already skips
folders, but filtering on a file column like `WHERE ts > "2024-03-15T00:00:00Z"` normally
opens every file. An ISO-8601 string compared to a datetime column does not need `::datetime`;
ES|QL casts the keyword literal. `::datetime` is still valid.

:::{include} _snippets/data-federation/experimental-warning.md
:::

[`partition_detection`](esql-data-federation-dataset-settings.md#partition-detection) and
[`partition_path`](esql-data-federation-dataset-settings.md#partition-path) still decide how the folder
names are found.

## Define a partition spec

Set `partition_spec` in your dataset settings as a comma-separated list. A **binding** maps a path
key to a transform on a file column. The `lag` and `lead` functions adjust the listing
window for a column that already has a time-based binding. They do not create path-key bindings:

```text
[key=]transform(column[, unit])
key=column
column
lag(column, duration)
lead(column, duration)
```

`transform` is one of `identity`, `year`, `month`, `day`, `hour`. `unit` is `epoch_second` or `epoch_millis`
(default `epoch_millis`, omit it) and applies only to temporal transforms. Units are epoch values, named like
[Elasticsearch date formats](/reference/elasticsearch/mapping-reference/mapping-date-format.md). Transforms and units are
case-insensitive. Keys and column names are case-sensitive. `duration` is a time value such as `15m`, `1h`, or `90s`.
The same Hive key may appear once per source column (`year(start), year(end)`). `key=lag(...)` is rejected.

Omitted `key=` uses the transform name (`year(ts)` binds path `year`). Bare `region` is `identity(region)`.
`@timestamp` is a legal column name. A name that is not an ES|QL identifier goes in backticks, as in
`` year(`event time`) ``. `{second}` in `partition_path` is a folder name, not this unit. Refer to
[Resource patterns](esql-data-federation-patterns.md#define-partition-paths).

Path keys you leave out of the spec still filter on their own name. `WHERE year == 2024` still skips
other years when a spec is set, and `WHERE region == "eu"` still skips other regions when the spec only
binds `year`, `month`, and `day`. The spec is not the list of the only keys that can skip folders. A folder
named `aws-region` needs the binding `aws-region=region` before `WHERE region == "eu"` matches it.

When `partition_path` is set, every spec key must be a `{name}` placeholder in that template. A Hive key is
not known until list time. A binding whose key was not detected is ignored and the query emits a warning.
The query does not skip folders from that binding. A spec that does not parse warns on the query and does
not skip folders. Registration still rejects it.

Omit `partition_spec` to keep path-key filters and skip these bindings. That is the default.
[`partition_detection`](esql-data-federation-dataset-settings.md#partition-detection) set to `none` turns path keys off.
A spec in that mode is rejected. `template` keeps `partition_path` and does not read Hive `key=value` names.
`hive` reads those names only.

## How folder pruning works

A filter like `WHERE ts > T` skips folders whose UTC time range falls entirely outside the filter. A filter
with only a lower bound skips nonmatching files after Elasticsearch lists them. A range with lower and
upper bounds can also narrow the folders Elasticsearch lists. For each configured time unit (`year`,
`month`, `day`, or `hour`), Elasticsearch creates an `IN` list from UTC calendar values. It omits
this optimization when the list contains every possible value for that unit or more than 64 values.
Elasticsearch can still skip individual files after listing the folders.

When you map multiple time units to the same column (`year(ts), month(ts), day(ts)`), Elasticsearch
evaluates them as one combined range at the smallest configured unit. For example,
`WHERE ts >= "2024-03-01"` keeps January 2025 even though month `01` is less than `03`. An exclusive
boundary that falls exactly on a folder boundary does not include the next folder. Elasticsearch
reads date and time columns as UTC. Path-key filters such as `WHERE year == 2024` remain exact.

When you write `YEAR(ts) > 2024` and the spec includes `year(ts)`, the engine converts this to a range
on `ts` and uses that range to skip folders. It does not become `WHERE year > 2024`. Without the spec, the
query returns the same rows but opens every file. `MONTH(ts) == 6` does not convert because June appears
in every year: the query opens every file and filters rows after reading. A literal like
`year == YEAR("2024-01-01")` folds to `year == 2024` and prunes the path key directly.

## Account for a delivery date

CloudTrail and VPC Flow Logs often land under a **delivery** date that lags the event time in the file. A
filter on the event-time column can miss a folder that contains matching rows. Add
`lag(column, duration)` to extend the listing window after the filter. Add `lead(column, duration)`
to extend it before the filter. For example, `lag(start, 15m)` keeps the next hour or day folder so
that Elasticsearch lists late-arriving rows. `lead` keeps the previous folder. Both functions only
widen the window. They never skip a folder the filter would keep.

### Configure Hive-compatible VPC Flow Logs

The following example registers a Hive-compatible, hourly VPC Flow Logs dataset. The `start` and
`end` fields contain Unix epoch seconds. Each partition in the resource path uses the `key=*` format.
Elasticsearch can continue through keys that are not included in the spec, such as `aws-account-id`
and `aws-region`.

The mapping exposes `start` as `@timestamp`, so the partition spec must bind `@timestamp`. The `PUT`
request rejects `year(start)`. The `end` field is not renamed, so bind `year(end)`. Date fields do not
require a unit. The `lag` function adjusts the listing window but does not create a path-key binding.

```console
PUT /_query/dataset/vpc_flow
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs/AWSLogs/aws-account-id=*/aws-service=vpcflowlogs/aws-region=*/year=*/month=*/day=*/hour=*/*.parquet",
  "settings": {
    "partition_spec": "year(@timestamp), month(@timestamp), day(@timestamp), hour(@timestamp), year(end), month(end), day(end), hour(end), lag(@timestamp, 20m), lag(end, 10m)"
  },
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date", "path": "start", "format": "epoch_second" },
      "end": { "type": "date", "format": "epoch_second" }
    }
  }
}
```

`aws-account-id`, `aws-service`, and `aws-region` stay unlisted identity keys; filter them on those names.
`aws-region=region` (row 3) applies only when the file itself has a `region` column. Default VPC Flow Logs Parquet
does not.

Default text layout uses no `key=value` folders. A segment is a placeholder only when it is exactly `{name}`; a
literal such as `vpcflowlogs` stays a required path segment. VPC Flow Logs have no header, so CSV settings must set
`"header_row": false`. `delimiter` is a single character; space is valid.

With `"header_row": false`, generated names are `col0`, `col1`, … (`column_prefix`). `partition_spec` binds a file
column by that name, so declare `start` in `mappings` with `path` set to its position or prune never fires. Default
VPC Flow Logs put `start` at `col10`.

```console
PUT /_query/dataset/vpc_flow_text
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs/AWSLogs/*/vpcflowlogs/*/*/*/*/*.log.gz",
  "settings": {
    "format": "csv",
    "delimiter": " ",
    "header_row": false,
    "partition_detection": "template",
    "partition_path": "{account}/vpcflowlogs/{region}/{year}/{month}/{day}",
    "partition_spec": "year(start, epoch_second), month(start, epoch_second), day(start, epoch_second)"
  },
  "mappings": {
    "properties": {
      "start": { "type": "long", "path": "col10" }
    }
  }
}
```

### Use mapped date fields

When a mapping exposes the file column as `@timestamp`, bind `@timestamp` and omit the unit because
the column is already a date. If the mapping renames a source column with `path`, the partition spec
must use the mapped field name. For example, when `@timestamp` has `path=start`, use
`year(@timestamp)`. The `PUT` request rejects `year(start)`.

A spec saved before the mapping rename continues to reference `start`, so it cannot prune filters on
`@timestamp`. The query still succeeds and returns a warning.

```console
PUT /_query/dataset/vpc_flow_mapped
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs/AWSLogs/*/vpcflowlogs/*/*/*/*/*.log.gz",
  "settings": {
    "format": "csv",
    "delimiter": " ",
    "header_row": false,
    "partition_detection": "template",
    "partition_path": "{account}/vpcflowlogs/{region}/{year}/{month}/{day}",
    "partition_spec": "year(@timestamp), month(@timestamp), day(@timestamp)"
  },
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date", "path": "col10", "format": "epoch_second" }
    }
  }
}
```

A time range on `@timestamp` (for example Kibana's time picker, or a `request.filter` range) narrows
year, month, day, and hour folders in the listing when those grains are in the spec, and skips day
folders whose UTC interval misses the window. A grain skipped at listing because its `IN` set is
complete or larger than 64 is still pruned after listing.

Renamed folders need an explicit key. `yyy=year(ts), mo=month(ts)` with `partition_path: {yyy}/{mo}` maps
the file column onto those folder names. `year(ts)` alone would look for a key named `year` and miss `yyy`.
`mo=month(ts)` alone also misses: a month folder needs a year, either as `yyy=year(ts)` or as a path key
named `year`. The query warns and opens every folder.

## Check which queries skip folders

The following table shows how different query patterns interact with `partition_spec`.

| | Query | Spec | What is skipped |
|---|---|---|---|
| 1 | `WHERE year == 2024` | none | Other years. No spec. |
| 2 | `WHERE year == 2024` | `year(ts), month(ts), day(ts)` | Same as row 1. The spec does not turn this off. |
| 3 | `WHERE region == "EU"` | `aws-region=region` | Other `aws-region` folders. |
| 4 | `WHERE ts > T` crossing a year | `year(ts), month(ts), day(ts)` | Folders whose UTC time range misses the filter. A start-only filter does not narrow the `year` folders in the listing. |
| 5 | `WHERE start > T` | `year(start, epoch_second), month(start, epoch_second), day(start, epoch_second)` | Same combined range. `start` is unix seconds. |
| 6 | `WHERE year == YEAR("2024-01-01")` | `year(ts), month(ts), day(ts)` | Becomes `year == 2024` before listing. `DATE_EXTRACT("year", ...)` on a literal does the same. |
| 7 | `WHERE YEAR(ts) > 2024` | `year(ts), month(ts), day(ts)` | 2025 folders. Without the spec, the same rows, every file opened. |
| 8 | `WHERE MONTH(ts) == 6` | `year(ts), month(ts), day(ts)` | Nothing. Every file is opened. June rows remain. |
| 9 | any filter on `ts` | `year(ts)` but the path key is `yyy` | Nothing from that binding. Warning: the key was not detected. |
| 10 | `WHERE start > T` | `year(start)` on unix seconds (default unit `epoch_millis`) | Nothing. Warning: the unit is likely wrong. |
| 11 | `WHERE ts > T` | `yyy=year(ts), mo=month(ts)` and `partition_path: {yyy}/{mo}` | Same combined range as row 4, on the renamed keys. |
| 12 | `WHERE @timestamp > T` | `year(@timestamp), month(@timestamp), day(@timestamp)` after mapping `start` to `@timestamp` | Same combined range as row 5. PUT rejects `year(start)` here; bind `@timestamp`. A stored spec on `start` matches nothing. |
| 13 | `request.filter` range on `@timestamp` | `year(@timestamp), month(@timestamp), day(@timestamp)` | Day folders whose UTC interval misses the window. Year folders in the listing too. |
| 14 | `WHERE @timestamp >= T AND @timestamp < U` | `year(@timestamp), month(@timestamp), day(@timestamp), hour(@timestamp)` | Year, month, day, and hour folders outside the adjusted time range. Elasticsearch omits an `IN` filter when it contains all possible values or more than 64 values. |
| 15 | `WHERE @timestamp > "2024-06-15T00:00:00Z"` | `year(@timestamp), month(@timestamp), day(@timestamp)` | Same as with `::datetime`. The keyword ISO literal is cast to datetime. |
