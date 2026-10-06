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

`partition_spec` maps a file column to your folder structure so that filters on the column skip
non-matching folders. Filtering on a partition key such as `WHERE year == 2024` already skips
folders, but filtering on a file column like `WHERE ts > "2024-03-15T00:00:00Z"::datetime` normally
opens every file.

:::{include} _snippets/data-federation/experimental-warning.md
:::

[`partition_detection`](esql-data-federation-datasets.md#common-settings) and
[`partition_path`](esql-data-federation-datasets.md#common-settings) still decide how the folder
names are found.

## Define a partition spec

Set `partition_spec` in your dataset settings as a comma-separated list of bindings. Each binding
maps a path key to a transform on a file column:

```text
[key=]transform(column[, unit])
key=column
column
```

`transform` is one of `identity`, `year`, `month`, `day`, `hour`. `unit` is `second`, `millis`, or `micros`
(default `millis`) and applies only to temporal transforms. Transforms and units are case-insensitive. Keys
and column names are case-sensitive.

Omitted `key=` uses the transform name (`year(ts)` maps path `year`). Bare `region` is `identity(region)`.
`@timestamp` is a legal column name. A name that is not an ES|QL identifier goes in backticks, as in
`` year(`event time`) ``. `{second}` in `partition_path` is a folder name, not this unit. Refer to
[Resource patterns](esql-data-federation-patterns.md#brace-groups-and-partition-placeholders).

Path keys you leave out of the spec still filter on their own name. `WHERE year == 2024` still skips
other years when a spec is set, and `WHERE region == "eu"` still skips other regions when the spec only
maps `year`, `month`, and `day`. The spec is not the list of the only keys that can skip folders. A folder
named `aws-region` needs `aws-region=region` before `WHERE region == "eu"` matches it.

When `partition_path` is set, every spec key must be a `{name}` placeholder in that template. A Hive key is
not known until list time. A binding whose key was not detected is ignored and the query emits a warning.
The query does not skip folders from that binding. A spec that does not parse warns on the query and does
not skip folders. Registration still rejects it.

Omit `partition_spec` to keep path-key filters and skip this mapping. That is the default.
[`partition_detection`](esql-data-federation-datasets.md#common-settings) set to `none` turns path keys off.
A spec in that mode is rejected. `template` keeps `partition_path` and does not read Hive `key=value` names.
`hive` reads those names only.

## How folder pruning works

A filter like `WHERE ts > T` skips folders whose UTC time range falls entirely outside the filter. A filter
with only a start does this while files are chosen. A filter with both a start and an end, or `year ==` /
`year IN`, can also narrow the `year` folders in the listing. When you map several granularities to the
same column (`year(ts), month(ts), day(ts)`), the engine evaluates them as one combined range at the finest
declared grain, not as independent conditions. This means `WHERE ts >= "2024-03-01"` correctly keeps
January 2025, even though month `01` is numerically less than `03`. An exclusive boundary that falls
exactly on a folder boundary does not include the next folder. Temporal columns are read as UTC.

When you write `YEAR(ts) > 2024` and the spec includes `year(ts)`, the engine converts this to a range
on `ts` and uses that range to skip folders. It does not become `WHERE year > 2024`. Without the spec, the
query returns the same rows but opens every file. `MONTH(ts) == 6` does not convert because June appears
in every year: the query opens every file and filters rows after reading. A literal like
`year == YEAR("2024-01-01")` folds to `year == 2024` and prunes the path key directly.

## Account for a delivery date

CloudTrail and VPC Flow Logs often land under a **delivery** date that lags the event time in the file. A
filter on the event-time column can miss a folder that still holds matching rows. Widen the range, or
filter the path keys directly, when the folder tree is organized by delivery time.

The following example registers a VPC Flow Logs dataset and maps the `start` column (unix seconds) to date folders.

```console
PUT /_query/dataset/vpc_flow
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs/AWSLogs/*/*/*/*/*/*.gz",
  "settings": {
    "format": "csv",
    "partition_path": "{account}/{region}/{year}/{month}/{day}",
    "partition_spec": "year(start, second), month(start, second), day(start, second)"
  }
}
```

`account` and `region` are not in the spec, so they stay on their own names. `WHERE region == "eu-west-1"` still
skips those folders.

When a mapping exposes the file column as `@timestamp`, bind `@timestamp` and omit the unit: the column
is already a date. Binding `start` matches nothing after the rename; `start` is no longer in the query.

```console
PUT /_query/dataset/vpc_flow
{
  "data_source": "prod_s3_logs",
  "resource": "s3://logs/AWSLogs/*/*/*/*/*/*.gz",
  "settings": {
    "format": "csv",
    "partition_path": "{account}/{region}/{year}/{month}/{day}",
    "partition_spec": "year(@timestamp), month(@timestamp), day(@timestamp)"
  },
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date", "path": "start", "format": "epoch_second" }
    }
  }
}
```

A time range on `@timestamp` (for example Kibana's time picker) already narrows the year folders in the
listing. Skipping day folders for that same range is a follow-up.

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
| 5 | `WHERE start > T` | `year(start, second), month(start, second), day(start, second)` | Same combined range. `start` is unix seconds. |
| 6 | `WHERE year == YEAR("2024-01-01")` | `year(ts), month(ts), day(ts)` | Becomes `year == 2024` before listing. `DATE_EXTRACT("year", ...)` on a literal does the same. |
| 7 | `WHERE YEAR(ts) > 2024` | `year(ts), month(ts), day(ts)` | 2025 folders. Without the spec, the same rows, every file opened. |
| 8 | `WHERE MONTH(ts) == 6` | `year(ts), month(ts), day(ts)` | Nothing. Every file is opened. June rows remain. |
| 9 | any filter on `ts` | `year(ts)` but the path key is `yyy` | Nothing from that binding. Warning: the key was not detected. |
| 10 | `WHERE start > T` | `year(start)` on unix seconds (default unit `millis`) | Nothing. Warning: the unit is likely wrong. |
| 11 | `WHERE ts > T` | `yyy=year(ts), mo=month(ts)` and `partition_path: {yyy}/{mo}` | Same combined range as row 4, on the renamed keys. |
| 12 | `WHERE @timestamp > T` | `year(@timestamp), month(@timestamp), day(@timestamp)` after mapping `start` to `@timestamp` | Same combined range as row 5. No unit: the column is a date. Binding `start` matches nothing. |
