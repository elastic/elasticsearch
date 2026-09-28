---
navigation_title: "Partition spec"
description: "Map a file-column filter onto path keys so ES|QL Data Federation can skip folders that cannot overlap the query."
applies_to:
  stack: experimental 9.6+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Project a file column onto path keys

[`partition_detection`](esql-data-federation-datasets.md#common-settings) and
[`partition_path`](esql-data-federation-datasets.md#common-settings) name the keys in the object path. They do
not say how a payload clock projects onto those keys. `partition_spec` is that overlay: a filter on a file
column can skip folders whose UTC interval cannot overlap the filter.

:::{include} _snippets/data-federation/experimental-warning.md
:::

Unlisted path keys stay identity. `WHERE year == 2024` still skips other years when a spec is set, and
`WHERE region == "eu"` still skips other regions on a VPC tree whose spec only binds `year`, `month`, and
`day`. A spec is not a list of the only keys that may prune. Remaps stay explicit: `aws-region=region` is how
a filter on `region` matches the folder `aws-region`.

## Syntax

The value is a comma-separated list of binds:

```text
[key=]transform(column[, unit])
key=column
column
```

`transform` is one of `identity`, `year`, `month`, `day`, `hour`. `unit` is `second`, `millis`, or `micros`
(default `millis`) and applies only to temporal transforms. Transforms and units are case-insensitive. Keys
and column names are case-sensitive.

Omitted `key=` uses the transform name (`year(ts)` binds path `year`). Bare `region` is `identity(region)`.
`{second}` in `partition_path` is a folder name, not this unit. Refer to
[Resource patterns](esql-data-federation-patterns.md#brace-groups-and-partition-placeholders).

When `partition_path` is set, every spec key must be a `{name}` placeholder in that template. A Hive key is
not known until list time. A bind whose key was not detected is ignored and the query emits a warning. The
query does not drop folders from that bind.

Omit `partition_spec` to keep path-key filters and skip this overlay. That is the default.
[`partition_detection`](esql-data-federation-datasets.md#common-settings) set to `none` turns path keys off.
A spec in that mode is rejected. `template` keeps `partition_path` and does not read Hive `key=value` names.
`hive` reads those names only.

## What skips, and where

An open filter such as `WHERE ts > T` skips folders at **split** time. Listing rewrites a `year IN (...)`
glob only when the filter range has both ends, or when the filter is already `year ==` or `year IN`. Several
temporal binds on the same column are one **joint** overlap at the finest declared grain. A folder is kept
when its UTC interval overlaps the filter. Independent `year >= 2024 AND month >= 3` is not what the spec
does: that would drop January of a later year. An exclusive end that lands on a grain boundary does not
include the next folder. Temporal columns are read as UTC.

`YEAR(ts) > 2024` with `year(ts)` in the spec inverts to a range on `ts`, then that range overlaps the
folders. The same query **without** a spec still returns the 2025 rows and opens every file. `YEAR(ts)` does
not become `WHERE year > 2024`. `MONTH(ts) == 6` does not invert: the scan opens every file and the row
filter keeps June. A literal is the other direction: `year == YEAR("2024-01-01")` folds to `year == 2024`
and prunes the path key.

CloudTrail and VPC Flow Logs often land under a **delivery** date that lags the event time in the file. A
filter on the payload clock can miss a folder that still holds matching rows. Widen the range, or filter the
path keys, when the tree is organized by delivery.

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

`account` and `region` are not in the spec, so they stay identity. `WHERE region == "eu-west-1"` still
prunes those folders.

Renamed folders need an explicit key. `yyy=year(ts), mo=month(ts)` with `partition_path: {yyy}/{mo}` maps
the payload clock onto those names. `year(ts)` alone would look for a key named `year` and miss `yyy`.

## Ladder

Each row is a shape the engine is tested for. Rows and files scanned are both checked.

| | Query | Spec | What is skipped |
|---|---|---|---|
| 1 | `WHERE year == 2024` | none | Other years. Layout identity. No spec. |
| 2 | `WHERE year == 2024` | `year(ts), month(ts), day(ts)` | Same as row 1. The spec does not turn this off. |
| 3 | `WHERE region == "EU"` | `aws-region=region` | Other `aws-region` folders. |
| 4 | `WHERE ts > T` crossing a year | `year(ts), month(ts), day(ts)` | Folders whose UTC interval misses the range. Split, not a listing `year IN`. |
| 5 | `WHERE start > T` | `year(start, second), month(start, second), day(start, second)` | Same joint overlap. `start` is unix seconds. |
| 6 | `WHERE year == YEAR("2024-01-01")` | `year(ts), month(ts), day(ts)` | Listing fold to `year == 2024`. `DATE_EXTRACT("year", ...)` on a literal is the same fold. |
| 7 | `WHERE YEAR(ts) > 2024` | `year(ts), month(ts), day(ts)` | 2025 folders, after invert. Without the spec, the same rows, every file opened. |
| 8 | `WHERE MONTH(ts) == 6` | `year(ts), month(ts), day(ts)` | Nothing. Every file is opened. June rows remain. |
| 9 | any filter on `ts` | `year(ts)` but the path key is `yyy` | No prune from that bind. Warning: the key was not detected. |
| 10 | `WHERE start > T` | `year(start)` on unix seconds (default unit `millis`) | No false prune. Warning: the unit is likely wrong. |
| 11 | `WHERE ts > T` | `yyy=year(ts), mo=month(ts)` and `partition_path: {yyy}/{mo}` | Same joint overlap as row 4, on the renamed keys. |
