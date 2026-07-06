```yaml {applies_to}
serverless: ga
stack: ga 9.4.0
```

The `TS_INFO` [processing command](/reference/query-languages/esql/commands/processing-commands.md) retrieves
information about individual time series available in
[time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md),
along with the dimension values that identify each series.

`TS_INFO` is a more fine-grained variant of
[`METRICS_INFO`](/reference/query-languages/esql/commands/metrics-info.md). Where `METRICS_INFO` returns one row
per distinct metric, `TS_INFO` returns one row per metric **and** time series combination. This
lets you discover the exact dimension values (labels) that identify each series. Like
`METRICS_INFO`, any [`WHERE`](/reference/query-languages/esql/commands/where.md) filters that precede
`TS_INFO` narrow the set of time series considered.

## Syntax

```esql
TS_INFO
```

## Parameters

:::{note}
`TS_INFO` takes no parameters.
:::

## Description

`TS_INFO` produces one row for every (metric, time series) combination that matches the
preceding filters. It includes all columns from
[`METRICS_INFO`](/reference/query-languages/esql/commands/metrics-info.md), plus a `dimensions` column
containing a JSON-encoded representation of the dimension key/value pairs that identify the
time series.

The output contains the following columns, all of type `keyword`:

`metric_name`
:   The name of the metric field (single-valued).

`data_stream`
:   The data stream(s) that contain this metric (multi-valued when the metric is included in multiple data streams
    which align on the unit, metric type, and field type).

`unit`
:   The [unit](/reference/elasticsearch/mapping-reference/mapping-field-meta.md) declared in the field mapping,
    such as `bytes` or `packets` (multi-valued when definitions differ across backing indices;
    may be `null` if no unit is declared).

`metric_type`
:   The metric type, for example `counter` or `gauge` (multi-valued when definitions differ across backing indices).

`field_type`
:   The Elasticsearch field type, for example `long`, `double`, or `integer` (multi-valued when definitions differ
    across backing indices).

`dimension_fields`
:   The dimension field names associated with this metric (multi-valued). The union of dimension
    keys across all time series for that metric.

`dimensions`
:   A JSON-encoded object containing the dimension key/value pairs that identify the time series (single-valued).
    For example: `{"job":"elasticsearch","instance":"instance_1"}`.

### Restrictions

- `TS_INFO` can only be used after a [`TS`](/reference/query-languages/esql/commands/ts.md) source command.
  Using it after `FROM` or other source commands produces an error.
- `TS_INFO` must appear before pipeline-breaking commands such as
  [`STATS`](/reference/query-languages/esql/commands/stats-by.md),
  [`SORT`](/reference/query-languages/esql/commands/sort.md), or
  [`LIMIT`](/reference/query-languages/esql/commands/limit.md).
- The output replaces the original table: downstream commands operate on the metadata rows, not the
  raw time series documents.

## Examples

### List all metric–time-series combinations

Return every (metric, time series) pair in the targeted data stream, sorted by metric name and
dimension values:

:::{include} ../examples/ts-info.csv-spec/tsInfoBasic.md
:::

### Discover series matching a filter

Place a [`WHERE`](/reference/query-languages/esql/commands/where.md) clause before `TS_INFO` to restrict
the time series considered. Only metrics and series with matching data are returned:

:::{include} ../examples/ts-info.csv-spec/tsInfoDiscover.md
:::

### Select specific columns

Use [`KEEP`](/reference/query-languages/esql/commands/keep.md) to return only the columns you need:

:::{include} ../examples/ts-info.csv-spec/tsInfoKeep.md
:::

### Filter by metric type

Use [`WHERE`](/reference/query-languages/esql/commands/where.md) after `TS_INFO` to narrow results by
metadata:

:::{include} ../examples/ts-info.csv-spec/tsInfoFilterByGauge.md
:::

### Count distinct time series per metric

Combine with [`STATS`](/reference/query-languages/esql/commands/stats-by.md) to count how many
time series exist for each metric:

:::{include} ../examples/ts-info.csv-spec/tsInfoCountByMetric.md
:::

### Count distinct metrics per time series

Find out how many different metrics each time series reports. This can help identify series that
report an unusually small or large number of metrics:

:::{include} ../examples/ts-info.csv-spec/tsInfoCountDistinctByDimensions.md
:::
