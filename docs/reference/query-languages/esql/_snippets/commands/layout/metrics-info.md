```yaml {applies_to}
serverless: ga 9.4.0
stack: ga 9.4.0
```

The `METRICS_INFO` [processing command](/reference/query-languages/esql/commands/processing-commands.md) returns one
row per distinct metric in a [time series data stream](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md),
with metadata about each metric.

## Syntax

```esql
METRICS_INFO
```

## Parameters

`METRICS_INFO` takes no parameters.

## Description

`METRICS_INFO` extracts metric metadata from the time series indices targeted by the
[`TS`](/reference/query-languages/esql/commands/ts.md) source command. It produces one row per distinct metric
signature — that is, per unique combination of metric name and its properties across backing indices.

The output contains the following columns, all of type `keyword`:

`metric_name`
:   The name of the metric field (single-valued).

`data_stream`
:   The data stream(s) that contain this metric (multi-valued when the metric spans multiple data streams).

`unit`
:   The unit declared in the field mapping, such as `bytes` or `packets` (multi-valued when backing indices
    differ; may be `null` if no unit is declared).

`metric_type`
:   The metric type, for example `counter` or `gauge` (multi-valued when definitions differ across backing indices).

`field_type`
:   The Elasticsearch field type, for example `long`, `double`, or `integer` (multi-valued when definitions differ).

`dimension_fields`
:   The dimension field names associated with this metric (multi-valued).

### Restrictions

- `METRICS_INFO` can only be used after a [`TS`](/reference/query-languages/esql/commands/ts.md) source command.
  Using it after `FROM` or other source commands produces an error.
- `METRICS_INFO` must appear before pipeline-breaking commands such as
  [`STATS`](/reference/query-languages/esql/commands/stats-by.md),
  [`SORT`](/reference/query-languages/esql/commands/sort.md), or
  [`LIMIT`](/reference/query-languages/esql/commands/limit.md).
- The output replaces the original table — downstream commands operate on the metadata rows, not the
  raw time series documents.

## Examples

### List all metrics

```esql
TS k8s
| METRICS_INFO
| SORT metric_name
```

### Select specific columns

Use [`KEEP`](/reference/query-languages/esql/commands/keep.md) to return only the columns you need:

```esql
TS k8s
| METRICS_INFO
| KEEP metric_name, metric_type
| SORT metric_name
```

### Filter by metric type

Use [`WHERE`](/reference/query-languages/esql/commands/where.md) to narrow results to a specific metric type:

```esql
TS k8s
| METRICS_INFO
| WHERE metric_type == "counter"
| SORT metric_name
```

### Filter by metric name pattern

```esql
TS k8s
| METRICS_INFO
| WHERE metric_name LIKE "network.eth0*"
| SORT metric_name
```

### Count distinct metrics

Combine with [`STATS`](/reference/query-languages/esql/commands/stats-by.md) to aggregate the metadata:

```esql
TS k8s
| METRICS_INFO
| STATS metric_count = COUNT_DISTINCT(metric_name)
```

### Count metrics by type

```esql
TS k8s
| METRICS_INFO
| STATS metric_count = COUNT(*) BY metric_type
| SORT metric_type
```
