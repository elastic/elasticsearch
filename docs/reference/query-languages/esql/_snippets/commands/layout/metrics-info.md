```yaml {applies_to}
serverless: ga
stack: ga 9.4.0
```

The `METRICS_INFO` [processing command](/reference/query-languages/esql/commands/processing-commands.md) retrieves
information about the metrics available in
[time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md),
along with their applicable dimensions and other metadata.

Use `METRICS_INFO` to discover which metrics exist, what types and units they have, and which
dimensions apply to them without having to inspect index mappings or rely on the field
capabilities API. Any [`WHERE`](/reference/query-languages/esql/commands/where.md) filters that precede
`METRICS_INFO` narrow the set of time series considered, so only metrics with matching data are
returned.

## Syntax

```esql
METRICS_INFO
```

## Parameters

:::{note}
`METRICS_INFO` takes no parameters.
:::

## Description

`METRICS_INFO` produces one row per distinct metric signature — that is, per unique combination
of metric name and its properties across backing indices. When the same metric is defined with
different properties (for example, different units) in different data streams, separate rows are
returned for each variant.

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

### Restrictions

- `METRICS_INFO` can only be used after a [`TS`](/reference/query-languages/esql/commands/ts.md) source command.
  Using it after `FROM` or other source commands produces an error.
- `METRICS_INFO` must appear before pipeline-breaking commands such as
  [`STATS`](/reference/query-languages/esql/commands/stats-by.md),
  [`SORT`](/reference/query-languages/esql/commands/sort.md), or
  [`LIMIT`](/reference/query-languages/esql/commands/limit.md).
- The output replaces the original table: downstream commands operate on the metadata rows, not the
  raw time series documents.

## Examples

### List all metrics

Return every metric available in the targeted time series data stream, sorted alphabetically by
name:

:::{include} ../examples/metrics-info.csv-spec/metricsInfoBasic.md
:::

### Discover metrics matching a filter

Place a [`WHERE`](/reference/query-languages/esql/commands/where.md) clause before `METRICS_INFO` to
restrict the time series considered. Only metrics that have actual data matching the filter are
returned:

:::{include} ../examples/metrics-info.csv-spec/metricsInfoDiscover.md
:::

### Select specific columns

Use [`KEEP`](/reference/query-languages/esql/commands/keep.md) to return only the columns you need:

:::{include} ../examples/metrics-info.csv-spec/metricsInfoKeep.md
:::

### Filter by metric type

Use [`WHERE`](/reference/query-languages/esql/commands/where.md) after `METRICS_INFO` to narrow results
by metadata, for example to only counter metrics:

:::{include} ../examples/metrics-info.csv-spec/metricsInfoFilterByType.md
:::

### Filter by metric name pattern

Use a `LIKE` pattern after `METRICS_INFO` to find metrics whose name matches a prefix or
wildcard. This is useful for exploring a specific subsystem when you know part of the metric
name:

:::{include} ../examples/metrics-info.csv-spec/metricsInfoFilterByName.md
:::

### Count matching metrics

Combine with [`STATS`](/reference/query-languages/esql/commands/stats-by.md) to aggregate the metadata.
For example, count distinct metrics whose name matches a pattern:

:::{include} ../examples/metrics-info.csv-spec/metricsInfoCountMatching.md
:::

### Count metrics by type

Group the metric catalogue by `metric_type` to see how many counter, gauge, or other metrics
exist:

:::{include} ../examples/metrics-info.csv-spec/metricsInfoCountByType.md
:::
