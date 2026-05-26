```yaml {applies_to}


serverless: ga
stack: preview 9.2, ga 9.4
```

The `TS` source command is similar to the [`FROM`](/reference/query-languages/esql/commands/from.md)
source command, with the following key differences:

 - Targets only [time series indices](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md)
 - Enables the use of [time series aggregation functions](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md) inside the
   [STATS](/reference/query-languages/esql/commands/stats-by.md) command

## Syntax

```esql
TS index_pattern [METADATA fields]
```

## Parameters

`index_pattern`
:   A list of indices, data streams or aliases. Supports wildcards and date math.

`fields`
:   A comma-separated list of [metadata fields](/reference/query-languages/esql/esql-metadata-fields.md) to retrieve.

## Description

The `TS` source command enables time series semantics and adds support for
[time series aggregation functions](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md) to the `STATS` command, such as
[`AVG_OVER_TIME()`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/avg_over_time.md),
or [`RATE`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/rate.md).
These functions are implicitly evaluated per time series, then aggregated by group using a secondary aggregation
function. For an example, refer to [Calculate the rate of search requests per host](#calculate-the-rate-of-search-requests-per-host).

This paradigm (a pair of aggregation functions) is standard for time series
querying. For supported inner (time series) functions per
[metric type](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-metric), refer to
[](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md). These functions also
apply to downsampled data, with the same semantics as for raw data.

::::{tip}
To discover which metrics, metric types, and dimensions are available before writing a
`TS` query, use [`METRICS_INFO`](/reference/query-languages/esql/commands/metrics-info.md)
or [`TS_INFO`](/reference/query-languages/esql/commands/ts-info.md).
::::

::::{note}
If a query is missing an inner (time series) aggregation function,
[`LAST_OVER_TIME()`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/last_over_time.md)
is assumed and used implicitly. For example, two equivalent queries that return the average of the last memory usage values per time series are shown in [Aggregate with implicit LAST_OVER_TIME](#aggregate-with-implicit-last_over_time). To calculate the average memory usage across per-time-series averages, refer to [Calculate the average of per-time-series averages](#calculate-the-average-of-per-time-series-averages).
::::

You can use [time series aggregation functions](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md)
directly in the `STATS` command ({applies_to}`stack: preview 9.3`). The output will contain one aggregate value per time series and time bucket (if specified). For an example, refer to [Use time series aggregation functions directly](#use-time-series-aggregation-functions-directly).

You can also combine time series aggregation functions with regular [aggregation functions](/reference/query-languages/esql/functions-operators/aggregation-functions.md) such as `SUM()`, as outer aggregation functions. For examples, refer to [Combine SUM and RATE](#combine-sum-and-rate) and [Combine SUM and AVG_OVER_TIME](#combine-sum-and-avg_over_time).

However, using a time series aggregation function in combination with an inner time series function causes an error. For an example, refer to [Invalid query: nested time series functions](#invalid-query-nested-time-series-functions).

If there is no `STATS` command in the query, the output of the `TS` command gets sorted by `@timestamp` in descending order by default. This helps listing recent values across many time series, as opposed to listing the results based on index sort configuration that may just return data points for a single time series.

## When to use TS vs FROM

`TS` is the right choice for aggregating metrics. It's optimized for time series
data and avoids correctness issues that
[`FROM`](/reference/query-languages/esql/commands/from.md) `| STATS` runs into on
raw metric data. The two most common examples where `FROM` silently produces
wrong results that `TS` handles correctly are counter handling and uneven
publish intervals for metrics.

### Counters need deltas, not raw sums

Counter metrics (such as a `request_count` that monotonically increases) record a
running total, not per-interval events. Summing or averaging the raw counter column
has no meaningful interpretation: the result reflects how many samples each series
happened to publish, not the underlying rate. Counter resets on process restart make
the problem worse by mixing pre-reset and post-reset values:

```esql
FROM metrics | STATS SUM(request_count) BY host
```

`TS` with [`RATE()`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/rate.md)
or [`INCREASE()`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/increase.md)
first computes the per-bucket delta for each time series (handling resets correctly
along the way), then lets you aggregate those deltas across series:

```esql
TS metrics | STATS SUM(RATE(request_count)) BY host
```

### Uneven publish intervals for metrics

Different hosts and agents publish at different cadences. A host emitting CPU samples
every second contributes 120x more rows than one emitting every two minutes, so `AVG()`
over the raw column weights the chatty host accordingly:

```esql
FROM metrics | STATS AVG(cpu_usage) BY cluster
```

`TS` first reduces each time series to a single value per time bucket (using
[`AVG_OVER_TIME`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/avg_over_time.md)
or another inner function), then aggregates across series. Each series contributes
equally to the outer average:

```esql
TS metrics
| STATS AVG(AVG_OVER_TIME(cpu_usage)) BY cluster, TBUCKET(1 minute)
```

## Grouping time series [grouping-time-series]

When the first `STATS` after `TS` uses a bare
[time series aggregation function](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md)
(that is, a time series function not wrapped in an outer aggregation such as `AVG()` or `SUM()`),
the rows are implicitly grouped by **all** [dimensions](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension)
of each time series. The result set includes a `_timeseries` `keyword`
column that contains a JSON-encoded object with the dimension key/value pairs
identifying each group. Only the dimensions that are actually present for a given time
series are included — not every dimension declared in the index mappings — so different
rows in the result may carry different dimension keys. For an example, refer to
[Group by all dimensions implicitly](#group-by-all-dimensions-implicitly).

You can make this grouping explicit, or narrow it to a subset of dimensions, using the
[`WITHOUT`](/reference/query-languages/esql/functions-operators/grouping-functions/without.md)
grouping function in the `BY` clause ({applies_to}`stack: ga 9.4`):

- `BY WITHOUT(dim1, dim2, ...)` groups by **all** dimensions **except** those listed.
  See [Exclude dimensions with WITHOUT](#exclude-dimensions-with-without).
- `BY WITHOUT()` (no arguments) explicitly groups by every dimension; it is equivalent
  to the implicit "group by all" behavior.

When combining a bare time series function with other groupings, only grouping functions
(such as [`TBUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions/tbucket.md)
or [`WITHOUT`](/reference/query-languages/esql/functions-operators/grouping-functions/without.md)) are allowed in the `BY` clause — bare dimension columns are not.
For example, `TS k8s | STATS rate(network.total_bytes_in) BY host` is rejected; use
`BY TBUCKET(1 hour)` or wrap the time series function with an outer aggregation instead.

::::{note}
`WITHOUT` can only be used inside the first `STATS` command under `TS` source. Using it in a `FROM | STATS ... BY WITHOUT(...)`
query leads to an error.
::::

## Best practices

- [Prefer `TS` over `FROM`](#when-to-use-ts-vs-from) for time series aggregations.
  `FROM` is still appropriate for non-aggregation uses such as listing document contents.
- Avoid aggregating multiple metrics in the same query when those metrics have different dimensional cardinalities.
  For example, in `STATS max(rate(foo)) + rate(bar))`, if `foo` and `bar` don't share the same dimension values, the rate
  for one metric will be null for some dimension combinations. Because the + operator returns null when either input
  is null, the entire result becomes null for those dimensions. Additionally, queries that aggregate a single metric
  can filter out null values more efficiently.
- Add a time range filter on `@timestamp` to limit the data volume scanned and improve query performance.
- Time series aggregations produce large result sets, especially if they involve many dimensions and small time buckets.
  The limits are updated accordingly, with the default result truncation size increased to 10,000 rows. For more
  information on the limits and how to adjust them, refer to
  [Result set size limitation](/reference/query-languages/esql/_snippets/common/result-set-size-limitation.md).

## Limitations

- **`COUNT(*)` under `TS`**: After the first `STATS` under `TS`, rows are grouped per
  time series rather than counted as raw metric documents, so `COUNT(*)` does not have
  the same meaning it does under `FROM`. To count samples for a specific metric, use
  `COUNT(<field>)` or
  [`COUNT_OVER_TIME()`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/count_over_time.md)
  against a named field.
- **Commands that reorder rows**: Commands that change row order, such as
  [`SORT`](/reference/query-languages/esql/commands/sort.md) or
  [`FORK`](/reference/query-languages/esql/commands/fork.md), cannot appear between
  `TS` and the first `STATS`. Time series aggregations rely on the natural `@timestamp`
  ordering produced by `TS`. After the first `STATS`, results are tabular and these
  commands are available again — see
  [Process tabular results after the first STATS](#process-tabular-results-after-the-first-stats).
- **Metric type compatibility**: Time series aggregation functions enforce the field's
  `metric_type` mapping. For example,
  [`RATE()`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/rate.md)
  requires a counter and rejects gauge fields. See
  [](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md#metric-type-compatibility)
  for the per-function mapping.

## Examples

The following examples demonstrate common time series query patterns using `TS`.

### Calculate the rate of search requests per host

Calculate the total rate of search requests (tracked by the `search_requests` counter) per host and hour. The `RATE()`
function is applied per time series in hourly buckets. These rates are summed for each
host and hourly bucket (since each host can map to multiple time series):

```esql
TS metrics
  | WHERE @timestamp >= now() - 1 hour
  | STATS SUM(RATE(search_requests)) BY TBUCKET(1 hour), host
```

### Aggregate with implicit LAST_OVER_TIME

The following two queries are equivalent, returning the average of the last memory usage values per time series. If a query is missing an inner (time series) aggregation function, `LAST_OVER_TIME()` is assumed and used implicitly:

```esql
TS metrics | STATS AVG(memory_usage)

TS metrics | STATS AVG(LAST_OVER_TIME(memory_usage))
```

### Calculate the average of per-time-series averages

This query calculates the average memory usage across per-time-series averages, rather than the average of all raw values:

```esql
TS metrics | STATS AVG(AVG_OVER_TIME(memory_usage))
```

### Use time series aggregation functions directly

You can use a [time series aggregation function](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md) directly in `STATS` ({applies_to}`stack: preview 9.3`):

```esql
TS metrics
| WHERE TRANGE(1 day)
| STATS RATE(search_requests) BY TBUCKET(1 hour)
```

### Combine SUM and RATE

Use `SUM` as the outer aggregation to sum counter rates across groups:

```esql
TS metrics | STATS SUM(RATE(search_requests)) BY host
```

### Combine SUM and AVG_OVER_TIME

Use `AVG_OVER_TIME` to compute per-time-series averages, then group the results by host and time bucket:

```esql
TS metrics
| WHERE @timestamp >= now() - 1 day
| STATS SUM(AVG_OVER_TIME(memory_usage)) BY host, TBUCKET(1 hour)
```

### Process tabular results after the first STATS

Once the first `STATS` under `TS` completes, the pipeline becomes a regular ES|QL table.
Chain additional `STATS`, `EVAL`, `SORT`, and similar commands to derive secondary
aggregations or computed columns. For example, take per-minute rates per cluster, then
compute the 95th-percentile and maximum rate per cluster and rank clusters by headroom:

```esql
TS k8s
| WHERE @timestamp >= now() - 1 hour
| STATS rate = MAX(RATE(network.total_bytes_in)) BY cluster, TBUCKET(1 minute)
| STATS p95 = PERCENTILE(rate, 95), peak = MAX(rate) BY cluster
| EVAL headroom = peak - p95
| SORT headroom DESC
```

### Group by all dimensions implicitly

When a bare time series aggregation function is used without a `BY` clause, results are
implicitly grouped by all dimensions of each time series and include a `_timeseries`
column with the dimension key/value pairs. Note how the `qa` cluster rows only carry
`cluster` and `pod` keys, while the `prod` and `staging` rows also include `region` —
only the dimensions that actually exist for a given time series appear in `_timeseries`:

::::{include} ../examples/k8s-timeseries.csv-spec/docsGroupByAllImplicitly.md
::::

### Exclude dimensions with WITHOUT

Use [`WITHOUT`](/reference/query-languages/esql/functions-operators/grouping-functions/without.md)
({applies_to}`stack: ga 9.4`) in the `BY` clause to exclude specific dimensions from
the time series grouping. For example, group by every dimension except `pod`:

::::{include} ../examples/k8s-timeseries-without.csv-spec/docsWithoutSingleDimension.md
::::

`WITHOUT` can be combined with
[`TBUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions/tbucket.md)
to add a time bucket to the grouping — useful for producing per-interval aggregates
across the surviving dimensions:

::::{include} ../examples/k8s-timeseries-without.csv-spec/docsWithoutWithTbucket.md
::::

Passing no arguments (`WITHOUT()`) is equivalent to grouping by all dimensions — the same
as omitting the `BY` clause entirely. Refer to the
[`WITHOUT`](/reference/query-languages/esql/functions-operators/grouping-functions/without.md)
function reference for more examples.

### Invalid query: nested time series functions

Using a time series aggregation function in combination with an inner time series function causes an error:

```esql
TS metrics | STATS AVG_OVER_TIME(RATE(memory_usage))
```
