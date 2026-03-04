```yaml {applies_to}
serverless: preview
stack: preview 9.2.0
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

::::{note}
If a query is missing an inner (time series) aggregation function,
[`LAST_OVER_TIME()`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/last_over_time.md)
is assumed and used implicitly. For example, two equivalent queries that return the average of the last memory usage values per time series are shown in [Aggregate with implicit LAST_OVER_TIME](#aggregate-with-implicit-last_over_time). To calculate the average memory usage across per-time-series averages, refer to [Calculate the average of per-time-series averages](#calculate-the-average-of-per-time-series-averages).
::::

You can use [time series aggregation functions](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md)
directly in the `STATS` command ({applies_to}`stack: preview 9.3`). The output will contain one aggregate value per time series and time bucket (if specified). For an example, refer to [Use time series aggregation functions directly](#use-time-series-aggregation-functions-directly).

You can also combine time series aggregation functions with regular [aggregation functions](/reference/query-languages/esql/functions-operators/aggregation-functions.md) such as `SUM()`, as outer aggregation functions. For examples, refer to [Combine SUM and RATE](#combine-sum-and-rate) and [Combine SUM and AVG_OVER_TIME](#combine-sum-and-avg_over_time).

However, using a time series aggregation function in combination with an inner time series function causes an error. For an example, refer to [Invalid query: nested time series functions](#invalid-query-nested-time-series-functions).

## Best practices

- Avoid aggregating multiple metrics in the same query when those metrics have different dimensional cardinalities.
  For example, in `STATS max(rate(foo)) + rate(bar))`, if `foo` and `bar` don't share the same dimension values, the rate
  for one metric will be null for some dimension combinations. Because the + operator returns null when either input
  is null, the entire result becomes null for those dimensions. Additionally, queries that aggregate a single metric
  can filter out null values more efficiently.
- Use the `TS` command for aggregations on time series data, rather than `FROM`. The `FROM` command is still available
  (for example, for listing document contents), but it's not optimized for processing time series data and may produce
  unexpected results.
- The `TS` command can't be combined with certain operations (such as
  [`FORK`](/reference/query-languages/esql/commands/fork.md)) before the `STATS` command is applied. Once `STATS` is
  applied, you can process the tabular output with any applicable ES|QL operations.
- Add a time range filter on `@timestamp` to limit the data volume scanned and improve query performance.

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

### Invalid query: nested time series functions

Using a time series aggregation function in combination with an inner time series function causes an error:

```esql
TS metrics | STATS AVG_OVER_TIME(RATE(memory_usage))
```
