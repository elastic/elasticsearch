```yaml {applies_to}
serverless: preview
stack: preview 9.2.0
```

**Brief description**

The `TS` source command is similar to the [`FROM`](/reference/query-languages/esql/commands/from.md)
source command, with the following key differences:

 - Targets only [time-series indices](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md)
 - Enables the use of time-series aggregation functions inside the
   [STATS](/reference/query-languages/esql/commands/stats-by.md) command

**Syntax**

```esql
TS index_pattern [METADATA fields]
```

**Parameters**

`index_pattern`
:   A list of indices, data streams or aliases. Supports wildcards and date math.

`fields`
:   A comma-separated list of [metadata fields](/reference/query-languages/esql/esql-metadata-fields.md) to retrieve.

**Description**

The `TS` source command enables time series semantics and adds support for
time series aggregation functions to the `STATS` command, such as
[`AVG_OVER_TIME()`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md#esql-avg_over_time),
or [`RATE`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md#esql-rate).
These functions are implicitly evaluated per time series, then aggregated by group using a secondary aggregation
function. For example:

```esql
TS metrics
  | WHERE @timestamp >= now() - 1 hour
  | STATS SUM(RATE(search_requests)) BY TBUCKET(1 hour), host
```

This query calculates the total rate of search requests (tracked by the `search_requests` counter) per host and hour. The `RATE()` function is 
applied per time series in hourly buckets. These rates are summed for each
host and hourly bucket (since each host can map to multiple time series).

This paradigm—a pair of aggregation functions—is standard for time series
querying. For supported inner (time series) functions per
[metric type](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-metric), refer to [](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md). These functions also
apply to downsampled data, with the same semantics as for raw data.

::::{note}
If a query is missing an inner (time series) aggregation function,
[`LAST_OVER_TIME()`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md#esql-last_over_time)
is assumed and used implicitly. For instance, the following two queries are
equivalent, returning the average of the last memory usage values per time series:

```esql
TS metrics | STATS AVG(memory_usage)

TS metrics | STATS AVG(LAST_OVER_TIME(memory_usage))
```

To calculate the average memory usage across per-time-series averages, use
the following query:

```esql
TS metrics | STATS AVG(AVG_OVER_TIME(memory_usage))
```
::::

Use regular (non-time-series) [aggregation functions](/reference/query-languages/esql/functions-operators/aggregation-functions.md),
such as `SUM()`, as outer aggregation functions. Using a time series aggregation
in combination with an inner function causes an error. For example, the
following query is invalid:

```esql
TS metrics | STATS AVG_OVER_TIME(RATE(memory_usage))
```

::::{note}
A time series aggregation function must be wrapped inside a
regular aggregation function. For instance, the following query is invalid:

```esql
TS metrics | STATS RATE(search_requests)
```
::::

**Best practices**

- Avoid mixing aggregation functions on different metrics in the same query, to
  avoid interference between different time-series. For instance, if one metric
  is missing values for a given time-series, the aggregation function
  may return null for a given combination of dimensions, which may lead to a
  null result for that group if the secondary function returns null on a null
  arg. More so, null metric filtering is more efficient when a query includes
  a single metric.
- Use the `TS` command for aggregations on time series data, rather than `FROM`. The `FROM` command is still available (for example, for listing document contents), but it's not optimized for time series data.
- The `TS` command can't be combined with certain operations (such as [`FORK`](/reference/query-languages/esql/commands/fork.md)) before the `STATS` command is applied. Once `STATS` is applied, you can process the tabular output with any applicable ES|QL operations.
- Add a time range filter on `@timestamp` to limit the data volume scanned and improve query performance.

**Examples**

```esql
TS metrics
| WHERE @timestamp >= now() - 1 day
| STATS SUM(AVG_OVER_TIME(memory_usage)) BY host, TBUCKET(1 hour)
```

