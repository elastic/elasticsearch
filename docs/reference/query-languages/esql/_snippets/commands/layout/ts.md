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

The `TS` source command enables time series semantics and enables the usage of
time series aggregation functions in the `STATS` command, such as
[`AVG_OVER_TIME()`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions#esql-avg_over_time),
or [`RATE`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions#esql-rate).
These functions are implicitly evaluated per per time-series, with their results
then aggregated per grouping bucket using a secondary aggregation
function. More concretely, consider the following query:

```esql
TS metrics
  | WHERE @timestamp >= now() - 1 hour
  | STATS SUM(RATE(search_requests)) BY TBUCKET(1 hour), host
```

This query calculates the total rate of search requests (tracked through
counter `search`) per host and hour. Here, the `rate()` function is first
applied per time-series and hourly time bucket, with the results then summed per
host and hourly bucket, as each host value may map to many time-series.

This paradigm with a pair of aggregation functions is standard for time-series
querying. Supported inner (time-series) functions per
[metric type](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-metric)
are listed [here](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md)
and apply to downsampled data too, with the same semantics as for raw data.

::::{note}
If a query is missing an inner (time-series) aggregation function,
[`LAST_OVER_TIME()`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions#esql-last_over_time)
is assumed and used implicitly. For instance, the following two queries are
equivalent, returning the average of the last memory usage values per time-series:

```esql
TS metrics | STATS AVG(memory_usage)

TS metrics | STATS AVG(LAST_OVER_TIME(memory_usage))
```

Calculating the average memory usage across per-time-series averages requires
the following query:

```esql
TS metrics | STATS AVG(AVG_OVER_TIME(memory_usage))
```
::::

Use regular (non-time-series) [aggregation functions](/reference/query-languages/esql/functions-operators/aggregation-functions.md),
such as `SUM()` as outer aggregation functions. Using a time-series aggregation,
in combination with an inner function, leads to an error. For instance, the
following query is invalid:

```esql
TS metrics | STATS AVG_OVER_TIME(RATE(memory_usage))
```

::::{note}
It's currently required to wrap a time-series aggregation function inside a
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
- Prefer the `TS` command for aggregations on time-series data. `FROM` is still
  applicable, e.g. to list document contents, but it's not optimized to process
  time-series data efficiently. More so, the  `TS` command can't be combined
  with certain operation such as [`FORK`](/reference/query-languages/esql/commands/fork.md),
  before the `STATS` command is applied. That said, once `STATS` is applied, its
  tabular output can be further processed as applicable, in line with regular
  ES|QL processing.
- Include a time range filter on `@timestamp`, to prevent scanning
  unnecessarily large data volumes.

**Examples**

```esql
TS metrics
| WHERE @timestamp >= now() - 1 day
| STATS SUM(AVG_OVER_TIME(memory_usage)) BY host, TBUCKET(1 hour)
```

