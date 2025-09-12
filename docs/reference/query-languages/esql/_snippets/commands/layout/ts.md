```yaml {applies_to}
serverless: preview
stack: preview 9.2.0
```

**Brief description**

The `TS` command is similar to the `FROM` source command,
with the following key differences:

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

The `TS` command is intended to be used in conjunction with the `STATS` command
to perform time-series analysis. `STATS` evaluation is adjusted to fit this context,
enabling the use of time-series aggregation functions such as `last_over_time()`,
or `rate`. These functions are implicitly evaluated per per time-series, with
their results then aggregated per grouping bucket using a secondary aggregation
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
include:

- `[LAST_OVER_TIME()](/reference/query-languages/esql/functions-operators/aggregation-functions#esql-last-over-time)`: gauges and counters
- `[FIRST_OVER_TIME()](/reference/query-languages/esql/functions-operators/aggregation-functions#esql-first-over-time)`: gauges and counters
- `[RATE()](/reference/query-languages/esql/functions-operators/aggregation-functions#esql-rate)`: counters only
- `[MIN_OVER_TIME()](/reference/query-languages/esql/functions-operators/aggregation-functions#esql-min-over-time)`: gauges only
- `[MAX_OVER_TIME()](/reference/query-languages/esql/functions-operators/aggregation-functions#esql-max-over-time)`: gauges only
- `[SUM_OVER_TIME()](/reference/query-languages/esql/functions-operators/aggregation-functions#esql-sum-over-time)`: gauges only
- `[COUNT_OVER_TIME()](/reference/query-languages/esql/functions-operators/aggregation-functions#esql-count-over-time)`: gauges only
- `[AVG_OVER_TIME()](/reference/query-languages/esql/functions-operators/aggregation-functions#esql-avg-over-time)`: gauges only
- `[PRESENT_OVER_TIME()](/reference/query-languages/esql/functions-operators/aggregation-functions#esql-present-over-time)`: gauges only
- `[ABSENT_OVER_TIME()](/reference/query-languages/esql/functions-operators/aggregation-functions#esql-absent-over-time)`: gauges only

These functions are supported for downsampled data too, with the same semantics
as for raw data. For instance, `RATE()` applies to downsampled counters only,
not gauges. No change in the syntax is required to query a time-series data
stream containing mixed raw and downsampled data.

::::{note}
If a query is missing an inner (time-series) aggregation function,
`LAST_OVER_TIME()` is assumed and used implicitly. For instance, the following
two queries are equivalent, returning the average of the last memory usage
values per time-series:

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

Standard (non-time-series) aggregation functions, such as `SUM()`, `AVG()`,
can be used as outer aggregation functions. Using a time-series aggregation as
the outer function leads to an error.

::::{note}
If the outer aggregation function is missing, results are grouped by time-series
and implicitly expanded to include all dimensions. For instance, the output for
the following query will include all dimension fields in `metrics` - as opposed
to just `hourly` for an equivalent query using `FROM` source command:

```esql
TS metrics | STATS RATE(search_requests) BY hourly=TBUCKET(1 hour)
```

Including fields except from time as grouping attributes is not allowed as it'd
require an aggregation function to combine per-time-series values.
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
  with certain operation such as `FORK`, before the `STATS` command is applied.
  That said, once `STATS` is applied, its tabular output can be further
  processed as applicable, in line with regular ES|QL processing.
- Include a time range filter on `@timestamp`, to prevent scanning
  unnecessarily large data volumes.

**Examples**

```esql
TS metrics
| WHERE @timestamp >= now() - 1 day
| STATS SUM(AVG_OVER_TIME(memory_usage)) BY host, TBUCKET(1 hour)
```

