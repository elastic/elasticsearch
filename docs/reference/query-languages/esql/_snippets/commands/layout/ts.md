```yaml {applies_to}
serverless: tech-preview
stack: tech-preview 9.2.0
```

The `TS` command is similar to the `FROM` source command,
with the following key differences:

 - Targets only [time-series indices](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md)
 - Propagates the metadata fields [`_tsid`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#tsid)
   and `@timestamp` to the next operators,
   even if they are not mentioned explicitly
 - Enables the use of time-series aggregation functions inside the
   [STATS](/reference/query-languages/esql/commands/stats-by.md) command

The `TS` command is expected to be used in conjunction with the `STATS` command
to perform time-series analysis. `STATS` behaves polymorphically in this context,
enabling the use of time-series aggregation functions such as `last_over_time()`,
or `rate`. These functions are implicitly evaluated per `_tsid`
(per time-series), with their results then aggregated per grouping bucket using
a secondary aggregation function. More concretely, consider the following query:

```esql
TS metrics
  | WHERE @timestamp >= now() - 1 hour
  | STATS total_rate=SUM(RATE(search_requests)) BY hourly=TBUCKET(1 hour), host
```

This query calculates the total rate of search requests (tracked through
counter `search`) per host-hour. Here, the `rate()` function is first
applied per `_tsid` and hourly time bucket, with the results then summed per
host and hourly bucket, as each host value may map to many `_tsid` values
(`_tsid` is calculated on all dimension values).

::::{note}
Metric fields are part of the time-series definition, so there is no point to
include null metric values in these queries. Therefore, null metric values get
filtered out implicitly under the `TS` command.
::::

This paradigm with a pair of aggregation functions is standard for time-series
querying. Supported inner (time-series) functions per
[metric type](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-metric)
include:

 - `LAST_OVER_TIME()`: applies to gauges and counters
 - `FIRST_OVER_TIME()`: applies to gauges and counters
 - `RATE()`: applies to counters only
 - `MIN_OVER_TIME()`: applies to gauges only
 - `MAX_OVER_TIME()`: applies to gauges only
 - `SUM_OVER_TIME()`: applies to gauges only
 - `COUNT_OVER_TIME()`: applies to gauges only
 - `AVG_OVER_TIME()`: applies to gauges only
 - `PRESENT_OVER_TIME()`: applies to gauges only'
 - `ABSENT_OVER_TIME()`: applies to gauges only'

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
If the outer aggregation function is missing, results are grouped by `_tsid` and
implicitly expanded to include all dimensions that are used to calculate `_tsid`.
For instance, the output for the following query will include all dimension
fields in `metrics` - as opposed to just `hourly` for an equivalent query using
`FROM` source command:

```esql
TS metrics | STATS RATE(search_requests) BY hourly=TBUCKET(1 hour)
```

Including fields except from time as grouping attributes is not allowed as it'd
require an aggregation function to combine per-tsid values.
::::

**Best practices**

 - Avoid mixing aggregation functions on different metrics in the same query, to
   avoid interference between different time-series. For instance, if one metric
   is missing values for a given `_tsid`, the time-series aggregation function
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

**Syntax**

```esql
TS index_pattern [METADATA fields]
```

**Parameters**

`index_pattern`
:   A list of indices, data streams or aliases. Supports wildcards and date math.

`fields`
:   A comma-separated list of [metadata fields](/reference/query-languages/esql/esql-metadata-fields.md) to retrieve.

**Examples**

```esql
TS metrics
| WHERE @timestamp >= now() - 1 day
| STATS SUM(AVG_OVER_TIME(memory_usage)) BY host, TBUCKET(1 hour)
```

