---
applies_to:
  stack: preview 9.2-9.3, ga 9.4+
  serverless: ga
navigation_title: "Time series aggregation functions"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-functions-operators.html#esql-time-series-agg-functions
---

# {{esql}} time series aggregation functions [esql-time-series-aggregation-functions]

The first [`STATS`](/reference/query-languages/esql/commands/stats-by.md) under a [`TS`](/reference/query-languages/esql/commands/ts.md) source command supports
aggregation functions per time series. These functions accept up to two arguments.
The first argument is required and denotes the metric name of the time series.
The second argument is optional and allows specifying a sliding time window for
aggregating metric values. Note that this is orthogonal to time bucketing of
output results, as specified in the BY clause (e.g. through
[`TBUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions/tbucket.md)).
For example, the following query calculates the average rate of requests per
host for every minute, using values over a sliding window of 10 minutes:

```esql
TS metrics
  | WHERE TRANGE(1h)
  | STATS AVG(RATE(requests, 10m)) BY TBUCKET(1m), host
```

::::{applies-switch}
:::{applies-item} stack: preview 9.2-9.3
Accepted window values are currently limited to multiples of the time bucket
interval in the BY clause. If no window is specified, the time bucket interval
is implicitly used as a window.
:::
:::{applies-item} stack: ga 9.4+
All window values are accepted, though there are performance optimizations for
the cases where the window is a multiple of the time bucket interval.

It's currently not allowed to mix windows that are smaller than the time bucket
for one metrics and larger than the time bucket for another metrics, in the same
query.
:::
::::

The following time series aggregation functions are supported:

:::{include} ../_snippets/lists/time-series-aggregation-functions.md
:::
