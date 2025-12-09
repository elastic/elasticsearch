---
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
[`TBUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions.md#esql-tbucket)).
For example, the following query calculates the average rate of requests per
host for every minute, using values over a sliding window of 10 minutes:

```esql
TS metrics
  | WHERE TRANGE(1h)
  | STATS AVG(RATE(requests, 10m)) BY TBUCKET(1m), host
```

Accepted window values are currently limited to multiples of the time bucket
interval in the BY clause. If no window is specified, the time bucket interval
is implicitly used as a window.

The following time series aggregation functions are supported:

:::{include} ../_snippets/lists/time-series-aggregation-functions.md
:::

:::{include} ../_snippets/functions/layout/absent_over_time.md
:::

:::{include} ../_snippets/functions/layout/avg_over_time.md
:::

:::{include} ../_snippets/functions/layout/count_over_time.md
:::

:::{include} ../_snippets/functions/layout/count_distinct_over_time.md
:::

:::{include} ../_snippets/functions/layout/delta.md
:::

:::{include} ../_snippets/functions/layout/first_over_time.md
:::

:::{include} ../_snippets/functions/layout/idelta.md
:::

:::{include} ../_snippets/functions/layout/increase.md
:::

:::{include} ../_snippets/functions/layout/irate.md
:::

:::{include} ../_snippets/functions/layout/last_over_time.md
:::

:::{include} ../_snippets/functions/layout/max_over_time.md
:::

:::{include} ../_snippets/functions/layout/min_over_time.md
:::

:::{include} ../_snippets/functions/layout/present_over_time.md
:::

:::{include} ../_snippets/functions/layout/rate.md
:::

:::{include} ../_snippets/functions/layout/stddev_over_time.md
:::

:::{include} ../_snippets/functions/layout/variance_over_time.md
:::

:::{include} ../_snippets/functions/layout/sum_over_time.md
:::

:::{include} ../_snippets/functions/layout/deriv.md
:::
