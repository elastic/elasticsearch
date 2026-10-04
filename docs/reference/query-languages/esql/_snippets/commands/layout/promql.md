```yaml {applies_to}
serverless: ga
stack: preview 9.4, ga 9.5
```

The `PROMQL` source command queries [time series indices](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) using [**Prometheus Query Language (PromQL)**](https://prometheus.io/docs/prometheus/latest/querying/basics/).
Like [`TS`](/reference/query-languages/esql/commands/ts.md), it enables time series aggregation functions, but accepts PromQL syntax instead of ES|QL.

::::{note}
`PROMQL` supports most, but not all, of PromQL. Refer to [Limitations](#esql-promql-limitations) for unsupported constructs, to [PromQL limitations](/reference/query-languages/promql/promql-limitations.md) for behavioral differences from Prometheus, and to [PromQL functions](/reference/query-languages/promql/functions.md) for the supported functions and their restrictions.
::::


## Syntax

The `PROMQL` command accepts zero or more space-separated `<option>=<value>` pairs, followed by a named PromQL expression.

```esql
PROMQL [ <option>=<value> ... ] <result_name>=(<PromQL Expression>)
```

## Options

The options are inspired by the Prometheus [HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries) with some additions specific to ES|QL.

`index`
:   A list of indices, data streams, or aliases. Supports wildcards and date math.
    Defaults to `metrics-*` querying matching indices with [`index.mode: time_series`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md).
    Example: `PROMQL index=metrics-*.otel-* http_rate=(sum(rate(http_requests_total)))`

`step`
:   Query resolution step width (optional).
    Automatically determined given the number of target `buckets` and the selected time range.
    Example: `PROMQL step=1m http_rate=(sum(rate(http_requests_total)))`

`buckets`
:   Target number of buckets for auto-step derivation.
    Defaults to `100`. Mutually exclusive with `step`. Requires a known time range, either by setting
    `start` and `end` explicitly or implicitly through Kibana's time range filter.
    Example: `PROMQL buckets=50 start="2026-04-01T00:00:00Z" end="2026-04-01T01:00:00Z" http_rate=(sum(rate(http_requests_total)))`

`start`
:   Start time of the query, inclusive (optional).
    Uses the start based on Kibana's date picker if missing. Set together with `end`.
    Refer to [Time range](#esql-promql-time-range) for queries without a time range.
    Example: `PROMQL start="2026-04-01T00:00:00Z" end="2026-04-01T01:00:00Z" http_rate=(sum(rate(http_requests_total)))`

`end`
:   End time of the query, inclusive (optional).
    Uses the end based on Kibana's date picker if missing. Set together with `start`, and not before it.
    Refer to [Time range](#esql-promql-time-range) for queries without a time range.
    Example: `PROMQL start="2026-04-01T00:00:00Z" end="2026-04-01T02:00:00Z" http_rate=(sum(rate(http_requests_total)))`

`time`
:   {applies_to}`stack: ga 9.5` {applies_to}`serverless: ga` Evaluation time of an instant query (optional).
    Evaluates the expression once, at this time, instead of at each step of a range query.
    Mutually exclusive with `start`, `end`, `step`, and `buckets`.
    Example: `PROMQL time="2026-04-01T01:00:00Z" http_rate=(sum(rate(http_requests_total)))`

`scrape_interval`
:   The expected metric collection interval.
    Defaults to `1m`. Used to determine implicit range selector windows as `max(step, scrape_interval)`.
    Example: `PROMQL scrape_interval=15s http_rate=(sum(rate(http_requests_total)))`

`<result_name>=(<PromQL Expression>)`
:   Name of the output column with the query result timeseries (optional).
    By default, the name of the output column is the PromQL expression itself.
    Example: `PROMQL http_rate=(sum by (instance) (rate(http_requests_total))) | SORT http_rate DESC`

`start`, `end`, and `time` accept an RFC 3339 timestamp with a UTC offset, such as `"2026-04-01T00:00:00Z"`,
or a Unix timestamp in seconds, which can be fractional, such as `1775001600`. Date math, such as `now-1h`,
isn't supported. In Kibana, the `?_tstart` and `?_tend` parameters reference the time range of the date picker,
such as `time=?_tend`.
`step` and `scrape_interval` accept a PromQL duration, such as `30s` or `5m`, or a number of seconds.


## Description

The `PROMQL` command takes standard PromQL parameters and a PromQL expression, runs the query, and returns the
results as regular ES|QL columns. You can continue to process the columns with other ES|QL commands.

### Time range [esql-promql-time-range]

A range query needs either `step`, or both `start` and `end`, from which the step is derived using `buckets`.
In Kibana, the date picker provides `start` and `end`. Without a time range and without `step`, the query fails.
With `step` but no time range, the query covers all data in the index.

### Output columns

The result contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| The PromQL expression (or `<result_name>` if specified) | `double` | The computed metric value |
| `step` | `date` | The timestamp for each evaluation step. For an instant query, the evaluation time |
| Grouping labels (if any) | `keyword` | One column per grouping label from `by` clauses |
| `_timeseries` (if any) | `keyword` | The labels of each series as a JSON string |

The label columns depend on the outermost aggregation of the PromQL expression:

- With a `by` grouping, such as `sum by (instance) (...)`, each grouping label gets its own output column.
- With an aggregation without grouping, such as `sum(...)`, there are no label columns, and the result is a single series.
- Without a cross-series aggregation, such as `rate(http_requests_total)`, or with a `without` grouping, such as
  `sum without (pod) (...)`, the remaining labels of each series are returned in a single `_timeseries` column as a JSON string.

A range query returns one row per series and evaluation step. An instant query returns one row per series.

### Index patterns

The `index` parameter accepts the same patterns as `FROM` and `TS`, including wildcards and comma-separated lists.
If omitted, it defaults to `metrics-*`, which queries matching indices configured with
[`index.mode: time_series`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md).
The Prometheus-compatible `query` and `query_range` endpoints use the same default when the `{index}` path parameter is omitted.

### Implicit range selectors [esql-promql-implicit-range-selectors]

In standard PromQL, functions like `rate` require a range selector: `rate(http_requests_total[5m])`.
The `PROMQL` command allows omitting the range selector entirely. When the range selector is absent, the window is
determined automatically as `max(step, scrape_interval)`.
For example: `PROMQL scrape_interval=15s http_rate=(sum(rate(http_requests_total)))`.

An implicit window adapts to the time range and step. A fixed window doesn't: when it's shorter than the step,
such as `[5m]` with a step of `1h`, each step only reflects the last 5 minutes and ignores the samples in between.
A fixed window is only useful when you need exactly that window, such as the rate over the last 5 minutes.

## Best practices [esql-promql-best-practices]

% This section serves both human readers and AI agents that write PROMQL queries.
% Only add a practice that a reader who already understands the command would still benefit from,
% and state the user-facing reason (performance, stable results, correct semantics).
% Don't add warnings against mistakes only a confused writer would make, or restate facts
% documented elsewhere on this page. Put those facts in the relevant reference section instead.

- Set `index` explicitly instead of relying on the `metrics-*` default, to narrow the data scanned.
- Omit range selectors, also when porting a Prometheus query: write `rate(http_requests_total)` instead of
  `rate(http_requests_total[5m])`, so the window adapts to the time range and step.
  Refer to [Implicit range selectors](#esql-promql-implicit-range-selectors).
- Name the result, such as `http_rate=(...)`. Otherwise, the value column is named after the expression text,
  which changes whenever the expression is reformatted, so later commands can't reliably reference it.
- Match the function to the metric type: use `rate`, `irate`, or `increase` for counters, and functions such as
  `avg_over_time` or `max_over_time`, or the raw metric, for gauges.
  For native histograms, use `increase` instead of `rate`, and wrap the result in a histogram function, such as
  `histogram_quantile(0.99, sum by (job) (increase(http_request_duration_seconds)))`.
- In Kibana, omit `start` and `end` so the query follows the date picker. Elsewhere, set `start` and `end`
  explicitly, rather than only `step`, which covers all data in the index.
- Filter by labels in the PromQL selector, such as `network.cost{cluster!="prod"}`, rather than with `WHERE`
  after `PROMQL`. Selector filters reduce the data read, and a later `WHERE` doesn't.

### Single-value results [esql-promql-single-value]

A range query returns a value for every step. To get a single value per series, such as for a metric or gauge chart
or a ranking:

- For the current value, use an [instant query](#esql-promql-instant-query). In Kibana, set `time=?_tend` to evaluate
  the expression at the end of the time range of the date picker
  (refer to [time range parameters](docs-content://explore-analyze/query-filter/languages/esql-kibana.md)):

  ```esql
  PROMQL index=metrics-generic.prometheus-* time=?_tend http_rate=(sum(rate(http_requests_total)))
  ```

- For a value over the whole time range, such as a total, collapse the steps of a range query with `STATS`.
  With an implicit range selector, the window of each step is one step wide, so summing the increase of each step
  gives the increase over the whole time range:

  ```esql
  PROMQL index=metrics-generic.prometheus-* requests=(sum(increase(http_requests_total)))
  | STATS total_requests = SUM(requests)
  ```

## Limitations [esql-promql-limitations]

:::{include} ../../../../promql/_snippets/promql-unsupported-constructs.md
:::

For behavioral differences from Prometheus, refer to [PromQL limitations](/reference/query-languages/promql/promql-limitations.md).

## Examples

### Fully adaptive query

Rely on Kibana's date picker for the time range, and let `step` and range selectors be inferred automatically:

```esql
PROMQL index=metrics-generic.prometheus-* http_rate=(sum(rate(http_requests_total)))
```

This is the recommended pattern for Kibana dashboards. The query responds to the date picker, adjusts the step size
to the selected time range, and sizes the range selector window accordingly.

### Instant query [esql-promql-instant-query]

{applies_to}`stack: ga 9.5` {applies_to}`serverless: ga`

Evaluate the expression once, for example to get the current value of a metric:

```esql
PROMQL index=metrics-generic.prometheus-*
  time="2026-04-01T01:00:00Z"
  http_rate=(sum(rate(http_requests_total)))
```

### Cross-series aggregation by label

::::{include} ../examples/k8s-timeseries-promql.csv-spec/cross_series_grouping_on_mapped_label.md
::::

### Label filtering with named result

::::{include} ../examples/k8s-timeseries-promql.csv-spec/not_equals_filter.md
::::

### Post-processing with ES|QL

Pipe PromQL results into ES|QL commands for further aggregation:

::::{include} ../examples/k8s-timeseries-promql.csv-spec/post_processing_stats_by_cluster.md
::::

### Ad-hoc query with inferred step

For queries outside Kibana, set `start` and `end` explicitly. The step and range selector are still inferred
automatically from the time range and the default `buckets` count:

```esql
PROMQL index=metrics-generic.prometheus-*
  start="2026-04-01T00:00:00Z"
  end="2026-04-01T01:00:00Z"
  http_rate=(sum(rate(http_requests_total)))
```

### Enrich with a lookup

Join PromQL results with external data using ES|QL commands:

```esql
PROMQL index=metrics-generic.prometheus-*
  http_rate=(sum by (instance) (rate(http_requests_total)))
| LOOKUP JOIN instance_metadata ON instance
```
