```yaml {applies_to}
serverless: preview
stack: preview 9.4
```

The `PROMQL` source command is similar to the [`TS`](/reference/query-languages/esql/commands/ts.md)
source command allowing you to query time series data using [**Prometheus Query Language**](https://prometheus.io/docs/prometheus/latest/querying/basics/).

::::{note}
In 9.4, `PROMQL` command is available as a preview feature. Current limitations include:

- Group modifiers such as `on(chip) group_left(chip_name)` are not supported.
- Set operators such as `or`, `and`, and `unless` are not supported.
- Some functions including `histogram_quantile`, `predict_linear`, and `label_join` are not supported.
- Time buckets align to fixed calendar boundaries rather than the query start time. This can cause slight differences from Prometheus, especially for short ranges or large step sizes.
::::


## Syntax

The `PROMQL` command accepts zero or more space-separated key value options followed by named PromQL expression.

```esql
PROMQL [ <option> ... ] <name> = ( <expression> )
```

## Options

The options are inspired by the Prometheus [HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries) with some additions specific to ES|QL.

`index`
:   A list of indices, data streams, or aliases. Supports wildcards and date math.
    Defaults to `*` querying all indices with [`index.mode: time_series`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md).
    Example: `PROMQL index=metrics-*.otel-* sum(rate(http_requests_total))`

`step`
:   Query resolution step width.
    Automatically determined given the number of target `buckets` and the selected time range.
    Example: `PROMQL step=1m sum(rate(http_requests_total[5m]))`

`buckets`
:   Target number of buckets for auto-step derivation.
    Defaults to `100`. Mutually exclusive with `step`. Requires a known time range, either by setting
    `start` and `end` explicitly or implicitly through Kibana's time range filter.
    Example: `PROMQL buckets=50 start="2026-04-01T00:00:00Z" end="2026-04-01T01:00:00Z" sum(rate(http_requests_total))`

`start`
:   Start time of the query, inclusive.
    Uses the start based on Kibana's date picker or unrestricted if missing.
    Example: `PROMQL start="2026-04-01T00:00:00Z" end="2026-04-01T01:00:00Z" sum(rate(http_requests_total))`

`end`
:   End time of the query, inclusive.
    Uses the end based on Kibana's date picker or unrestricted if missing.
    Example: `PROMQL start="2026-04-01T00:00:00Z" end="2026-04-01T02:00:00Z" sum(rate(http_requests_total))`

`scrape_interval`
:   The expected metric collection interval.
    Defaults to `1m`. Used to determine implicit range selector windows as `max(step, scrape_interval)`.
    Example: `PROMQL scrape_interval=15s sum(rate(http_requests_total))`

`result_name`
:   Name of the output column with the query result timeseries.
    By default, the name of the output column is the text of the PromQL expression itself.
    Example: `PROMQL http_rate=(sum by (instance) (rate(http_requests_total))) | SORT http_rate DESC`


## Description

The `PROMQL` command takes standard PromQL parameters and a PromQL expression, runs the query, and returns the
results as regular ES|QL columns . You can continue to process the columns with other ES|QL commands.

### Output columns

The result contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| The PromQL expression (or `result_name` if specified) | `double` | The computed metric value |
| `step` | `date` | The timestamp for each evaluation step |
| Grouping labels (if any) | `keyword` | One column per grouping label from `by` clauses |

When the PromQL expression includes a cross-series aggregation like `sum by (instance)`, each grouping label gets
its own output column. When there is no cross-series aggregation, all labels are returned in a single `_timeseries`
column as a JSON string.

### Index patterns

The `index` parameter accepts the same patterns as `FROM` and `TS`, including wildcards and comma-separated lists.
If omitted, it defaults to `*`, which queries all indices configured with
[`index.mode: time_series`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md).
In production, specifying an explicit index pattern avoids scanning unrelated data.

### Implicit range selectors

In standard PromQL, functions like `rate` require a range selector: `rate(http_requests_total[5m])`.
The `PROMQL` command allows omitting the range selector entirely. When the range selector is absent, the window is
determined automatically as `max(step, scrape_interval)`.
For example: `PROMQL scrape_interval=15s sum(rate(http_requests_total))`.

## Examples

### Fully adaptive query

Rely on Kibana's date picker for the time range, and let `step` and range selectors be inferred automatically:

```esql
PROMQL index=metrics-* sum by (instance) (rate(http_requests_total))
```

This is the recommended pattern for Kibana dashboards. The query responds to the date picker, adjusts the step size
to the selected time range, and sizes the range selector window accordingly.

### Range query with explicit parameters

::::{include} ../examples/k8s-timeseries-promql.csv-spec/promql_start_end_step.md
::::

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
PROMQL index=metrics-*
  start="2026-04-01T00:00:00Z"
  end="2026-04-01T01:00:00Z"
  sum by (instance) (rate(http_requests_total))
```

### Enrich with a lookup

Join PromQL results with external data using ES|QL commands:

```esql
PROMQL index=metrics-*
  http_rate=(sum by (instance) (rate(http_requests_total)))
| LOOKUP JOIN instance_metadata ON instance
```
