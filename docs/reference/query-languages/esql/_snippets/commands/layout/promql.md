```yaml {applies_to}
serverless: preview
stack: preview 9.4
```

The `PROMQL` source command is similar to the [`TS`](/reference/query-languages/esql/commands/ts.md)
source command allowing you to query time series data using [**Prometheus Query Language**](https://prometheus.io/docs/prometheus/latest/querying/basics/).

::::{note}
In 9.4, the `PROMQL` command is available as a preview feature. The most notable gaps:

- **Group modifiers** like `on(chip) group_left(chip_name)` are not yet supported.
- **Binary set operators** (`or`, `and`, `unless`) are not yet available.
- **Some functions** are still missing, including `histogram_quantile`, `predict_linear`, and `label_join`.
::::


## Syntax

```esql
PROMQL [index=<pattern>] [step=<duration>] [start=<timestamp>] [end=<timestamp>]
  [<result_name>=](<PromQL Expression>)
```

## Parameters

The parameters mirror the [Prometheus API](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries).

`index_pattern`
:   A list of indices, data streams or aliases. Supports wildcards and date math.

`step`
:   Query resolution step width.

`start`
:   Start time of the query, inclusive.

`end`
:   End time of the query, inclusive.

`result_name`
:   Name of the output column with the query result timeseries.


## Description

The `PROMQL` command takes standard PromQL parameters and a PromQL expression, executes the query, and returns the
results as regular ES|QL columns that you can continue to process with other ES|QL commands.

### Output columns

The result contains the following columns:

| Column | Type | Description |
|--------|------|-------------|
| The PromQL expression (or `result_name` if specified) | `double` | The computed metric value |
| `step` | `date` | The timestamp for each evaluation step |
| Grouping labels (if any) | `keyword` | One column per grouping label from `by` clauses |

When the PromQL expression includes a cross-series aggregation like `sum by (instance)`, it gets
its own output column. When there is no cross-series aggregation, all labels are returned in a single `_timeseries`
column as a JSON string.

### Index patterns

The `index` parameter accepts the same patterns as `FROM` and `TS`, including wildcards and comma-separated lists.
If omitted, it defaults to `*`, which queries all indices configured with
[`index.mode: time_series`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md).
In production, specifying an explicit index pattern avoids scanning unrelated data.

### Smart defaults

The `PROMQL` command has several features that simplify queries by inferring parameters automatically.

**Auto-step**: If you omit the `step` parameter, the command derives it automatically based on the time range and a
target bucket count. You can set the target explicitly with `buckets=<n>`. This uses the same
date-rounding logic as the ES|QL
[`BUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions.md#esql-bucket) function.

**Inferred start and end**: Kibana adds a time range filter to every ES|QL request via a Query DSL `range` filter on
`@timestamp`. The `PROMQL` command extracts those bounds and uses them as `start` and `end` when they are not specified
in the query. The command picks up the date picker range from the request context without any additional configuration.

**Implicit range selectors**: In standard PromQL, functions like `rate` require a range selector:
`rate(http_requests_total[5m])`. The `PROMQL` command allows omitting the range selector entirely. When the range
selector is absent, the window is determined automatically as `max(step, scrape_interval)`. The `scrape_interval`
defaults to `1m` and can be overridden with the `scrape_interval` parameter if your data has a different collection
interval, for example: `PROMQL scrape_interval=15s sum(rate(http_requests_total))`.

Combining all three defaults, a fully adaptive query in Kibana looks like this:

```esql
PROMQL sum(rate(http_requests_total))
```

This query responds to the date picker, adjusts the step size to the selected time range, and sizes the range selector
window accordingly.

## Examples

### Basic range query

Calculate the per-second rate of HTTP requests over a sliding 5-minute window, grouped by instance:

```esql
PROMQL index=metrics-*
  step=1m
  start="2026-04-01T00:00:00Z"
  end="2026-04-01T01:00:00Z"
  sum by (instance) (rate(http_requests_total[5m]))
```

### Name the value column

Assign a custom name to the value column to make it easier to reference in downstream commands:

```esql
PROMQL index=metrics-*
  step=1m
  start="2026-04-01T00:00:00Z"
  end="2026-04-01T01:00:00Z"
  http_rate=(sum by (instance) (rate(http_requests_total[5m])))
| SORT http_rate DESC
```

### Fully adaptive query

Rely on Kibana's date picker for the time range, and let `step` and range selectors be inferred automatically:

```esql
PROMQL index=metrics-* sum(rate(http_requests_total))
```

### Filter results

```esql
PROMQL index=metrics-*
  http_rate=(sum by (instance) (rate(http_requests_total[5m])))
| WHERE http_rate > 100
```

### Sort and limit

```esql
PROMQL index=metrics-*
  http_rate=(sum by (instance) (rate(http_requests_total[5m])))
| SORT http_rate DESC
| LIMIT 10
```

### Enrich with a lookup

Join PromQL results with external data using ES|QL commands:

```esql
PROMQL index=metrics-*
  http_rate=(sum by (instance) (rate(http_requests_total[5m])))
| LOOKUP JOIN instance_metadata ON instance
```
