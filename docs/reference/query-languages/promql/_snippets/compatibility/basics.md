% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Headings follow https://prometheus.io/docs/prometheus/latest/querying/basics/. Rows are numbered across all
% snippets in _snippets/compatibility/. A row without a version badge applies to every version.

Sections follow [Querying basics](https://prometheus.io/docs/prometheus/latest/querying/basics/) in the Prometheus documentation.

### Expression language data types [prometheus-compatibility-expression-language-data-types]

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Native histograms are stored as `exponential_histogram` fields.
`histogram_quantile`, `increase`, and `sum` support them, and so do `histogram_count`, `histogram_sum`, `histogram_avg`, `histogram_fraction`, and `changes` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga`.
For example: `histogram_quantile(0.9, sum by (job) (increase(metric)))`.

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 1 | Range vector as the result of an instant query: `x[5m]` | Unsupported | Returns the raw samples | - |
| 2 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Native histogram as the result: `my_histogram`, `increase(my_histogram)` | Unsupported. Wrap it in one of the functions above | Returns the histograms | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 3 | {applies_to}`stack: ga =9.5` `NaN` and infinite results: `x / 0`, `ln(vector(-1))`, `scalar()` of several series | Series left out | Returned as `NaN`, `+Inf`, or `-Inf` | - |
:::

### Literals [prometheus-compatibility-literals]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 4 | `step()`, `range()`, `min_of()`, and `max_of()` in a duration: `x[step()]`, `x[min_of(5m, step())]` | Unsupported. Duration arithmetic, such as `x[5m + 1m]`, is supported | Supported with the `promql-duration-expr` feature flag | - |
| 5 | {applies_to}`stack: ga =9.5` Durations as numbers: `1h`, `5m - 1m` | Unsupported | Seconds, such as `3600` for `1h` | - |
| 6 | {applies_to}`stack: ga =9.5` String literal as the query: `"text"` | Instant query: an empty scalar. Range query: runs | Instant query: the string. Range query: an error | - |
:::

### Time series selectors [prometheus-compatibility-time-series-selectors]

#### Instant vector selectors [prometheus-compatibility-instant-vector-selectors]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 7 | No metric name: `{job="api"}` | Unsupported | Supported | [#146977](https://github.com/elastic/elasticsearch/issues/146977) |
| 8 | Regex matcher on `__name__`: `{__name__=~"node_.*"}`, `{__name__!~"up", job="node"}` | Unsupported | Supported | [#146977](https://github.com/elastic/elasticsearch/issues/146977) |
| 9 | Negative matcher on `__name__`: `{__name__!="up", job="node"}` | No results | Every other matching metric | [#146977](https://github.com/elastic/elasticsearch/issues/146977) |
:::

#### Range vector selectors [prometheus-compatibility-range-vector-selectors]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 10 | `anchored` and `smoothed` modifiers: `increase(x[5m] anchored)`, `rate(x[5m] smoothed)` | Unsupported | Supported with the `promql-extended-range-selectors` feature flag | - |
:::

#### Offset modifier [prometheus-compatibility-offset-modifier]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 11 | Negative offset at the end of the query range: `x offset -5m` | Doesn't read past the query end, so the last steps have no value | Reads samples after the query end | - |
| 12 | {applies_to}`stack: preview =9.4` `offset` | Unsupported | Supported | - |
:::

#### @ modifier [prometheus-compatibility-at-modifier]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 13 | `@`: `x @ 1700000000`, `x @ start()`, `x @ end()` | Unsupported | Supported | [#164139](https://github.com/elastic/elasticsearch/issues/164139) |
:::

### Subquery [prometheus-compatibility-subquery]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 14 | Subqueries: `max_over_time(rate(x[5m])[1h:1m])`, `max_over_time(x[10m:])` | Unsupported | Supported | [#164139](https://github.com/elastic/elasticsearch/issues/164139) |
:::

### Gotchas [prometheus-compatibility-gotchas]

#### Staleness [prometheus-compatibility-staleness]

A selector without a range, such as `http_requests_total`, returns the latest sample of each series at each evaluation time.

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 15 | Lookback in instant queries (`/api/v1/query`) | Fixed at five minutes | Five minutes by default, configurable with `lookback_delta` | [#149939](https://github.com/elastic/elasticsearch/issues/149939) |
| 16 | Lookback in range queries (`/api/v1/query_range`) | One `step`. A series disappears at the first step without its own sample | Five minutes. The last sample repeats until then | - |
| 17 | Staleness markers | Ignored. A series disappears once its samples leave the lookback window | A series disappears at its staleness marker | - |
| 18 | {applies_to}`stack: preview =9.4` Instant queries | A five-minute range query ending at `time`, returning the last sample per series | Evaluated at `time` | - |
| 19 | {applies_to}`stack: preview =9.4` Range query time buckets | Aligned to calendar boundaries. Results can differ for short ranges or large steps | Aligned to the query start | - |
:::
