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
:::

### Literals [prometheus-compatibility-literals]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 3 | `step()`, `range()`, `min_of()`, and `max_of()` in a duration: `x[step()]`, `x[min_of(5m, step())]` | Unsupported. Duration arithmetic, such as `x[5m + 1m]`, is supported | Supported with the `promql-duration-expr` feature flag | - |
:::

### Time series selectors [prometheus-compatibility-time-series-selectors]

#### Instant vector selectors [prometheus-compatibility-instant-vector-selectors]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 4 | No metric name: `{job="api"}` | Unsupported | Supported | [#146977](https://github.com/elastic/elasticsearch/issues/146977) |
| 5 | Regex matcher on `__name__`: `{__name__=~"node_.*"}`, `{__name__!~"up", job="node"}` | Unsupported | Supported | [#146977](https://github.com/elastic/elasticsearch/issues/146977) |
| 6 | Negative matcher on `__name__`: `{__name__!="up", job="node"}` | No results | Every other matching metric | [#146977](https://github.com/elastic/elasticsearch/issues/146977) |
:::

#### Range vector selectors [prometheus-compatibility-range-vector-selectors]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 7 | `anchored` and `smoothed` modifiers: `increase(x[5m] anchored)`, `rate(x[5m] smoothed)` | Unsupported | Supported with the `promql-extended-range-selectors` feature flag | - |
:::

#### Offset modifier [prometheus-compatibility-offset-modifier]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 8 | Negative offset at the end of the query range: `x offset -5m` | Doesn't read past the query end, so the last steps have no value | Reads samples after the query end | - |
:::

#### @ modifier [prometheus-compatibility-at-modifier]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 9 | `@`: `x @ 1700000000`, `x @ start()`, `x @ end()` | Unsupported | Supported | [#164139](https://github.com/elastic/elasticsearch/issues/164139) |
:::

### Subquery [prometheus-compatibility-subquery]

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 10 | Subqueries: `max_over_time(rate(x[5m])[1h:1m])`, `max_over_time(x[10m:])` | Unsupported | Supported | [#164139](https://github.com/elastic/elasticsearch/issues/164139) |
:::

### Gotchas [prometheus-compatibility-gotchas]

#### Staleness [prometheus-compatibility-staleness]

A selector without a range, such as `http_requests_total`, returns the latest sample of each series at each evaluation time.

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 11 | Lookback in instant queries (`/api/v1/query`) | Fixed at five minutes | Five minutes by default, configurable with `lookback_delta` | [#149939](https://github.com/elastic/elasticsearch/issues/149939) |
| 12 | Lookback in range queries (`/api/v1/query_range`) | One `step`. A series disappears at the first step without its own sample | Five minutes. The last sample repeats until then | - |
| 13 | Staleness markers | Ignored. A series disappears once its samples leave the lookback window | A series disappears at its staleness marker | - |
:::
