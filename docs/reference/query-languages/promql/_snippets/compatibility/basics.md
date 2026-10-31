% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Headings follow https://prometheus.io/docs/prometheus/latest/querying/basics/. Rows are numbered across all
% snippets in _snippets/compatibility/. A row without a version badge applies to every version.

These rows follow [Querying basics](https://prometheus.io/docs/prometheus/latest/querying/basics/) in the Prometheus documentation.

### Expression language data types [prometheus-compatibility-expression-language-data-types]

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` {{es}} supports Prometheus native histograms through the `exponential_histogram` field type.
`histogram_quantile`, `increase`, and `sum` work on native histograms, and so do `histogram_count`, `histogram_sum`, `histogram_avg`, `histogram_fraction`, and `changes` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga`.
For example, `histogram_quantile(0.9, sum by (job) (increase(metric)))` works.

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 1 | A range vector as the result of an instant query: `x[5m]` | Error | Returns the raw samples of each series | - |
| 2 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` A raw native histogram as the result: `my_histogram`, `increase(my_histogram)` | Error. Wrap it in `histogram_quantile`, `histogram_count`, `histogram_sum`, `histogram_avg`, `histogram_fraction`, or `changes` | Returns the histograms | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 3 | {applies_to}`stack: ga =9.5` Results that are `NaN` or infinite: `x / 0`, `ln(vector(-1))`, `asin(x)` outside its domain, `histogram_quantile(NaN, ...)`, `scalar()` of several series | Leaves those series out | Returns `NaN`, `+Inf`, or `-Inf` | - |

### Literals [prometheus-compatibility-literals]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 4 | Duration functions in a duration expression: `x[step()]`, `x[range()]`, `x[min_of(5m, step())]`, `x[max_of(step(), 5s)]` | Error. Arithmetic in durations, such as `rate(x[5m + 1m])`, is supported | Evaluates when the `promql-duration-expr` feature flag is enabled | - |
| 5 | {applies_to}`stack: ga =9.5` Durations as numbers: `1h`, `90s`, `5m - 1m` | Error | The number of seconds, such as `3600` for `1h` | - |
| 6 | {applies_to}`stack: ga =9.5` A string literal as the query: `"text"` | Instant queries return an empty scalar. Range queries run | Instant queries return the string. Range queries return an error | - |

### Time series selectors [prometheus-compatibility-time-series-selectors]

#### Instant vector selectors [prometheus-compatibility-instant-vector-selectors]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 7 | Selectors without a metric name: `{job="api"}` | Error | Every series that matches | [#146977](https://github.com/elastic/elasticsearch/issues/146977) |
| 8 | Regex matchers on `__name__`: `{__name__=~"node_.*"}`, `{__name__!~"up", job="node"}` | Error | Every series whose name matches | [#146977](https://github.com/elastic/elasticsearch/issues/146977) |
| 9 | A negative matcher on `__name__`: `{__name__!="up", job="node"}` | No results | Every other metric that matches | [#146977](https://github.com/elastic/elasticsearch/issues/146977) |

#### Range vector selectors [prometheus-compatibility-range-vector-selectors]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 10 | The `anchored` and `smoothed` range modifiers: `increase(x[5m] anchored)`, `rate(x[5m] smoothed)` | Error | Evaluates when the `promql-extended-range-selectors` feature flag is enabled | - |

#### Offset modifier [prometheus-compatibility-offset-modifier]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 11 | A negative offset near the end of the query range: `x offset -5m` | Reads no samples after the end of the query range, so the last evaluation times have no value | Reads samples after the end of the query range | - |
| 12 | {applies_to}`stack: preview =9.4` The `offset` modifier | Error | Evaluates | - |

#### @ modifier [prometheus-compatibility-at-modifier]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 13 | The `@` modifier: `x @ 1700000000`, `x @ start()`, `x @ end()` | Error | Evaluates the selector at the given time | [#164139](https://github.com/elastic/elasticsearch/issues/164139) |

### Subquery [prometheus-compatibility-subquery]

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 14 | Subqueries: `max_over_time(rate(x[5m])[1h:1m])`, `max_over_time(x[10m:])` | Error | Evaluates the inner expression at each step of the subquery range | [#164139](https://github.com/elastic/elasticsearch/issues/164139) |

### Gotchas [prometheus-compatibility-gotchas]

#### Staleness [prometheus-compatibility-staleness]

A selector without a range, such as `http_requests_total`, returns the latest sample of each series at every evaluation time.

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 15 | Instant queries: `/api/v1/query` | Looks back a fixed five minutes for the latest sample. The `lookback_delta` parameter isn't supported | Looks back five minutes by default, configurable with `lookback_delta` | [#149939](https://github.com/elastic/elasticsearch/issues/149939) |
| 16 | Range queries: a series that stops reporting, in `/api/v1/query_range` | Looks back one `step`. The series disappears at the first step without a sample of its own | Keeps returning the last sample for up to five minutes | - |
| 17 | Staleness markers: a scrape target that disappears | Ignores staleness markers. The series disappears only once all its samples fall outside the lookback window | Drops the series as soon as the staleness marker arrives | - |
| 18 | {applies_to}`stack: preview =9.4` Instant queries: `/api/v1/query` | A five-minute range query ending at `time`, returning the last sample per series | Evaluates the expression at `time` | - |
| 19 | {applies_to}`stack: preview =9.4` Time buckets of range queries | Aligned to fixed calendar boundaries, so results can differ slightly, especially for short ranges or large steps | Aligned to the query start time | - |
