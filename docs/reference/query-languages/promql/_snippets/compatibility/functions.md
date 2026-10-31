% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Follows https://prometheus.io/docs/prometheus/latest/querying/functions/: rows are sorted by function name.
% Rows are numbered across all snippets in _snippets/compatibility/. A row without a version badge applies to every
% version.

These rows follow [Functions](https://prometheus.io/docs/prometheus/latest/querying/functions/) in the Prometheus documentation.
Further differences are listed under **Differences from Prometheus** on each [function's reference entry](/reference/query-languages/promql/functions.md).
Prometheus drops the metric name (`__name__`) from the result of most functions, because the result is no longer a sample of that metric.

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 53 | Functions listed under [Not yet supported](/reference/query-languages/promql/functions.md#promql-not-supported), such as `absent`, `resets`, `info`, and `sort_by_label` | Error | Evaluates | [#164001](https://github.com/elastic/elasticsearch/issues/164001), [#164003](https://github.com/elastic/elasticsearch/issues/164003), [#164002](https://github.com/elastic/elasticsearch/issues/164002) |
| 54 | Functions over a series: math functions such as `abs`, `round`, and `clamp`; `timestamp` and the date functions with an argument; `rate`, `irate`, `increase`, `delta`, `idelta`, `deriv`, and `changes`; the `_over_time` functions other than `first_over_time` and `last_over_time`; `histogram_quantile` and `histogram_fraction` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 55 | Parameters that aren't number literals: `clamp_min(a, scalar(b))`, `histogram_quantile(scalar(a), b)` | Error | Evaluates | - |
| 56 | `absent_over_time` for a selector without data, or over a gap: `absent_over_time(x{job="none"}[5m])` | No results | `1`, with the labels of the selector's equality matchers | - |
| 57 | `histogram_count`, `histogram_sum`, or `histogram_avg` over classic histogram buckets (float series with an `le` label) | Error. They accept only native histograms | No results | - |
| 58 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `histogram_quantile` or `histogram_fraction` over `topk`, `bottomk`, `limitk`, or `limit_ratio`, unless the input is first aggregated with `by(...)`: `histogram_quantile(0.9, topk(5, a))`. `histogram_quantile(0.9, topk(5, sum by (job, le) (a)))` is supported | Error | Evaluates | - |
| 59 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `histogram_quantile` or `histogram_fraction` over a `without(...)` aggregation: `histogram_quantile(0.9, sum without (instance) (rate(x[5m])))`. Use `by(...)` instead | Error | Evaluates | [#157671](https://github.com/elastic/elasticsearch/issues/157671) |
| 60 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `histogram_stddev` and `histogram_stdvar` | Error | Evaluates | [#164152](https://github.com/elastic/elasticsearch/issues/164152) |
| 61 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `irate` and `delta` over native histograms | Error | Evaluates | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 62 | `label_replace` or `label_join` outside a `by(...)` aggregation: `label_replace(x, "dst", "$1", "instance", "(.*):.*")` | Error. Supported inside `sum by (dst) (label_replace(...))` | Evaluates | [#157672](https://github.com/elastic/elasticsearch/issues/157672) |
| 63 | `label_replace` or `label_join` over an aggregation: `label_replace(sum by (job) (x), ...)` | Error | Evaluates | [#157672](https://github.com/elastic/elasticsearch/issues/157672) |
| 64 | `label_replace` or `label_join` that writes a label named `value` or `step` | Error | Evaluates | - |
| 65 | `predict_linear` | Error | Evaluates | [#164004](https://github.com/elastic/elasticsearch/issues/164004) |
| 66 | `quantile_over_time` | Approximate, computed with the t-digest percentile aggregation | Exact, with linear interpolation | - |
| 67 | `rate` and `increase` | Computed over fixed time buckets. Values can differ, especially when an evaluation time falls exactly on a sample | Extrapolated within each range window | - |
| 68 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `rate` over native histograms | Error. Use `increase` instead, such as `histogram_quantile(0.99, sum by (job) (increase(metric)))` | Evaluates | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 69 | `sort` or `sort_desc` below the top level: `sum(sort(x))` | Error | Evaluates. Sorting has no effect there | - |
| 70 | `step()`, `range()`, `start()`, and `end()` | Error | Evaluates when the `promql-experimental-functions` feature flag is enabled | - |
| 71 | {applies_to}`stack: ga =9.5` `sort`, `sort_desc`, `timestamp`, `changes`, `histogram_fraction`, `label_replace`, and `label_join` | Error | Evaluates | - |
