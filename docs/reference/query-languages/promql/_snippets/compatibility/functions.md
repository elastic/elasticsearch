% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Follows https://prometheus.io/docs/prometheus/latest/querying/functions/: rows are sorted by function name.
% Rows are numbered across all snippets in _snippets/compatibility/. A row without a version badge applies to every
% version.

Follows [Functions](https://prometheus.io/docs/prometheus/latest/querying/functions/) in the Prometheus documentation, sorted by function name.
Each [function reference entry](/reference/query-languages/promql/functions.md) lists further differences.
Prometheus drops the metric name (`__name__`) from the result of most functions.

:::{table}
:widths: 1-4-3-3-1

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 42 | Functions listed under [Not yet supported](/reference/query-languages/promql/functions.md#promql-not-supported), such as `absent`, `resets`, `info`, and `sort_by_label` | Unsupported | Supported | [#164001](https://github.com/elastic/elasticsearch/issues/164001), [#164003](https://github.com/elastic/elasticsearch/issues/164003), [#164002](https://github.com/elastic/elasticsearch/issues/164002) |
| 43 | Functions over a series: math functions such as `abs`, `round`, and `clamp`; `timestamp` and the date functions with an argument; `rate`, `irate`, `increase`, `delta`, `idelta`, `deriv`, and `changes`; the `_over_time` functions except `first_over_time` and `last_over_time`; `histogram_quantile` and `histogram_fraction` | Keeps `__name__` | Drops `__name__` | [#150318](https://github.com/elastic/elasticsearch/issues/150318) |
| 44 | Parameter that isn't a number literal: `clamp_min(a, scalar(b))`, `histogram_quantile(scalar(a), b)` | Unsupported | Supported | - |
| 45 | `absent_over_time` with no data or over a gap: `absent_over_time(x{job="none"}[5m])` | No results | `1`, with the labels of the selector's equality matchers | - |
| 46 | `histogram_count`, `histogram_sum`, or `histogram_avg` on classic buckets (float series with an `le` label) | Unsupported. Native histograms only | No results | - |
| 47 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `histogram_quantile` or `histogram_fraction` over `topk`, `bottomk`, `limitk`, or `limit_ratio`: `histogram_quantile(0.9, topk(5, a))` | Unsupported, unless the input is first aggregated with `by(...)`: `histogram_quantile(0.9, topk(5, sum by (job, le) (a)))` | Supported | - |
| 48 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `histogram_quantile` or `histogram_fraction` over `without(...)`: `histogram_quantile(0.9, sum without (instance) (rate(x[5m])))` | Unsupported. Use `by(...)` | Supported | [#157671](https://github.com/elastic/elasticsearch/issues/157671) |
| 49 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `histogram_stddev`, `histogram_stdvar` | Unsupported | Supported | [#164152](https://github.com/elastic/elasticsearch/issues/164152) |
| 50 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `irate`, `delta` on native histograms | Unsupported | Supported | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 51 | `label_replace` or `label_join` outside `by(...)`: `label_replace(x, "dst", "$1", "instance", "(.*):.*")` | Unsupported. Supported inside `sum by (dst) (label_replace(...))` | Supported | [#157672](https://github.com/elastic/elasticsearch/issues/157672) |
| 52 | `label_replace` or `label_join` over an aggregation: `label_replace(sum by (job) (x), ...)` | Unsupported | Supported | [#157672](https://github.com/elastic/elasticsearch/issues/157672) |
| 53 | `label_replace` or `label_join` that writes a label named `value` or `step` | Unsupported | Supported | - |
| 54 | `predict_linear` | Unsupported | Supported | [#164004](https://github.com/elastic/elasticsearch/issues/164004) |
| 55 | `quantile_over_time` | Approximate, using t-digest | Exact, with linear interpolation | - |
| 56 | `rate`, `increase` | Computed over fixed time buckets. Values can differ, especially when an evaluation time falls on a sample | Extrapolated within each range window | - |
| 57 | {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `rate` on native histograms | Unsupported. Use `increase`: `histogram_quantile(0.99, sum by (job) (increase(metric)))` | Supported | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 58 | `sort` or `sort_desc` below the top level: `sum(sort(x))` | Unsupported | Supported, with no effect | - |
| 59 | `step()`, `range()`, `start()`, `end()` | Unsupported | Supported with the `promql-experimental-functions` feature flag | - |
:::
