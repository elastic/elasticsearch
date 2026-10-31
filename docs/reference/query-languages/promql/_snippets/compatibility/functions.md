% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Follows https://prometheus.io/docs/prometheus/latest/querying/functions/: rows are sorted by function name.
% Rows are numbered across all snippets in _snippets/compatibility/. A row without a version badge applies to every
% version. Columns: # | Construct | Example | Elasticsearch | Prometheus | Issue.

Follows [Functions](https://prometheus.io/docs/prometheus/latest/querying/functions/) in the Prometheus documentation, sorted by function name.
Each [function reference entry](/reference/query-languages/promql/functions.md) lists further differences.

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | Prometheus | Issue |
|---|---|---|---|---|---|
| 38 | Functions listed under [Not yet supported](/reference/query-languages/promql/functions.md#promql-not-supported) | `absent(x)`, `resets(x[1h])`, `info(x)` | **Unsupported** | Supported | [#164001](https://github.com/elastic/elasticsearch/issues/164001), [#164003](https://github.com/elastic/elasticsearch/issues/164003), [#164002](https://github.com/elastic/elasticsearch/issues/164002) |
| 39 | Parameter that isn't a number literal | `clamp_min(a, scalar(b))`, `histogram_quantile(scalar(a), b)` | **Unsupported** | Supported | - |
| 40 | `absent_over_time` with no data or over a gap | `absent_over_time(x{job="none"}[5m])` | **No results** | `1`, with the labels of the selector's equality matchers | - |
| 41 | `histogram_count`, `histogram_sum`, or `histogram_avg` on classic buckets | `histogram_count(rate(x_bucket[5m]))` | **Unsupported**. Native histograms only | No results | - |
| 42 | `histogram_quantile` or `histogram_fraction` over `topk`, `bottomk`, `limitk`, or `limit_ratio` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `histogram_quantile(0.9, topk(5, a))` | **Unsupported**, unless the input is first aggregated with `by(...)`: `histogram_quantile(0.9, topk(5, sum by (job, le) (a)))` | Supported | - |
| 43 | `histogram_quantile` or `histogram_fraction` over `without(...)` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `histogram_quantile(0.9, sum without (instance) (rate(x[5m])))` | **Unsupported**. Use `by(...)` | Supported | [#157671](https://github.com/elastic/elasticsearch/issues/157671) |
| 44 | `histogram_stddev`, `histogram_stdvar` {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `histogram_stddev(h)` | **Unsupported** | Supported | [#164152](https://github.com/elastic/elasticsearch/issues/164152) |
| 45 | `irate`, `delta` on native histograms {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `irate(h[5m])` | **Unsupported** | Supported | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 46 | `label_replace` or `label_join` outside `by(...)` | `label_replace(x, "dst", "$1", "instance", "(.*):.*")` | **Unsupported**. {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` Supported inside `sum by (dst) (label_replace(...))` | Supported | [#157672](https://github.com/elastic/elasticsearch/issues/157672) |
| 47 | `label_replace` or `label_join` over an aggregation | `label_replace(sum by (job) (x), ...)` | **Unsupported** | Supported | [#157672](https://github.com/elastic/elasticsearch/issues/157672) |
| 48 | `label_replace` or `label_join` that writes a label named `step` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `label_replace(x, "step", ...)` | **Unsupported**. `step` is an output column of the `PROMQL` command | Supported | - |
| 49 | `predict_linear` | `predict_linear(x[1h], 3600)` | **Unsupported** | Supported | [#164004](https://github.com/elastic/elasticsearch/issues/164004) |
| 50 | `quantile_over_time` | `quantile_over_time(0.9, x[5m])` | **Approximate**, using t-digest | Exact, with linear interpolation | - |
| 51 | `rate`, `increase` | `rate(x[5m])` | **Fixed time buckets**, interpolated at bucket boundaries. Results can differ slightly | Extrapolated within each range window | - |
| 52 | `rate` on native histograms {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `rate(h[5m])` | **Unsupported**. Use `increase`: `histogram_quantile(0.99, sum by (job) (increase(h[5m])))` | Supported | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 53 | `sort` or `sort_desc` below the top level | `sum(sort(x))` | **Unsupported** | Supported, with no effect | - |
| 54 | `step()`, `range()`, `start()`, `end()` | `time() - start()` | **Unsupported** | Supported with the `promql-experimental-functions` feature flag | - |
:::
