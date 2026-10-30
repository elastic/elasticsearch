% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Follows https://prometheus.io/docs/prometheus/latest/querying/functions/: rows are sorted by function name.
% Rows are numbered across all snippets in _snippets/compatibility/, in page order. A row without a version badge
% applies to every version. "Not supported yet" columns: # | Construct | Example | Workaround | Issue.
% "Behaves differently" columns: # | Construct | Example | Elasticsearch | Prometheus | Issue.

Follows [Functions](https://prometheus.io/docs/prometheus/latest/querying/functions/) in the {{prom}} documentation, sorted by function name.
Each [function reference entry](/reference/query-languages/promql/functions.md) lists further differences.

**Supported**

The functions in the [{{promql}} functions](/reference/query-languages/promql/functions.md) reference, such as `rate`, `increase`, `histogram_quantile`, `label_replace`, and the `<aggregation>_over_time` functions.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 38 | Functions listed under [Not supported yet](/reference/query-languages/promql/functions.md#promql-not-supported) | `absent(x)`, `resets(x[1h])`, `info(x)` | - | [#164001](https://github.com/elastic/elasticsearch/issues/164001), [#164003](https://github.com/elastic/elasticsearch/issues/164003), [#164002](https://github.com/elastic/elasticsearch/issues/164002) |
| 39 | Parameter that isn't a number literal | `clamp_min(a, scalar(b))`, `histogram_quantile(scalar(a), b)` | Use a number | - |
| 40 | `histogram_quantile` or `histogram_fraction` over `topk`, `bottomk`, `limitk`, or `limit_ratio` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `histogram_quantile(0.9, topk(5, a))` | Aggregate with `by(...)` first: `histogram_quantile(0.9, topk(5, sum by (job, le) (a)))` | - |
| 41 | `histogram_quantile` or `histogram_fraction` over `without(...)` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `histogram_quantile(0.9, sum without (instance) (rate(x[5m])))` | Use `by(...)` | [#157671](https://github.com/elastic/elasticsearch/issues/157671) |
| 42 | `histogram_stddev`, `histogram_stdvar` {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `histogram_stddev(h)` | - | [#164152](https://github.com/elastic/elasticsearch/issues/164152) |
| 43 | `irate`, `delta` on native histograms {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `irate(h[5m])` | - | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 44 | `label_replace` or `label_join` outside `by(...)` | `label_replace(x, "dst", "$1", "instance", "(.*):.*")` | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` Use it inside `by(...)`: `sum by (dst) (label_replace(...))` | [#157672](https://github.com/elastic/elasticsearch/issues/157672) |
| 45 | `label_replace` or `label_join` over an aggregation | `label_replace(sum by (job) (x), ...)` | - | [#157672](https://github.com/elastic/elasticsearch/issues/157672) |
| 46 | `label_replace` or `label_join` that writes a label named `step` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `label_replace(x, "step", ...)` | `step` is an output column of the `PROMQL` command | - |
| 47 | `predict_linear` | `predict_linear(x[1h], 3600)` | - | [#164004](https://github.com/elastic/elasticsearch/issues/164004) |
| 48 | `rate` on native histograms {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `rate(h[5m])` | Use `increase`: `histogram_quantile(0.99, sum by (job) (increase(h[5m])))` | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 49 | `sort` or `sort_desc` below the top level | `sum(sort(x))` | Remove it. Sorting has no effect there | - |
| 50 | `step()`, `range()`, `start()`, `end()` ({{prom}}: `promql-experimental-functions` feature flag) | `time() - start()` | - | - |
:::

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 51 | `absent_over_time` with no data or over a gap | `absent_over_time(x{job="none"}[5m])` | No results | `1`, with the labels of the selector's equality matchers | - |
| 52 | `histogram_count`, `histogram_sum`, or `histogram_avg` on classic buckets | `histogram_count(rate(x_bucket[5m]))` | An error. They accept only native histograms | No results | - |
| 53 | `quantile_over_time` | `quantile_over_time(0.9, x[5m])` | Approximate, using t-digest | Exact, with linear interpolation | - |
| 54 | `rate`, `increase` | `rate(x[5m])` | Computed over fixed time buckets, interpolated at bucket boundaries. Results can differ slightly | Extrapolated within each range window | - |
:::
