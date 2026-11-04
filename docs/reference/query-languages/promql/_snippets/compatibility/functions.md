% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Follows https://prometheus.io/docs/prometheus/latest/querying/functions/: rows are sorted by function name.
% Rows are numbered across all snippets in _snippets/compatibility/, in page order. A row without a version badge
% applies to every version. "Not supported yet" columns: # | Construct | Example | Workaround | Issue.
% "Behaves differently" columns: # | Construct | Example | Elasticsearch | Prometheus | Issue.

Rows follow [Functions](https://prometheus.io/docs/prometheus/latest/querying/functions/) in the {{prom}} documentation and are sorted by function name.
Each [function reference entry](/reference/query-languages/promql/functions.md) lists further differences.

**Supported**

The functions in the [{{promql}} functions](/reference/query-languages/promql/functions.md) reference, such as `rate`, `increase`, `histogram_quantile`, `label_replace`, and the `<aggregation>_over_time` functions.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 39 | Functions listed under [Not supported yet](/reference/query-languages/promql/functions.md#promql-not-supported) | `absent(x)`, `resets(x[1h])`, `info(x)` | - | [#164001]({{es-issue}}164001), [#164003]({{es-issue}}164003), [#164002]({{es-issue}}164002) |
| 40 | Parameter that isn't a number literal | `clamp_min(a, scalar(b))`, `histogram_quantile(scalar(a), b)` | Use a number | [#164225]({{es-issue}}164225) |
| 41 | `histogram_quantile` or `histogram_fraction` over `topk`, `bottomk`, `limitk`, or `limit_ratio` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `histogram_quantile(0.9, topk(5, a))` | Aggregate with `by(...)` first: `histogram_quantile(0.9, topk(5, sum by (job, le) (a)))` | [#164226]({{es-issue}}164226) |
| 42 | `histogram_quantile` or `histogram_fraction` over `without(...)` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `histogram_quantile(0.9, sum without (instance) (rate(x[5m])))` | Use `by(...)` | [#157671]({{es-issue}}157671) |
| 43 | `histogram_stddev`, `histogram_stdvar` {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `histogram_stddev(h)` | - | [#164152]({{es-issue}}164152) |
| 44 | `irate`, `delta` on native histograms {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `irate(h[5m])` | - | [#152263]({{es-issue}}152263) |
| 45 | `label_replace` or `label_join` outside `by(...)` | `label_replace(x, "dst", "$1", "instance", "(.*):.*")` | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` Use it inside `by(...)`: `sum by (dst) (label_replace(...))` | [#157672]({{es-issue}}157672) |
| 46 | `label_replace` or `label_join` over an aggregation {applies_to}`stack: ga =9.6` | `label_replace(sum by (job) (x), ...)` | - | [#157672]({{es-issue}}157672) |
| 47 | `label_replace` or `label_join` over anything other than a `by(...)` aggregation, or that overwrites a grouping label {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `label_replace(sum without (instance) (x), "dst", "$1", "job", "(.*)")`, `label_replace(sum by (job) (x), "job", "$1", "job", "(.*)")` | Aggregate with `by(...)` and write a new label: `label_replace(sum by (job) (x), "dst", "$1", "job", "(.*)")` | [#157672]({{es-issue}}157672) |
| 48 | `label_replace` or `label_join` that writes a label named `step` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `label_replace(x, "step", ...)` | `step` is an output column of the `PROMQL` command | - |
| 49 | `predict_linear` with a horizon computed by a function {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `predict_linear(x[1h], time())`, `predict_linear(x[1h], scalar(y))` | Use a number, a duration literal, or arithmetic on literals | [#164225]({{es-issue}}164225) |
| 50 | `rate` on native histograms {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `rate(h[5m])` | Use `increase`: `histogram_quantile(0.99, sum by (job) (increase(h[5m])))` | [#152263]({{es-issue}}152263) |
| 51 | `sort` or `sort_desc` below the top level | `sum(sort(x))` | Remove it. Sorting has no effect there | [#164227]({{es-issue}}164227) |
| 52 | `sort_by_label` or `sort_by_label_desc` below the top level ({{prom}}: `promql-experimental-functions` feature flag) {applies_to}`stack: preview 9.7+` {applies_to}`serverless: preview` | `sum(sort_by_label(x, "job"))` | Remove it. Sorting has no effect there | [#164227]({{es-issue}}164227) |
| 53 | `step()`, `range()`, `start()`, `end()` ({{prom}}: `promql-experimental-functions` feature flag) | `time() - start()` | - | [#164228]({{es-issue}}164228) |
:::

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 54 | `absent_over_time` with no data or over a gap | `absent_over_time(x{job="none"}[5m])` | No results | `1`, with the labels of the selector's equality matchers | - |
| 55 | `histogram_count`, `histogram_sum`, or `histogram_avg` on classic buckets | `histogram_count(rate(x_bucket[5m]))` | An error. They accept only native histograms | No results | - |
| 56 | `quantile_over_time` | `quantile_over_time(0.9, x[5m])` | Approximate, using t-digest | Exact, with linear interpolation | - |
| 57 | `rate`, `increase` | `rate(x[5m])` | Computed over fixed time buckets, interpolated at bucket boundaries. Results can differ slightly | Extrapolated within each range window | - |
:::
