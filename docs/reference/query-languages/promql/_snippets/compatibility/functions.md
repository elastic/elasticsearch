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

The functions in the [{{promql}} functions](/reference/query-languages/promql/functions.md) reference, such as `rate`, `increase`, `histogram_quantile`, `label_replace`, `absent` {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga`, and the `<aggregation>_over_time` functions.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 45 | Functions listed under [Not supported yet](/reference/query-languages/promql/functions.md#promql-not-supported) | `resets(x[1h])`, `info(x)` | - | [#164003]({{es-issue}}164003), [#164002]({{es-issue}}164002) |
| 46 | `absent` as an operand of an arithmetic or comparison operator whose other operand reads data {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `absent(a) + b`, `b > bool absent(a)` | - | - |
| 47 | `or` between `absent` and a vector whose label of the same name is mapped as a type other than `keyword` {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `sum by (shard) (x) or absent(x{shard="7"})`, where `shard` is a `long` field | - | - |
| 48 | Parameter that isn't a number literal | `clamp_min(a, scalar(b))`, `histogram_quantile(scalar(a), b)` | Use a number | [#164225]({{es-issue}}164225) |
| 49 | `histogram_quantile` or `histogram_fraction` over `topk`, `bottomk`, `limitk`, or `limit_ratio` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `histogram_quantile(0.9, topk(5, a))` | Aggregate with `by(...)` first: `histogram_quantile(0.9, topk(5, sum by (job, le) (a)))` | [#164226]({{es-issue}}164226) |
| 50 | `histogram_quantile` or `histogram_fraction` over `without(...)` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `histogram_quantile(0.9, sum without (instance) (rate(x[5m])))` | Use `by(...)` | [#157671]({{es-issue}}157671) |
| 51 | `histogram_stddev`, `histogram_stdvar` {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `histogram_stddev(h)` | - | [#164152]({{es-issue}}164152) |
| 52 | `irate`, `delta` on native histograms {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `irate(h[5m])` | - | [#152263]({{es-issue}}152263) |
| 53 | `label_replace` or `label_join` outside `by(...)` | `label_replace(x, "dst", "$1", "instance", "(.*):.*")` | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` Use it inside `by(...)`: `sum by (dst) (label_replace(...))` | [#157672]({{es-issue}}157672) |
| 54 | `label_replace` or `label_join` over an aggregation {applies_to}`stack: ga =9.6` | `label_replace(sum by (job) (x), ...)` | - | [#157672]({{es-issue}}157672) |
| 55 | `label_replace` or `label_join` over anything other than a `by(...)` aggregation, `count_values`, or `absent`, or that overwrites a grouping label or the `count_values` label {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `label_replace(sum without (instance) (x), "dst", "$1", "job", "(.*)")`, `label_replace(sum by (job) (x), "job", "$1", "job", "(.*)")` | Aggregate with `by(...)` and write a new label: `label_replace(sum by (job) (x), "dst", "$1", "job", "(.*)")` | [#157672]({{es-issue}}157672) |
| 56 | `label_replace` or `label_join` that writes a label named `step` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga`, or `absent` of a selector with an equality matcher on `step` or on the value column (set by `name=(...)`) {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `label_replace(x, "step", ...)`, `absent(x{step="a"})`, `result=(absent(x{result="ok"}))` | `step` and the value column are output columns of the `PROMQL` command. For the value column, pick another name | - |
| 57 | `predict_linear` with a horizon computed by a function {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `predict_linear(x[1h], time())`, `predict_linear(x[1h], scalar(y))` | Use a number, a duration literal, or arithmetic on literals | [#164225]({{es-issue}}164225) |
| 58 | `rate` on native histograms {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `rate(h[5m])` | Use `increase`: `histogram_quantile(0.99, sum by (job) (increase(h[5m])))` | [#152263]({{es-issue}}152263) |
| 59 | `sort` or `sort_desc` below the top level | `sum(sort(x))` | Remove it. Sorting has no effect there | [#164227]({{es-issue}}164227) |
| 60 | `sort_by_label` or `sort_by_label_desc` below the top level ({{prom}}: `promql-experimental-functions` feature flag) {applies_to}`stack: preview 9.7+` {applies_to}`serverless: preview` | `sum(sort_by_label(x, "job"))` | Remove it. Sorting has no effect there | [#164227]({{es-issue}}164227) |
| 61 | `step()`, `range()`, `start()`, `end()` ({{prom}}: `promql-experimental-functions` feature flag) | `time() - start()` | - | [#164228]({{es-issue}}164228) |
:::

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 62 | `absent_over_time` with no data or over a gap | `absent_over_time(x{job="none"}[5m])` | No results | `1`, with the labels of the selector's equality matchers | - |
| 63 | `histogram_count`, `histogram_sum`, or `histogram_avg` on classic buckets | `histogram_count(rate(x_bucket[5m]))` | An error. They accept only native histograms | No results | - |
| 64 | `quantile_over_time` | `quantile_over_time(0.9, x[5m])` | Approximate, using t-digest | Exact, with linear interpolation | - |
| 65 | `rate`, `increase` | `rate(x[5m])` | Computed over fixed time buckets, interpolated at bucket boundaries. Results can differ slightly | Extrapolated within each range window | - |
:::
