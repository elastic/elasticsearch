% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Headings follow https://prometheus.io/docs/prometheus/latest/querying/operators/.
% Rows are numbered across all snippets in _snippets/compatibility/, in page order. A row without a version badge
% applies to every version. "Not supported yet" columns: # | Construct | Example | Workaround | Issue.
% "Behaves differently" columns: # | Construct | Example | Elasticsearch | Prometheus | Issue.

Sections follow [Operators](https://prometheus.io/docs/prometheus/latest/querying/operators/) in the {{prom}} documentation.
Each [operator reference entry](/reference/query-languages/promql/operators.md) lists further differences.

### Binary operators [prometheus-compatibility-binary-operators]

#### Arithmetic binary operators [prometheus-compatibility-arithmetic-binary-operators]

**Supported**

`+`, `-`, `*`, `/`, `%`, and `^` between scalars, between an instant vector and a scalar, such as `rate(x[5m]) * 60`, and between two instant vectors.
[Vector matching](#prometheus-compatibility-vector-matching) lists which pairs of instant vectors match.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 14 | Native histogram operands {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `h * 2`, `h1 + h2` | - | [#152263]({{es-issue}}152263) |
:::

#### Trigonometric binary operators [prometheus-compatibility-trigonometric-binary-operators]

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 15 | `atan2` | `x atan2 y` | - | [#164153]({{es-issue}}164153) |
:::

#### Comparison binary operators [prometheus-compatibility-comparison-binary-operators]

**Supported**

`==`, `!=`, `>`, `<`, `>=`, and `<=` between an instant vector and a number, such as `rate(x[5m]) > 100`, with or without `bool`, such as `x > bool 100`.

{applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` A scalar as either operand, such as `100 < x`, `time() <= x`, or `x > scalar(y)`, two instant vectors, such as `x > y`, and a comparison inside another expression, such as `(x > 100) + 1` or `count(x > 100)`.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 16 | Filter with a scalar as the first operand {applies_to}`stack: preview =9.4, ga 9.5-9.6` | `100 < x`, `time() <= x` | Swap the operands: `x > 100` | [#158184]({{es-issue}}158184) |
| 17 | Non-literal scalar as the second operand {applies_to}`stack: preview =9.4, ga 9.5-9.6` | `x > time()`, `x > scalar(y)` | - | [#158184]({{es-issue}}158184) |
| 18 | Two vectors, or `bool` with a scalar as the first operand {applies_to}`stack: preview =9.4, ga 9.5-9.6` | `x > y`, `100 > bool x` | For `bool`, swap the operands: `x < bool 100` | [#158184]({{es-issue}}158184) |
| 19 | Two different metrics stored as separate series with a `__name__` dimension, such as data ingested with {{prom}} remote write {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `a > b` | Aggregate both by the labels to compare on: `sum by (instance) (a) > sum by (instance) (b)` | [#158184]({{es-issue}}158184) |
| 20 | Comparison inside another expression {applies_to}`stack: preview =9.4, ga 9.5-9.6` | `(x > 100) + 1`, `count(x > 100)` | - | [#158184]({{es-issue}}158184) |
:::

#### Logical/set binary operators [prometheus-compatibility-logical-set-binary-operators]

**Supported**

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `or` between instant vectors, such as `rate(a[5m]) or rate(b[5m])`.
An `or` chain takes at most 8 operands by default.

{applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` Change the limit with the `esql.query.max_branch_count_per_merge` cluster setting.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 21 | `and`, `unless` | `a and b`, `a unless b` | - | [#158184]({{es-issue}}158184) |
| 22 | `or` with `on(...)` or `ignoring(...)` | `a or on(job) b` | - | [#158184]({{es-issue}}158184) |
| 23 | `or` inside another expression | `(sum(a) or sum(b)) * 2`, `count(a or b)` | - | [#158184]({{es-issue}}158184) |
| 24 | `or` between an aggregated and an unaggregated operand, or between different metrics stored as separate series with a `__name__` dimension, such as data ingested with {{prom}} remote write {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `sum by (job) (a) or b`, `a or b` | Aggregate both operands alike: `sum by (job) (a) or sum by (job) (b)` | [#158184]({{es-issue}}158184) |
:::

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 25 | `or` between an aggregated and an unaggregated operand, or between different metrics stored as separate series with a `__name__` dimension {applies_to}`stack: ga 9.5-9.6` | `sum by (job) (a) or b`, `a or b` | Can keep a series of the right operand that a left one matches | Keeps only the left one | [#158184]({{es-issue}}158184) |
:::

### Vector matching [prometheus-compatibility-vector-matching]

**Supported**

Arithmetic between unaggregated metrics stored together in the same time series, such as `network.bytes_in / network.connections`, without explicit vector matching modifiers.

Arithmetic between operands aggregated without grouping, such as `sum(a) / sum(b)`.

{applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` One-to-one, many-to-one, and one-to-many matching between operands aggregated with `by(...)`, including `on`, `ignoring`, `group_left`, and `group_right`.
For example: `sum by (instance) (a) / on(instance) sum by (instance) (b)`.

{applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` Arithmetic between different unaggregated metrics stored as separate series with a `__name__` dimension, such as data ingested with {{prom}} remote write: `a / b` matches series on their other labels.

{applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` A different `offset` per operand, such as `rate(x[5m]) / rate(x[5m] offset 1h)`.

{applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` `on`, `ignoring`, `group_left`, and `group_right` with operands that aren't aggregated, such as `a * on(pod) group_left(node) b`, and operands aggregated differently, such as `sum(a) / b`, `b / scalar(sum(a))`, or `count(count by (pod) (a)) / sum(b)`.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 26 | Different unaggregated metrics stored as separate series with a `__name__` dimension, such as data ingested with {{prom}} remote write {applies_to}`stack: ga =9.6` | `a / b` | Aggregate both by the labels to match on: `sum by (instance) (a) / sum by (instance) (b)` | [#158184]({{es-issue}}158184) |
| 27 | `on`, `ignoring`, `group_left`, or `group_right` with an operand that isn't aggregated {applies_to}`stack: preview =9.4, ga 9.5-9.6` | `a / on(instance) b`, `a * on(pod) group_left(node) b` | Aggregate both operands by the labels to match on | [#157669]({{es-issue}}157669) |
| 28 | Operands aggregated differently {applies_to}`stack: ga 9.5-9.6` | `sum(a) / b`, `b / scalar(sum(a))`, `topk(2, a) * b` | Aggregate both operands alike: `sum(a) / sum(b)`. A number, such as `sum(a) * 2`, and `or` are supported | [#158184]({{es-issue}}158184) |
| 29 | Nested aggregation next to an operand that reads a metric {applies_to}`stack: ga 9.5-9.6` | `count(count by (pod) (a)) / sum(b)` | - | [#158184]({{es-issue}}158184) |
| 30 | A different `offset` per operand {applies_to}`stack: ga 9.5-9.6` | `rate(x[5m]) / rate(x[5m] offset 1h)` | `or` is supported | [#158184]({{es-issue}}158184) |
| 31 | `topk`, `bottomk`, `limitk`, or `limit_ratio` next to an operand that isn't aggregated alike {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `topk(2, a) * b` | Aggregate the other operand with `by(...)` | [#158184]({{es-issue}}158184) |
| 32 | Matching on `__name__` {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `a / on(__name__, pod) b`, `a * on(pod) group_left(__name__) b` | - | [#146989]({{es-issue}}146989) |
| 33 | `label_replace` or `label_join` as an operand | `label_replace(a, ...) * on(pod) group_left(node) b` | - | [#157672]({{es-issue}}157672) |
| 34 | `fill()`, `fill_left()`, and `fill_right()` ({{prom}}: `promql-binop-fill-modifiers` feature flag) | `a + fill(0) b` | - | [#164223]({{es-issue}}164223) |
:::

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 35 | Different unaggregated metrics stored as separate series {applies_to}`stack: preview =9.4, ga =9.5` | `a / b` | No results | Series matched on their other labels | [#158184]({{es-issue}}158184) |
:::

### Aggregation operators [prometheus-compatibility-aggregation-operators]

**Supported**

- `sum`, `avg`, `min`, `max`, `count`, `quantile`, `stddev`, and `stdvar`, with `by(...)`, `without(...)`, or no grouping.
- {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `topk`, `bottomk`, `limitk`, and `limit_ratio`.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 36 | `group`, `count_values` | `group by (job) (x)`, `count_values("v", x)` | - | [#164009]({{es-issue}}164009), [#136248]({{es-issue}}136248) |
| 37 | `topk`, `bottomk`, `limitk`, or `limit_ratio` with `without(...)` | `topk without (instance) (1, x)` | Use `by(...)` | [#157671]({{es-issue}}157671) |
| 38 | Parameter that isn't a number literal | `topk(scalar(a), b)`, `quantile(scalar(a), b)` | Use a number | [#164225]({{es-issue}}164225) |
| 39 | Nested `without(...)` | `sum without (pod) (max without (container) (x))` | Use `by(...)` | [#157671]({{es-issue}}157671) |
| 40 | `without(...)` as an operand {applies_to}`stack: preview =9.4, ga 9.5-9.6` | `sum without (method) (x) * 2` | Use `by(...)` | [#145308]({{es-issue}}145308) |
| 41 | Grouping by a label named `step` | `sum by (step) (x)` | `step` is an output column of the `PROMQL` command | [#164944]({{es-issue}}164944) |
:::

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 42 | `quantile` | `quantile(0.9, x)` | Approximate, using t-digest | Exact, with linear interpolation | - |
| 43 | `limitk`, `limit_ratio` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` | `limitk(10, x)` | A different, equally valid subset | A deterministic pseudo-random subset, with the `promql-experimental-functions` feature flag | - |
:::
