% Section of prometheus-compatibility.md, also included in the PROMQL command page. The command page is copied
% verbatim into the context of agents: Kibana's ES|QL generation docs (esql_docs/esql-promql.txt) and the
% elasticsearch-esql agent skill (references/promql-command.md).
% Headings follow https://prometheus.io/docs/prometheus/latest/querying/basics/.
% Rows are numbered across all snippets in _snippets/compatibility/, in page order. A row without a version badge
% applies to every version. "Not supported yet" columns: # | Construct | Example | Workaround | Issue.
% "Behaves differently" columns: # | Construct | Example | Elasticsearch | Prometheus | Issue.

Sections follow [Querying basics](https://prometheus.io/docs/prometheus/latest/querying/basics/) in the {{prom}} documentation.

### Histograms [prometheus-compatibility-histograms]

In the examples on this page, `h` is a native histogram metric.

**Supported**

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` Native histograms, stored as `exponential_histogram` fields.
`histogram_quantile`, `histogram_count`, `histogram_sum`, `histogram_avg`, `increase`, and `sum` support them, such as `histogram_quantile(0.9, sum by (job) (increase(h[5m])))`.

{applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` `histogram_fraction` and `changes` support them too.

Classic histograms, stored as `_bucket` series with an `le` label, such as `histogram_quantile(0.9, sum by (le) (rate(x_bucket[5m])))`.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 1 | Native histogram as the result {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` | `h`, `increase(h[5m])` | Wrap it in a histogram function, such as `histogram_count` | [#152263]({{es-issue}}152263) |
:::

### Range vector [prometheus-compatibility-range-vector]

**Supported**

Range vectors as function arguments, such as `rate(x[5m])`.
In the `PROMQL` command, the range can be left out, such as `rate(x)`.
Refer to [Implicit range selectors](/reference/query-languages/esql/commands/promql.md#esql-promql-implicit-range-selectors).

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 2 | Range vector as the result of an instant query | `x[5m]` | - | [#164220]({{es-issue}}164220) |
:::

### Literals [prometheus-compatibility-literals]

**Supported**

String literals in single quotes, double quotes, or backticks.
Float literals, including scientific notation, hexadecimal, `Inf`, and `NaN`, such as `1e3` and `0x1f`.
Time durations, such as `1h30m`, and duration arithmetic, such as `x[5m + 1m]`.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 3 | `step()`, `range()`, `min_of()`, and `max_of()` in a duration ({{prom}}: `promql-duration-expr` feature flag) | `x[step()]`, `x[min_of(5m, step())]` | Use a fixed duration | [#164221]({{es-issue}}164221) |
:::

### Time series selectors [prometheus-compatibility-time-series-selectors]

#### Instant vector selectors [prometheus-compatibility-instant-vector-selectors]

**Supported**

A metric name with the label matchers `=`, `!=`, `=~`, and `!~`, such as `http_requests_total{job="api", code=~"5.."}`.

{applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` The regex of `=~` and `!~` in RE2 syntax, as in {{prom}}, such as `{code=~"\\d{3}"}` and `{path=~"(?i)/api/.*"}`. While the cluster, or a remote cluster the query reads, has nodes on an earlier version, a matcher that still needs a regex after rewriting and that Lucene regexp syntax reads differently, such as `(?i)/api/.*`, is rejected, even when every queried shard is on an upgraded node. Requests coordinated by a node on an earlier version still use Lucene regexp syntax.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 4 | Selector without a metric name | `{job="api"}` | - | [#146977]({{es-issue}}146977) |
| 5 | Regex matcher on `__name__` | `{__name__=~"node_.*"}` | Combine the metrics with `or`: `node_cpu or node_mem` | [#146977]({{es-issue}}146977) |
| 6 | In a regex matcher, a capture group name used twice, a long or lowercase Unicode class name, or a `{` that starts no count followed by a repetition {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `{l=~"(?P<n>a)(?P<n>b)"}`, `{l=~"\\p{Letter}"}`, `{l=~"x{*"}` | Use distinct group names, short class names such as `\\p{L}`, and `\\{` | - |
:::

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 7 | Negative matcher on `__name__` | `{__name__!="up", job="node"}` | No results | Every other matching metric | [#146977]({{es-issue}}146977) |
| 8 | Regex matcher syntax {applies_to}`stack: preview =9.4, ga 9.5-9.6` | `{path=~"a~b"}`, `{v=~"<1-5>"}` | Lucene regexp syntax, where `~`, `&`, `@`, `"` and `<1-5>` are operators | RE2 syntax, where they are literals | - |
| 9 | Unicode classes in a regex matcher {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `{l=~"\\pL+"}` | The classes of Unicode 6.0 | The classes of Unicode 15.0, which hold characters added since | - |
| 10 | A case-insensitive group then an alternative that starts with the same letters {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `{l=~"(?i:a)b\|ac"}` | Doesn't match `AB` | Matches `AB` | - |
| 11 | A backslash before a non-ASCII character in a regex matcher {applies_to}`stack: ga 9.7+` {applies_to}`serverless: ga` | `{l=~"\\é"}` | Matches `é` | Rejected | - |
:::

#### Range vector selectors [prometheus-compatibility-range-vector-selectors]

**Supported**

A range selector with a duration, such as `x[5m]` or `x[1h30m]`.

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 12 | `anchored` and `smoothed` modifiers ({{prom}}: `promql-extended-range-selectors` feature flag) | `increase(x[5m] anchored)` | - | [#164222]({{es-issue}}164222) |
:::

#### Offset modifier [prometheus-compatibility-offset-modifier]

**Supported**

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga` `offset` on instant and range selectors, such as `x offset 5m` and `rate(x[5m] offset 1h)`.

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 13 | Negative offset at the end of the query range | `x offset -5m` | No value at the last steps. Samples after the query end aren't read | Reads samples after the query end | - |
:::

#### @ modifier [prometheus-compatibility-at-modifier]

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 14 | `@` modifier | `x @ 1700000000`, `x @ end()` | - | [#164139]({{es-issue}}164139) |
:::

### Subquery [prometheus-compatibility-subquery]

**Not supported yet**

:::{table}
:widths: 1-4-3-3-1

| # | Construct | Example | Workaround | Issue |
|---|---|---|---|---|
| 15 | Subquery | `max_over_time(rate(x[5m])[1h:1m])` | - | [#164139]({{es-issue}}164139) |
:::

### Gotchas [prometheus-compatibility-gotchas]

#### Staleness [prometheus-compatibility-staleness]

**Supported**

A selector without a range, such as `http_requests_total`, returns the latest sample of each series within the lookback window at each evaluation time.

**Behaves differently**

:::{table}
:widths: 1-3-2-3-2-1

| # | Construct | Example | {{es}} | {{prom}} | Issue |
|---|---|---|---|---|---|
| 16 | Lookback in instant queries | `/api/v1/query` | Five minutes, fixed | Five minutes by default, configurable with `lookback_delta` | [#149939]({{es-issue}}149939) |
| 17 | Lookback in range queries | `/api/v1/query_range` | One `step`. A series disappears at the first step without its own sample | Five minutes. The last sample repeats until then | - |
| 18 | Staleness markers | - | Ignored. A series disappears once its samples leave the lookback window | A series disappears at its staleness marker | - |
:::
