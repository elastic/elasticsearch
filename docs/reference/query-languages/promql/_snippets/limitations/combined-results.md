% Section of promql-limitations.md, also included in other pages. Rows are numbered across all sections.

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 61 | `or` between two metrics whose series have the same labels apart from the name: `a or b` | Returns the series of both metrics | Returns the series of `a` only |
| 62 | `vector()` as an operand: `x + vector(2)`, `vector(2) * x` | Applies the value to every series, like a number | No results: the `vector()` series has no labels to match |
| 63 | `sum(vector(1))` | The number of series in the queried data streams | `1` |
| 64 | {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga` A `by(...)` aggregation over a derived label: `sum by (dst) (label_replace(up, "dst", "$1", "instance", "(.*):.*"))` | Can undercount series that end up with the same derived label | Sums all series |
| 65 | {applies_to}`stack: ga =9.5` Arithmetic between two different metrics that aren't aggregated: `a / b` | No results | Matches the series of both metrics on their labels |
| 66 | {applies_to}`stack: ga =9.5` `or` with a `vector()` operand: `x or vector(0)` | Error for instant queries; leaves out series for range queries | Evaluates |
