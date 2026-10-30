% Section of promql-limitations.md, also included in other pages. Rows are numbered across all sections.

{{es}} runs some queries that Prometheus rejects.

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 80 | Function names in another case: `RATE(x[5m])` | Runs the function | Error: unknown function |
| 81 | A range vector function over an instant vector: `rate(x)` | Runs the function | Error: expected a range vector |
