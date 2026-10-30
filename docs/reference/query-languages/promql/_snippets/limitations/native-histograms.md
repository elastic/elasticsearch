% Section of promql-limitations.md, also included in other pages. Rows are numbered across all sections.

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga`

{{es}} supports Prometheus native histograms through the `exponential_histogram` field type.
`histogram_quantile`, `increase`, and `sum` work on native histograms, and so do `histogram_count`, `histogram_sum`, `histogram_avg`, `histogram_fraction`, and `changes` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga`.
For example, `histogram_quantile(0.9, sum by (job) (increase(metric)))` works.

| # | Construct | {{es}} | Prometheus |
|---|---|---|---|
| 82 | `rate` over native histograms | Error. Use `increase` instead, such as `histogram_quantile(0.99, sum by (job) (increase(metric)))` | Evaluates |
| 83 | A raw native histogram as the result: `my_histogram`, `increase(my_histogram)` | Error. Wrap it in `histogram_quantile`, `histogram_count`, `histogram_sum`, `histogram_avg`, `histogram_fraction`, or `changes` | Returns the histograms |
| 84 | `irate` and `delta` over native histograms | Error | Evaluates |
| 85 | Arithmetic on native histograms: `+`, `-`, `*`, `/` | Error | Evaluates |
| 86 | `histogram_stddev` and `histogram_stdvar` | Error | Evaluates |
