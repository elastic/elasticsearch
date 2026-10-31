% Section of promql-limitations.md, also included in other pages. Rows are numbered across all sections.

{applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga`

{{es}} supports Prometheus native histograms through the `exponential_histogram` field type.
`histogram_quantile`, `increase`, and `sum` work on native histograms, and so do `histogram_count`, `histogram_sum`, `histogram_avg`, `histogram_fraction`, and `changes` {applies_to}`stack: ga 9.6+` {applies_to}`serverless: ga`.
For example, `histogram_quantile(0.9, sum by (job) (increase(metric)))` works.

| # | Construct | {{es}} | Prometheus | Issue |
|---|---|---|---|---|
| 60 | `rate` over native histograms | Error. Use `increase` instead, such as `histogram_quantile(0.99, sum by (job) (increase(metric)))` | Evaluates | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 61 | A raw native histogram as the result: `my_histogram`, `increase(my_histogram)` | Error. Wrap it in `histogram_quantile`, `histogram_count`, `histogram_sum`, `histogram_avg`, `histogram_fraction`, or `changes` | Returns the histograms | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 62 | `irate` and `delta` over native histograms | Error | Evaluates | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 63 | Arithmetic on native histograms: `+`, `-`, `*`, `/` | Error | Evaluates | [#152263](https://github.com/elastic/elasticsearch/issues/152263) |
| 64 | `histogram_stddev` and `histogram_stdvar` | Error | Evaluates | [#164152](https://github.com/elastic/elasticsearch/issues/164152) |
