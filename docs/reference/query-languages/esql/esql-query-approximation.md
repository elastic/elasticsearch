---
applies_to:
   stack: preview 9.4+
   serverless: preview
navigation_title: "Approximate STATS queries"
mapped_pages:
 - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-query-approximation.html
---
# Approximate `STATS` queries

{{esql}} [`STATS`](/reference/query-languages/esql/commands/stats-by.md) commands summarize large volumes of data into aggregated statistics. For many analytics workloads, exact results are not strictly necessary — approximate results with known error bounds are sufficient, and can be computed dramatically faster. The `approximation` setting enables this: {{esql}} rewrites your query to use random sampling and extrapolation, returning estimates together with confidence intervals and a certification flag.

Approximation breaks the dependency between performance and dataset size. Accuracy depends principally on the data characteristics and the query itself, not on how many rows are in the source index. This means the performance advantage grows as your data grows.


## Getting started

To approximate a STATS query, prepend `SET approximation=true;` to your existing query:

```esql
SET approximation=true;
FROM web_traffic | WHERE @timestamp >= NOW()-1w
                 | STATS total_hits = COUNT(),
                         avg_load_time = AVG(page_load_ms)
                   BY country_code
                 | SORT total_hits DESC
                 | LIMIT 5
```

No other changes to the query are required. The underlying rewrite — sampling, extrapolation, and confidence interval computation — is handled automatically.


## Understanding the output

An approximate query returns the same columns as the exact query, plus two additional columns for each estimated quantity.

Consider a query that counts page hits and computes the average page load time per country over the last week. The exact results might be:

| total_hits | avg_load_time | country_code | 
|------------|---------------|--------------|
| 12483710   | 237.4         | US           |
| 4271856    | 189.2         | DE           |
| 3804219    | 211.7         | GB           |
| 2156033    | 195.8         | FR           |
| 1847291    | 302.6         | JP           |

The approximate query returns additional columns for each estimated quantity:

| total_hits | avg_load_time | country_code | \_approximation\_confidence\_interval(total_hits) | \_approximation\_certified(total_hits) | \_approximation\_confidence\_interval(avg_load_time) | \_approximation\_certified(avg_load_time) |
|--------------|------------|---------------------------------------------------|---------------------------------------|---------------|------------------------------------------------------|------------------------------------------|
| 12510000   | 237.1         | US           | [12430000, 12590000]                              | true                                  | [235.8, 238.4]                                       | true                                     |
| 4284000    | 189.5         | DE           | [4240000, 4328000]                                | true                                  | [187.9, 191.1]                                       | true                                     |
| 3790000    | 212.3         | GB           | [3752000, 3828000]                                | true                                  | [210.1, 214.5]                                       | true                                     |
| 2162000    | 195.1         | FR           | [2134000, 2190000]                                | true                                  | [192.8, 197.4]                                       | true                                     |
| 1839000    | 303.8         | JP           | [1814000, 1864000]                                | true                                  | [300.2, 307.4]                                       | true                                     |

The additional columns are:

- **`_approximation_confidence_interval(<agg>)`**: The central 90% confidence interval for the estimate. This is an interval that has a high probability (0.9) of containing the true value.
- **`_approximation_certified(<agg>)`**: A boolean indicating whether the statistical properties of the estimate are behaving as expected. When `true`, the confidence interval is trustworthy. When `false`, the estimate may still be accurate, but the distribution of the approximation could not be confirmed to satisfy the assumptions used to compute the interval.


## Configuration options

The default settings work well for most queries, but you can tune the confidence interval computation and sample size to trade off between accuracy and speed.

### Disabling confidence intervals

Computing confidence intervals adds overhead. If you only need point estimates, disable them by setting `confidence_level` to `null`:

```esql
SET approximation={"confidence_level":null};
FROM web_traffic | WHERE @timestamp >= NOW()-1d
                 | STATS total_bytes = SUM(response_bytes),
                         avg_load_time = AVG(page_load_ms)
                   BY datacenter_region
                 | SORT total_bytes DESC
                 | LIMIT 10
```

This skips the interval and certification computation and can yield additional speedup.

### Controlling the sample size

The default sample size is 1,000,000 rows for grouped STATS (queries with a `BY` clause) and 100,000 rows otherwise. If you find results are too imprecise — particularly for high-cardinality grouping — you can increase the sample size:

```esql
SET approximation={"rows":5000000};
FROM web_traffic | WHERE @timestamp >= NOW()-1w
                 | STATS total_hits = COUNT(*),
                         avg_load_time = AVG(page_load_ms)
                   BY url_path
                 | SORT total_hits DESC
                 | LIMIT 25
```

Larger sample sizes improve accuracy at the cost of reduced speedup. As long as the sample size remains well below the total row count, you will still see performance benefits.


## Queries that use index summary statistics

Some aggregations can be computed directly from summary statistics maintained in the index (for example, simple `COUNT(*)` over an indexed numeric field with no grouping). The query planner detects these cases automatically and executes the query exactly, since it is already fast. You do not need to handle this yourself — when this happens, the confidence intervals will have zero length, indicating that the results are exact.


## Supported aggregation functions

Approximation works with aggregation functions where sampling and extrapolation produce statistically sound estimates. The functions that are **not currently supported** and will cause the query to fall back to exact execution include:

- `COUNT_DISTINCT`
- `MIN`
- `MAX`
- `FIRST`
- `LAST`
- `TOP`
- `ABSENT`
- `PRESENT`
- `ST_CENTROID_AGG`
- `ST_EXTENT_AGG`

Some of these (such as `MIN` and `MAX`) are intrinsically difficult to estimate reliably from samples without making strong distributional assumptions. Rather than risk accidental misuse, they are excluded from automatic approximation.


## Unsupported query patterns

The following query patterns are not currently supported for approximation and fall back to exact execution:

- Queries using the `TS` source command
- Queries using the `FORK` or `JOIN` processing commands
- Pipelines containing two or more `STATS` commands


## When approximation is less effective

Approximation works best on large, broad queries. Certain query patterns reduce or eliminate the benefit.

### Highly selective filters

If a query's `WHERE` clause matches only a small fraction of the data, sampling provides little benefit — the data is already small. {{esql}} detects this during the rewrite phase and falls back to exact execution. However, the rewrite step itself adds some overhead, so if you know in advance that your query will match very few rows, it is better to run without approximation.

### High-cardinality grouping

When the `BY` expression has very high cardinality (many distinct values), individual groups may end up with very few sampled rows. This can cause:

- Infrequent groups being dropped entirely (groups with fewer than 10 samples are excluded from results).
- Large estimation errors for groups that are retained.
- If the grouping field is unique per document, the query may return no results at all.

Sorting by ascending count (finding the rarest groups) is particularly problematic, as heavy hitters may require sampling most of the dataset.

If accuracy for high-cardinality queries matters, increase the sample size using the `rows` configuration option. As a rule of thumb, you want at least a few hundred samples per group for reliable estimates.


## Using SAMPLE directly

For expert users who want full control, {{esql}} provides the [`SAMPLE`](/reference/query-languages/esql/commands/sample.md) command. This gives you raw sampled data with no automatic extrapolation or confidence interval computation:

```esql
FROM web_traffic | SAMPLE 0.01
                 | STATS unique_visitors = COUNT_DISTINCT(client_ip)
```

This computes the distinct count of client IPs on roughly 1% of the data. Since `COUNT_DISTINCT` is not supported by automatic approximation, `SAMPLE` is the alternative — but interpreting the result and accounting for sampling bias is your responsibility.

You can also use `SAMPLE` to build custom estimation pipelines, for example by extracting frequency profiles:

```esql
FROM web_traffic | SAMPLE 0.01
                 | STATS c = COUNT(*) BY search_phrase
```

Adjusting the sample probability and observing how results change gives you a sense of convergence.


## Summary

| Aspect | Detail |
|---|---|
| Enable approximation | `SET approximation=true;` |
| Disable confidence intervals | `SET approximation={"confidence_level":null};` |
| Custom sample size | `SET approximation={"rows":N};` |
| Default sample size (grouped) | 1,000,000 rows |
| Default sample size (ungrouped) | 100,000 rows |
| Confidence interval default | Central 90% interval |
| Minimum samples per group | 10 |
