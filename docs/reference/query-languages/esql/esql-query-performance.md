---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
navigation_title: "Optimize query performance"
description: "Techniques for writing fast ES|QL queries and operating ES|QL workloads at scale."
type: how-to
---

# Optimize {{esql}} query performance

This guide covers practical techniques for writing fast {{esql}} queries and operating {{esql}} workloads at scale. It starts with common anti-patterns, then shows how to reduce scanned data, reduce returned data, and avoid expensive operations. It also covers tools for monitoring query performance and investigating slow queries across multiple clusters.

::::{tip}
For a quick overview of the most common issues with {{esql}} queries, refer to [Common anti-patterns](#common-anti-patterns).
::::

## Before you begin

This guide assumes familiarity with {{esql}} syntax and command pipelines. To learn the basics, refer to [Get started with {{esql}} queries](/reference/query-languages/esql/esql-getting-started.md).

This guide serves two audiences:

- If you write {{esql}} queries, the following sections cover the techniques most relevant to you:
  - [Common anti-patterns](#common-anti-patterns)
  - [Reduce what you scan](#reduce-what-you-scan)
  - [Reduce what you return](#reduce-what-you-return)
  - [Avoid expensive operations](#avoid-expensive-operations)
- If you administer clusters and need to monitor query performance across multiple clusters, the following section describes the tools available:
  - [Monitor query performance](#monitor-query-performance)

### Check your Elastic Stack version

If you're not on [{{serverless-full}}](docs-content://deploy-manage/deploy/elastic-cloud/serverless.md), check your {{stack}} version. The {{esql}} query engine improves with each release, so upgrading is often one of the highest-impact performance changes you can make.

Some tips on this page require a recent version of the {{stack}}, and individual subsections carry an applicability badge when this is the case. Sections without a version badge apply to all versions where {{esql}} is generally available.

The most important version-specific performance improvements are visible in the table below, including improvements in 9.x for query logging, time series support, query activity, and full-text search functions.

For clusters on a version before 8.17, upgrading provides the largest single performance improvement, because full-text search functions and Lucene pushdowns become available. For clusters on 8.17 but before 8.18, upgrading to 8.18 provides the next largest improvement. That release adds [`LIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-like) and [`RLIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-rlike) pushdown to Lucene and a mapping discovery optimization that reduces overhead on clusters with many indices.

:::{dropdown} Version-specific performance improvements

| Version | What improved | Impact |
| :---- | :---- | :---- |
| 8.13 | [`CIDR_MATCH`](/reference/query-languages/esql/functions-operators/ip-functions/cidr_match.md) pushed to Lucene | Faster IP filtering in security queries |
| 8.16 | Per-aggregation [`WHERE`](/reference/query-languages/esql/commands/where.md) | Replaces slow [`CASE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/case.md)-based conditional aggregation |
| 8.17 | [`MATCH`](/reference/query-languages/esql/functions-operators/search-functions/match.md) and [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md) full-text search functions | Orders of magnitude faster than [`LIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-like) or [`RLIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-rlike) for text search |
| 8.18, 9.0 | [`LIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-like) and [`RLIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-rlike) pushed to Lucene, mapping discovery optimization, [`LOOKUP JOIN`](/reference/query-languages/esql/commands/lookup-join.md) | Faster pattern matching, cheaper queries on clusters with many indices, and native lookup joins |
| 9.1 | [{{esql}} query log](/reference/query-languages/esql/esql-query-log.md), full-text functions GA | Dedicated query performance logging, [`MATCH`](/reference/query-languages/esql/functions-operators/search-functions/match.md), [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md), and [`KQL`](/reference/query-languages/esql/functions-operators/search-functions/kql.md) stable |
| 9.2 | [`INLINE STATS`](/reference/query-languages/esql/commands/inlinestats-by.md), [`TS`](/reference/query-languages/esql/commands/ts.md) command with [`RATE`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/rate.md) and [`TBUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions/tbucket.md) in preview, [`CHANGE_POINT`](/reference/query-languages/esql/commands/change-point.md) GA | Window-function-like queries, native time series support |
| 9.3 | [`INLINE STATS`](/reference/query-languages/esql/commands/inlinestats-by.md) GA, [`TRANGE`](/reference/query-languages/esql/functions-operators/date-time-functions/trange.md), Lucene-pushable [`LOOKUP JOIN`](/reference/query-languages/esql/commands/lookup-join.md) predicates | Faster filtered joins, simpler time range syntax |
| 9.4+ | [`TS`](/reference/query-languages/esql/commands/ts.md) and time series aggregation functions GA, [Query activity](docs-content://deploy-manage/monitor/query-activity.md), [unified query logging](docs-content://deploy-manage/monitor/logging-configuration/query-logs.md) | Native time series support, real-time view of in-flight queries in {{kib}}, single log for all query types |

:::

### Index only what you need

Query performance starts at index time. Your field mappings control what {{esql}} can do efficiently. 

If you only aggregate or sort on a field, and never filter, set [`index: false`](/reference/elasticsearch/mapping-reference/mapping-index.md) to save disk space. {{esql}} can still read the field through doc values. Keep doc values enabled for fields that {{esql}} needs to read, group, sort, or return. For fields that are rarely needed in results, remove them from the query output with [`KEEP`](/reference/query-languages/esql/commands/keep.md) or [`DROP`](/reference/query-languages/esql/commands/drop.md).

### Know your circuit breaker limits

{{esql}} enforces memory limits through [circuit breakers](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#circuit-breakers-page-esql). When a query exceeds the limit, the cluster rejects it to protect node stability. High-cardinality aggregations are the most common trigger. To learn more, refer to [Avoid high-cardinality STATS BY](#avoid-high-cardinality-stats-by).

## Common anti-patterns

These anti-patterns are the most common causes of {{esql}} query latency in production.

:::{tip}
:applies_to: { ech:, serverless: }

[AutoOps](docs-content://deploy-manage/monitor/autoops.md) detects most of these patterns automatically and surfaces actionable recommendations. To browse detected events, refer to [AutoOps events](docs-content://deploy-manage/monitor/autoops/ec-autoops-events.md).
:::

| Pattern | What to look for | Why it's slow |
| :---- | :---- | :---- |
| Broad index pattern | [`FROM *`](/reference/query-languages/esql/commands/from.md) or wide wildcards | Expensive mapping discovery, plus scans across many indices |
| Wide time range | `@timestamp` range spanning weeks or months | Scans proportionally more data |
| Missing [`WHERE`](/reference/query-languages/esql/commands/where.md) | No filter conditions at all | Full index scan |
| Missing [`KEEP`](/reference/query-languages/esql/commands/keep.md) | No column selection | Returns all fields, producing large payloads |
| Missing [`LIMIT`](/reference/query-languages/esql/commands/limit.md) | Unbounded result set | Slow serialization, can trigger deserialization errors in {{kib}} |
| High-cardinality [`STATS BY`](/reference/query-languages/esql/commands/stats-by.md) | Grouping by raw timestamps, full URLs, or document IDs | Produces millions of buckets, can trip circuit breakers |
| [`LIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-like) or [`RLIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-rlike) | Wildcard or regex text matching | Slower than full-text functions for text search, especially pre 8.18/9.0 |
| [`GROK`](/reference/query-languages/esql/commands/grok.md) or [`DISSECT`](/reference/query-languages/esql/commands/dissect.md) | Text parsing on large datasets | CPU-intensive regex or tokenization per row |
| [`CASE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/case.md) | Conditional aggregation through `CASE` | Lazy evaluation, slow |
| [`LOOKUP JOIN`](/reference/query-languages/esql/commands/lookup-join.md) | Join against a large lookup index | Cost is proportional to the lookup index size |

:::{tip}
The most impactful fixes are usually: add a time range filter, add a [`WHERE`](/reference/query-languages/esql/commands/where.md), and add a [`KEEP`](/reference/query-languages/esql/commands/keep.md).
:::

## Reduce what you scan

Most {{esql}} queries spend the bulk of their time reading data from disk. The fastest queries read the least data. This section covers the levers that most directly control scan size.

### Narrow the time range

A tight time range is the single biggest performance lever in most workloads. {{esql}} uses the `@timestamp` field to skip entire shards and segments that fall outside the range, so a narrower window directly reduces the amount of data read.

Avoid running queries that span more time than the result actually needs:

❌ **Don't:** Query without a time bound
```esql
FROM logs-*
| WHERE host.name == "web-01"
| STATS count = COUNT(*) BY log.level
```

✅ **Do:** Add an explicit `@timestamp` filter to bound the scan

```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 day // Bound the scan to a tight window
  AND host.name == "web-01"
| STATS count = COUNT(*) BY log.level
```

In {{kib}}, the time picker automatically applies a time range filter. When writing queries directly in the [{{kib}} Console](docs-content://explore-analyze/query-filter/tools/console.md) or through the API, always include an explicit `@timestamp` filter.

### Filter early with WHERE

A [`WHERE`](/reference/query-languages/esql/commands/where.md) clause earlier in the pipeline reduces the dataset before downstream commands process it. Conditions on indexed fields such as `keyword`, [numeric](/reference/elasticsearch/mapping-reference/number.md), `date`, `ip`, `geo_point`, `geo_shape`, `cartesian_point`, or `cartesian_shape` types are pushed down to Lucene, which skips irrelevant documents entirely.

Without a [`WHERE`](/reference/query-languages/esql/commands/where.md), {{esql}} scans every document in the matched indices:

❌ **Don't:** Filter after the aggregation
```esql
FROM logs-*
| STATS count = COUNT(*) BY host.name, log.level
| WHERE log.level == "error"
```

✅ **Do:** Push the filter up so it runs before the aggregation

```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 day // Filter pushed to Lucene
  AND log.level == "error"
| STATS count = COUNT(*) BY host.name
```

### Restrict the index pattern

A broad [`FROM *`](/reference/query-languages/esql/commands/from.md) forces {{esql}} to discover field mappings across every index and then query each one. On clusters with thousands of indices, that discovery overhead alone can dominate query time. In {{serverless-short}}, `FROM *` can also expand to all linked projects in [cross-project search](/reference/query-languages/esql/esql-cross-serverless-projects.md). Use [`project_routing`](/reference/query-languages/esql/esql-cross-serverless-projects.md#use-project-routing) to limit a cross-project query to the projects that it actually needs.

❌ **Don't:** Use wildcards that match more indices than the query needs
```esql
FROM *
| WHERE @timestamp > NOW() - 1 hour
  AND event.category == "authentication"
| STATS failures = COUNT(*) BY user.name
```

✅ **Do:** Target a specific index pattern instead

```esql
FROM logs-system-*
| WHERE @timestamp > NOW() - 1 hour
  AND event.category == "authentication"
| STATS failures = COUNT(*) BY user.name
```

When a query genuinely needs multiple patterns, list them explicitly with [`FROM`](/reference/query-languages/esql/commands/from.md). For example:

```esql
FROM logs-system-*, logs-auth-*
```

### Use TS for time series data

```{applies_to}
stack: preview 9.2-9.3, ga 9.4+
serverless: ga
```

For time series data streams (TSDS), use [`TS`](/reference/query-languages/esql/commands/ts.md) rather than [`FROM`](/reference/query-languages/esql/commands/from.md). `TS` understands time series structure, including dimensions, metrics, and time ordering, and skips data more efficiently than `FROM` paired with [`WHERE`](/reference/query-languages/esql/commands/where.md). It also unlocks time series functions such as [`RATE`](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions/rate.md) and bucketing through [`TBUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions/tbucket.md).

❌ **Don't:** Query TSDS indices through [`FROM`](/reference/query-languages/esql/commands/from.md) when you intend to aggregate metrics
```esql
FROM metrics-system.cpu-*
| WHERE @timestamp > NOW() - 1 hour
| STATS avg_cpu = AVG(system.cpu.user.pct) BY host.name, bucket = DATE_TRUNC(5 minutes, @timestamp)
```

✅ **Do:** Use [`TS`](/reference/query-languages/esql/commands/ts.md) with [`TBUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions/tbucket.md) for time series metrics

```esql
TS metrics-system.cpu-*
| STATS avg_cpu = AVG(AVG_OVER_TIME(system.cpu.user.pct)) // Inner-then-outer aggregation pattern
        BY host.name, TBUCKET(5 minutes)                  // TBUCKET replaces DATE_TRUNC under TS
```

:::{important}
[`TS`](/reference/query-languages/esql/commands/ts.md) only works on indices created as time series data streams. For non-TSDS indices, continue to use [`FROM`](/reference/query-languages/esql/commands/from.md).
:::

## Reduce what you return

Every column returned has to be read from storage, serialized, and transmitted. Shrinking the result set, by returning fewer columns or rows, often produces significant gains on large indices.

### Select columns with KEEP

[`KEEP`](/reference/query-languages/esql/commands/keep.md) selects which columns to return. [`DROP`](/reference/query-languages/esql/commands/drop.md) does the inverse. Without either, {{esql}} returns every field in every matching document. This is the single biggest source of avoidable overhead on indices with hundreds or thousands of fields.

❌ **Don't:** Return every field by default
```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 hour AND log.level == "error"
| SORT @timestamp DESC
| LIMIT 100
```

✅ **Do:** Project only the fields the consumer actually needs

```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 hour AND log.level == "error"
| KEEP @timestamp, host.name, message, log.level // Return only the four fields needed downstream
| SORT @timestamp DESC
| LIMIT 100
```

:::{tip}
Use wildcards in [`KEEP`](/reference/query-languages/esql/commands/keep.md) sparingly. `host.*` is better than no `KEEP` at all, but `host.name` is better than `host.*` because it avoids pulling in adjacent fields.
:::

When using the REST API on sparse datasets where many columns are `null`, consider setting the [`drop_null_columns`](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query-get#operation-esql-async-query-get-drop_null_columns) query parameter. This removes columns that contain only `null` values from the response, which can significantly reduce serialization overhead.

### Cap rows with LIMIT

Always include a [`LIMIT`](/reference/query-languages/esql/commands/limit.md) on queries that return raw rows. {{esql}} appends a default limit of 1000 rows to every query. Reducing it with an explicit `LIMIT` is one of the simplest ways to speed up a query. Increasing it beyond the default makes serialization slower and can trigger deserialization errors in {{kib}}. The maximum configurable limit is 10,000 rows.

❌ **Don't:** Leave the result set unbounded
```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 day AND log.level == "error"
| SORT @timestamp DESC
```

✅ **Do:** Cap the result to the rows the consumer actually needs

```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 day AND log.level == "error"
| SORT @timestamp DESC
| LIMIT 100
```

## Avoid expensive operations

Some {{esql}} operations are intrinsically more expensive than their alternatives. Knowing the cheaper substitute, and when it applies, often replaces a slow query with a fast one. The subsections below are ordered roughly by impact, with the highest-leverage changes first.

### Use full-text search instead of LIKE or RLIKE

For text search, prefer [`MATCH`](/reference/query-languages/esql/functions-operators/search-functions/match.md), [`MATCH_PHRASE`](/reference/query-languages/esql/functions-operators/search-functions/match_phrase.md), [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md), or [`KQL`](/reference/query-languages/esql/functions-operators/search-functions/kql.md) over [`LIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-like) or [`RLIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-rlike). The full-text search functions use the inverted index and are optimized for analyzed text. `LIKE` and `RLIKE` are pattern-matching operators. Pre 8.18/9.0 they are especially costly because they are not pushed down to Lucene. Leading wildcards (for example `*something`) are particularly expensive because they cannot use the inverted index efficiently.

❌ **Don't:** Use pattern matching on free text with [`LIKE`](/reference/query-languages/esql/functions-operators/operators.md#esql-like)
```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 hour
  AND message LIKE "*connection refused*"
```

✅ **Do:** Use [`MATCH_PHRASE`](/reference/query-languages/esql/functions-operators/search-functions/match_phrase.md) against the inverted index

```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 hour
  AND MATCH_PHRASE(message, "connection refused") // Inverted index lookup, not a row scan
```

[`MATCH`](/reference/query-languages/esql/functions-operators/search-functions/match.md) works on `text` and `keyword` fields. Use [`MATCH_PHRASE`](/reference/query-languages/esql/functions-operators/search-functions/match_phrase.md) when the words must appear together in order. For Lucene query syntax with `field:value` and boolean operators, use [`QSTR`](/reference/query-languages/esql/functions-operators/search-functions/qstr.md). For {{kib}} Query Language syntax, use [`KQL`](/reference/query-languages/esql/functions-operators/search-functions/kql.md).

{applies_to}`stack: preview 9.5` {applies_to}`serverless: preview`
`MATCH` can also target expressions that are not backed by an index, such as
columns produced by [`EVAL`](/reference/query-languages/esql/commands/eval.md) or [`STATS`](/reference/query-languages/esql/commands/stats-by.md).
When the target is not an indexed field, `MATCH` evaluates by scanning values
row by row instead of using the inverted index, which is slower on large
datasets. For best performance, prefer searching indexed fields when possible.

:::{tip}
To learn more about using {{esql}} for search use cases, refer to [{{esql}} for search](docs-content://solutions/search/esql-for-search.md).
:::

### Avoid high-cardinality STATS BY

Each unique combination of `BY` values creates a bucket in memory. Grouping by high-cardinality fields such as raw timestamps, full URLs, or document IDs, or by many fields at once, can produce millions of buckets.

:::{warning}
High-cardinality groupings can exhaust memory and trip circuit breakers. Always bucket timestamps and choose the lowest-cardinality representation of a field that still answers the question.
:::

❌ **Don't:** Group by raw, high-cardinality fields
```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 day
| STATS count = COUNT(*) BY url.full, user.name, @timestamp
```

✅ **Do:** Reduce cardinality before grouping

```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 day
| STATS count = COUNT(*) BY url.path, user.name, bucket = DATE_TRUNC(1 hour, @timestamp) // Uses `url.path` instead of `url.full`, bucketed timestamps instead of raw
```

Common reductions include: bucketing timestamps with [`DATE_TRUNC`](/reference/query-languages/esql/functions-operators/date-time-functions/date_trunc.md) or [`BUCKET`](/reference/query-languages/esql/functions-operators/grouping-functions/bucket.md), using `url.path` instead of `url.full`, and filtering to a known subset before the [`STATS`](/reference/query-languages/esql/commands/stats-by.md).

### Prefer fields backed by doc values

{{esql}} reads field values through a block-loading system that strongly prefers [doc values](/reference/elasticsearch/mapping-reference/doc-values.md). Fields with doc values, such as `keyword`, [numeric](/reference/elasticsearch/mapping-reference/number.md), `date`, `ip`, `geo_point`, `geo_shape`, `cartesian_point`, and `cartesian_shape` types, are read in fast columnar batches. Fields without doc values, such as `text` and `match_only_text`, fall back to reading [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md), which requires decompressing and parsing the full JSON document per row. This applies to any operation that reads the field value, including filtering, grouping, sorting, and returning fields through [`KEEP`](/reference/query-languages/esql/commands/keep.md).

If an exact `.keyword` subfield exists, the query planner automatically rewrites expressions to use it, so `message` and `message.keyword` perform the same in that case. However, if the `text` field has no `keyword` subfield, or if the subfield is not exact (for example, it uses `ignore_above`), the planner cannot rewrite and falls back to reading from `_source`, which is significantly slower. When no exact subfield is available, filter aggressively to limit the number of documents that require `_source` reads.

❌ **Don't:** Group by an analyzed field
```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 hour
| STATS count = COUNT(*) BY message
```

✅ **Do:** Use the `.keyword` subfield

```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 hour
| STATS count = COUNT(*) BY message.keyword
```

For free-text grouping, [`CATEGORIZE`](/reference/query-languages/esql/functions-operators/grouping-functions/categorize.md) {applies_to}`stack: preview 9.0, ga 9.1+` groups similar messages automatically:

```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 hour
| STATS count = COUNT(*) BY category = CATEGORIZE(message)
| SORT count DESC
| LIMIT 20
```

### Return spatial fields only when you need source precision

The spatial types `geo_point`, `geo_shape`, `cartesian_point`, and `cartesian_shape` are maintained at source precision in the original documents, but indexed at reduced precision by Lucene for performance. Reading spatial values from doc values is fast and usually precise enough. Returning the original spatial field preserves source precision, but requires reading from `_source`, which is slower. To prioritize performance, drop original spatial fields from the result unless the query consumer needs the exact original value.

To learn more, refer to [Spatial precision](/reference/query-languages/esql/limitations.md#esql-limitations-spatial-precision).

### Use per-aggregation WHERE instead of CASE

For conditional aggregations, attach a [`WHERE`](/reference/query-languages/esql/commands/where.md) clause directly to each [`STATS`](/reference/query-languages/esql/commands/stats-by.md) expression rather than wrapping values in [`CASE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/case.md). `CASE` is lazy-evaluated and slow for this pattern.

❌ **Don't:** Emulate conditional aggregations through [`CASE`](/reference/query-languages/esql/functions-operators/conditional-functions-and-expressions/case.md) and [`SUM`](/reference/query-languages/esql/functions-operators/aggregation-functions/sum.md)
```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 day
| EVAL is_error = CASE(log.level == "error", 1, 0)
| EVAL is_warn = CASE(log.level == "warning", 1, 0)
| STATS
    total = COUNT(*),
    errors = SUM(is_error),
    warnings = SUM(is_warn)
  BY service.name
```

✅ **Do:** Compute each conditional metric directly with a per-aggregation [`WHERE`](/reference/query-languages/esql/commands/where.md)

```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 day
| STATS
    total = COUNT(*),
    errors = COUNT(*) WHERE log.level == "error",     // Per-aggregation filter, no CASE needed
    warnings = COUNT(*) WHERE log.level == "warning"  // One filter per metric
  BY service.name
```

### Prefer DISSECT over GROK

[`GROK`](/reference/query-languages/esql/commands/grok.md) uses regular expressions, which are CPU-intensive per row. [`DISSECT`](/reference/query-languages/esql/commands/dissect.md) uses delimiter-based tokenization and is much cheaper. When the log format uses consistent delimiters, prefer `DISSECT`. When you must use `GROK`, filter aggressively first to shrink the dataset.

❌ **Don't:** Use regex parsing when a delimiter is available
```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 hour
| GROK message "%{TIMESTAMP_ISO8601:ts} %{LOGLEVEL:level} %{GREEDYDATA:msg}"
```

✅ **Do:** Use [`DISSECT`](/reference/query-languages/esql/commands/dissect.md) for delimiter-based formats

```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 hour
| DISSECT message "%{ts} %{level} %{msg}" // Delimiter tokenization, no regex engine
```

### Filter before LOOKUP JOIN

[`LOOKUP JOIN`](/reference/query-languages/esql/commands/lookup-join.md) combines each incoming row with matching rows from a lookup index. Joining fewer incoming rows is usually faster, and large lookup matches can increase memory pressure.

Filter the source data before joining, and keep the lookup index as small and purpose-built as possible:

```esql
FROM logs-*
| WHERE @timestamp > NOW() - 1 hour
  AND event.category == "network"
| LOOKUP JOIN threat_list ON source.ip
| KEEP @timestamp, source.ip, threat_list.risk, event.action
```

{{esql}} tries to push filters before the join when possible. Write the query with the selective filters before `LOOKUP JOIN` so the intended execution order is clear and the join receives the smallest practical input.

### Use approximate aggregations where possible

```{applies_to}
stack: preview 9.4
serverless: preview
```

For large [`STATS`](/reference/query-languages/esql/commands/stats-by.md) queries, exact results can be expensive. If approximate results are acceptable, [approximate `STATS` queries](/reference/query-languages/esql/esql-query-approximation.md) can trade exactness for much faster execution on large datasets.

Approximation is useful for exploratory analysis, dashboard panels, and high-cardinality aggregations where a close estimate is enough. Use exact aggregations when the result feeds billing, compliance, alerting, or other workflows that require precise values.

## Monitor query performance

Once a query is written, several tools help confirm whether it is actually fast and identify regressions over time. When reviewing query logs, scan for [common anti-patterns](#common-anti-patterns) first.

- [**Inspect panel**](#inspect-panel-in-kib): Check a query from Discover or a dashboard.
- [**Query activity**](#query-activity): Find and cancel in-flight queries.
- [**Profile API responses**](#profile-api-responses): Inspect how a query executed through the API.
- [**Query logging**](#query-logging): Analyze historical slow queries.
- [**Task management API**](#task-management-api): Monitor or cancel {{esql}} tasks from the API.

### Inspect panel in {{kib}}

In [Discover](docs-content://explore-analyze/discover.md) or within a [dashboard](docs-content://explore-analyze/dashboards.md), select **Inspect** to see the {{esql}} query sent to the cluster and the `took` value, which is the server-side execution time in milliseconds. This helps clarify if the root cause is the query itself, the network, or {{kib}}'s rendering.

### Query activity

```{applies_to}
stack: preview 9.4
serverless: preview
```

The [**Query activity**](docs-content://deploy-manage/monitor/query-activity.md) page in {{kib}} provides a real-time view of all in-flight search work in your cluster, including {{esql}}, Query DSL, EQL, and SQL queries. Use it to find long-running queries, trace them back to their source in {{kib}}, and cancel them when needed.

### Profile API responses

When running an {{esql}} query through the [ES|QL query API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-query), set the `profile` body parameter to `true` to include a `profile` object in the response. The profile output is intended for human debugging and can help identify which parts of a query contribute to its runtime. The response format can change at any time, so use it for investigation rather than automation.

### Query logging

```{applies_to}
stack: preview 9.4
serverless: unavailable
```

Query logging captures Query DSL, EQL, KQL, and {{esql}} queries that exceed configurable duration thresholds and stores them in a managed data stream for analysis. This is the recommended way to log slow queries. To configure it, refer to [Query logging](docs-content://deploy-manage/monitor/logging-configuration/query-logs.md).

For clusters on earlier versions, a legacy {{esql}}-specific query log {applies_to}`stack: ga 9.1+` writes slow queries to a `_esql_querylog.json` file in the {{es}} log directory. To configure it, refer to [{{esql}} query log](/reference/query-languages/esql/esql-query-log.md).

### Task management API

The [task management API](/reference/query-languages/esql/esql-task-management.md) lets you monitor and cancel long-running {{esql}} queries.

List running {{esql}} tasks:

```console
GET _tasks?actions=*esql*&detailed
```

Cancel a specific task:

```console
POST _tasks/<task_id>/_cancel
```


## Related pages

- Inspect query logs:
  - (recommended) [Query logging](docs-content://deploy-manage/monitor/logging-configuration/query-logs.md) {applies_to}`stack: preview 9.4`: Log all query types through a managed data stream
  - [{{esql}} query log](/reference/query-languages/esql/esql-query-log.md) {applies_to}`stack: ga 9.1+`: Log {{esql}} queries to a file on each node
- [Circuit breaker settings](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#circuit-breakers-page-esql): Configure {{esql}} memory limits and troubleshoot circuit breaker errors
- [{{esql}} task management](/reference/query-languages/esql/esql-task-management.md): Monitor and cancel long-running queries
- [Approximate STATS queries](/reference/query-languages/esql/esql-query-approximation.md) {applies_to}`stack: preview 9.4+`: Trade exact results for faster aggregations on large datasets
- [Filing a support case](/reference/query-languages/esql/esql-troubleshooting.md#filing-a-support-case): Learn what to include when reporting a slow or failing query
- [Explicit mapping](docs-content://manage-data/data-store/mapping/explicit-mapping.md): Control which fields are indexed and how
- [{{esql}} for search](docs-content://solutions/search/esql-for-search.md): Use {{esql}} for full-text search, vector search, and AI-powered retrieval
- [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md): Learn how columnar storage enables fast sorting, aggregations, and field reads
- [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md): Learn how the original JSON document is stored with each record
