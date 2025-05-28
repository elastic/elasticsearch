---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-metric-agg-combine-context.html
products:
  - id: painless
---

# Metric aggregation combine context [painless-metric-agg-combine-context]

Use a Painless script to [combine](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) values for use in a scripted metric aggregation. A combine script is run once per shard following a [map script](/reference/scripting-languages/painless/painless-metric-agg-map-context.md) and is optional as part of a full metric aggregation.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`state` (`Map`)
:   `Map` with values available from the prior map script.

**Return**

`List`, `Map`, `String`, or primitive
:   A value collected for use in a [reduce script](/reference/scripting-languages/painless/painless-metric-agg-reduce-context.md). If no reduce script is specified, the value is used as part of the result.

**API**

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

