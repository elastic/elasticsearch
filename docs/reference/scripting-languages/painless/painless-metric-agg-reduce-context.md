---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-metric-agg-reduce-context.html
products:
  - id: painless
---

# Metric aggregation reduce context [painless-metric-agg-reduce-context]

Use a Painless script to [reduce](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) values to produce the result of a scripted metric aggregation. A reduce script is run once on the coordinating node following a [combine script](/reference/scripting-languages/painless/painless-metric-agg-combine-context.md) (or a [map script](/reference/scripting-languages/painless/painless-metric-agg-map-context.md) if no combine script is specified) and is optional as part of a full metric aggregation.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`states` (`Map`)
:   `Map` with values available from the prior combine script (or a map script if no combine script is specified).

**Return**

`List`, `Map`, `String`, or primitive
:   A value used as the result.

**API**

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

