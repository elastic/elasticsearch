---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-metric-agg-init-context.html
---

# Metric aggregation initialization context [painless-metric-agg-init-context]

Use a Painless script to [initialize](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) values for use in a scripted metric aggregation. An initialization script is run prior to document collection once per shard and is optional as part of the full metric aggregation.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`state` (`Map`)
:   Empty `Map` used to add values for use in a [map script](/reference/scripting-languages/painless/painless-metric-agg-map-context.md).

**Side Effects**

`state` (`Map`)
:   Add values to this `Map` to for use in a map. Additional values must be of the type `Map`, `List`, `String` or primitive.

**Return**

`void`
:   No expected return value.

**API**

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

