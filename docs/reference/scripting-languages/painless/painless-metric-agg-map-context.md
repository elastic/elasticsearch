---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-metric-agg-map-context.html
---

# Metric aggregation map context [painless-metric-agg-map-context]

Use a Painless script to [map](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md) values for use in a scripted metric aggregation. A map script is run once per collected document following an optional [initialization script](/reference/scripting-languages/painless/painless-metric-agg-init-context.md) and is required as part of a full metric aggregation.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`state` (`Map`)
:   `Map` used to add values for processing in a [combine script](./painless-metric-agg-combine-context.md) or to be returned from the aggregation.

`doc` (`Map`, read-only)
:   Contains the fields of the current document where each field is a `List` of values.

`_score` (`double` read-only)
:   The similarity score of the current document.

**Side Effects**

`state` (`Map`)
:   Use this `Map` to add values for processing in a combine script. Additional values must be of the type `Map`, `List`, `String` or primitive. The same `state` `Map` is shared between all aggregated documents on a given shard. If an initialization script is provided as part of the aggregation then values added from the initialization script are available. If no combine script is specified, values must be directly stored in `state` in a usable form. If no combine script and no [reduce script](/reference/scripting-languages/painless/painless-metric-agg-reduce-context.md) are specified, the `state` values are used as the result.

**Return**

`void`
:   No expected return value.

**API**

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

