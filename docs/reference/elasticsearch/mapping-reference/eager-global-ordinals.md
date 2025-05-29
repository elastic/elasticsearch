---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/eager-global-ordinals.html
---

# eager_global_ordinals [eager-global-ordinals]

## What are global ordinals? [_what_are_global_ordinals]

To support aggregations and other operations that require looking up field values on a per-document basis, Elasticsearch uses a data structure called [doc values](/reference/elasticsearch/mapping-reference/doc-values.md). Term-based field types such as `keyword` store their doc values using an ordinal mapping for a more compact representation. This mapping works by assigning each term an incremental integer or *ordinal* based on its lexicographic order. The field’s doc values store only the ordinals for each document instead of the original terms, with a separate lookup structure to convert between ordinals and terms.

When used during aggregations, ordinals can greatly improve performance. As an example, the `terms` aggregation relies only on ordinals to collect documents into buckets at the shard-level, then converts the ordinals back to their original term values when combining results across shards.

Each index segment defines its own ordinal mapping, but aggregations collect data across an entire shard. So to be able to use ordinals for shard-level operations like aggregations, Elasticsearch creates a unified mapping called *global ordinals*. The global ordinal mapping is built on top of segment ordinals, and works by maintaining a map from global ordinal to the local ordinal for each segment.

Global ordinals are used if a search contains any of the following components:

* Certain bucket aggregations on `keyword`, `ip`, and `flattened` fields. This includes `terms` aggregations as mentioned above, as well as `composite`, `diversified_sampler`, and `significant_terms`.
* Bucket aggregations on `text` fields that require [`fielddata`](/reference/elasticsearch/mapping-reference/text.md#fielddata-mapping-param) to be enabled.
* Operations on parent and child documents from a `join` field, including `has_child` queries and `parent` aggregations.

::::{note}
The global ordinal mapping uses heap memory as part of the [field data cache](/reference/elasticsearch/configuration-reference/field-data-cache-settings.md). Aggregations on high cardinality fields can use a lot of memory and trigger the [field data circuit breaker](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md#fielddata-circuit-breaker).
::::



## Loading global ordinals [_loading_global_ordinals]

The global ordinal mapping must be built before ordinals can be used during a search. By default, the mapping is loaded during search on the first time that global ordinals are needed. This is the right approach if you are optimizing for indexing speed, but if search performance is a priority, it’s recommended to eagerly load global ordinals on fields that will be used in aggregations:

```console
PUT my-index-000001/_mapping
{
  "properties": {
    "tags": {
      "type": "keyword",
      "eager_global_ordinals": true
    }
  }
}
```
% TEST[s/^/PUT my-index-000001\n/]

When `eager_global_ordinals` is enabled, global ordinals are built when a shard is [refreshed](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-refresh) — Elasticsearch always loads them before exposing changes to the content of the index. This shifts the cost of building global ordinals from search to index-time. Elasticsearch will also eagerly build global ordinals when creating a new copy of a shard, as can occur when increasing the number of replicas or relocating a shard onto a new node.

Eager loading can be disabled at any time by updating the `eager_global_ordinals` setting:

```console
PUT my-index-000001/_mapping
{
  "properties": {
    "tags": {
      "type": "keyword",
      "eager_global_ordinals": false
    }
  }
}
```
% TEST[continued]


## Avoiding global ordinal loading [_avoiding_global_ordinal_loading]

Usually, global ordinals do not present a large overhead in terms of their loading time and memory usage. However, loading global ordinals can be expensive on indices with large shards, or if the fields contain a large number of unique term values. Because global ordinals provide a unified mapping for all segments on the shard, they also need to be rebuilt entirely when a new segment becomes visible.

In some cases it is possible to avoid global ordinal loading altogether:

* The `terms`, `sampler`, and `significant_terms` aggregations support a parameter [`execution_hint`](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md#search-aggregations-bucket-terms-aggregation-execution-hint) that helps control how buckets are collected. It defaults to `global_ordinals`, but can be set to `map` to instead use the term values directly.
* If a shard has been [force-merged](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-forcemerge) down to a single segment, then its segment ordinals are already *global* to the shard. In this case, Elasticsearch does not need to build a global ordinal mapping and there is no additional overhead from using global ordinals. Note that for performance reasons you should only force-merge an index to which you will never write to again.


