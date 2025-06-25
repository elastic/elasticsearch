---
applies_to:
  stack: all
---
# Index settings

:::{include} _snippets/serverless-availability.md
:::

$$$index-modules-settings-description$$$
{{es}} organizes index-level settings into index modules, each controlling a specific aspect of index behavior.

These settings are configured on a per-index basis and may be:

* _Static_
  They can only be set at index creation time or on a closed index, or by using the [update index settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings) with the `reopen` query parameter set to `true` (which automatically closes and reopens impacted indices).
* _Dynamic_
  They can be changed on a live index using the [update index settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings).

::::{warning}
You can change any documented index settings on closed indices. However, changing undocumented index settings on closed indices is unsupported and might result in errors.
::::

Settings are available for the following modules:

* [General](index-modules.md):
  Index settings not tied to a specific module.
* [Index shard allocation](shard-allocation.md):
  Control where, when, and how shards are allocated to nodes.
* [History retention](history-retention.md)
  Control how long the history of operations is retained in the index.
* [Index blocks](./index-block.md):
  Block different type of operations to the indices.
* [Mapping limits](./mapping-limit.md):
  Limit the number of field mappings.
* [Merge](merge.md):
  Control how shards are merged by the background merge process.
* [Similarities](similarity.md):
  Configure custom similarity settings to customize how search results are scored.
* [Slowlog](slow-log.md):
  Control how slow queries and fetch requests are logged.
* [Sorting](./sorting.md):
  Configure how to sort the segments inside each shard.
* [Store](store.md):
  Configure the type of filesystem used to access shard data.
* [Time series](time-series.md)
  Configure the backing indices in a time series data stream (TSDS).
* [Translog](translog.md)
  Control the transaction log and background flush operations.
* [Indexing pressure](pressure.md)
  Configure indexing back pressure limits.

There are also index settings associated with [text analysis](docs-content://manage-data/data-store/text-analysis.md), which define analyzers, tokenizers, token filters, and character filters.
