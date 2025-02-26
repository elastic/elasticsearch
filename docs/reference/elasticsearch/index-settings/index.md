# Index settings

$$$index-modules-settings-description$$$
Index level settings can be set per-index. Settings may be:

* _Static_
  They can only be set at index creation time or on a closed index, or by using the [update index settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings) with the `reopen` query parameter set to `true` (which automatically closes and reopens impacted indices).
* _Dynamic_
  They can be changed on a live index using the [update index settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings).

::::{warning}
You can change any documented index settings on closed indices. However, changing undocumented index settings on closed indices is unsupported and might result in errors.
::::

Settings are available for the following modules:

* [History retention](history-retention.md)
  Control the retention of a history of operations in the index.
* [Index](index-modules.md)
  General settings that affect the behavior of indices.
* [Index shard allocation](shard-allocation.md)
  Control where, when, and how shards are allocated to nodes.
* [Indexing pressure](pressure.md)
  Configure indexing back pressure limits.
* [Merge](merge.md)
  Control how shards are merged by the background merge process.
* [Similarities](similarity.md)
  Configure custom similarity settings to customize how search results are scored.
* [Slowlog](slow-log.md)
  Control how slow queries and fetch requests are logged.
* [Store](store.md)
  Configure the type of filesystem used to access shard data.
* [Time series](time-series.md)
  Configure the backing indices in a time series data stream (TSDS).
* [Translog](translog.md)
  Control the transaction log and background flush operations.
