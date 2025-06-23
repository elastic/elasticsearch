---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/allocation-total-shards.html
---

# Total shards per node [allocation-total-shards]

The cluster-level shard allocator tries to spread the shards of a single index across as many nodes as possible. However, depending on how many shards and indices you have, and how big they are, it may not always be possible to spread shards evenly.

The following *dynamic* setting allows you to specify a hard limit on the total number of shards from a single index allowed per node:

$$$total-shards-per-node$$$

`index.routing.allocation.total_shards_per_node`
:   The maximum number of shards (replicas and primaries) that will be allocated to a single node. Defaults to unbounded.

You can also limit the amount of shards a node can have regardless of the index:

$$$cluster-total-shards-per-node$$$

`cluster.routing.allocation.total_shards_per_node`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Maximum number of primary and replica shards allocated to each node. Defaults to `-1` (unlimited).

{{es}} checks this setting during shard allocation. For example, a cluster has a `cluster.routing.allocation.total_shards_per_node` setting of `100` and three nodes with the following shard allocations:

* Node A: 100 shards
* Node B: 98 shards
* Node C: 1 shard

If node C fails, {{es}} reallocates its shard to node B. Reallocating the shard to node A would exceed node A’s shard limit.


::::{warning}
These settings impose a hard limit which can result in some shards not being allocated.

Use with caution.

::::


