---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/data-tier-shard-filtering.html
navigation_title: Data tier allocation
---

# Data tier allocation settings [data-tier-shard-filtering]

You can use the index-level `_tier_preference` setting to control which [data tier](docs-content://manage-data/lifecycle/data-tiers.md) an index is allocated to.

This setting corresponds to the data node roles:

* [data_content](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#data-content-node)
* [data_hot](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#data-hot-node)
* [data_warm](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#data-warm-node)
* [data_cold](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#data-cold-node)
* [data_frozen](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#data-frozen-node)

::::{note}
The [data](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#data-node-role) role is not a valid data tier and cannot be used with the `_tier_preference` setting. The frozen tier stores [partially mounted indices](docs-content://deploy-manage/tools/snapshot-and-restore/searchable-snapshots.md#partially-mounted) exclusively.
::::



## Data tier allocation settings [data-tier-allocation-filters]

$$$tier-preference-allocation-filter$$$

`index.routing.allocation.include._tier_preference`
:   Assign the index to the first tier in the list that has an available node. This prevents indices from remaining unallocated if no nodes are available in the preferred tier. For example, if you set `index.routing.allocation.include._tier_preference` to `data_warm,data_hot`, the index is allocated to the warm tier if there are nodes with the `data_warm` role. If there are no nodes in the warm tier, but there are nodes with the `data_hot` role, the index is allocated to the hot tier. Used in conjunction with [data tiers](docs-content://manage-data/lifecycle/data-tiers.md#data-tier-allocation).

