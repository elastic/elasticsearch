---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/allocation-total-shards.html
applies_to:
  stack: all
---

# Total shards per node [allocation-total-shards]

:::{include} _snippets/serverless-availability.md
:::

The cluster-level shard allocator tries to spread the shards of a single index across as many nodes as possible. However, depending on how many shards and indices you have, and how big they are, it may not always be possible to spread shards evenly.

The following *dynamic* setting allows you to specify a hard limit on the total number of shards from a single index allowed per node:

$$$total-shards-per-node$$$

`index.routing.allocation.total_shards_per_node`
:   The maximum number of shards (replicas and primaries) that will be allocated to a single node. Defaults to unbounded.

% Temporary anchor: docs-content links to it from troubleshoot/elasticsearch/increase-cluster-shard-limit.md.
% Remove it after that link points to configuration-reference/cluster-level-shard-allocation-routing-settings.md.
$$$cluster-total-shards-per-node$$$

::::{note}
To also limit the number of shards a node can have regardless of the index, use the node-level [`cluster.routing.allocation.total_shards_per_node`](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md#cluster-routing-allocation-total-shards-per-node) cluster setting. It applies across all indices on a node.
::::

::::{warning}
This setting imposes a hard limit which can result in some shards not being allocated.

Use with caution.

::::


