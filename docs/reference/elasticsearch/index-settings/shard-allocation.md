---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-allocation-filtering.html
navigation_title: Shard allocation
---

# Index-level shard allocation settings [shard-allocation-filtering]

You can use shard allocation filters to control where {{es}} allocates shards of a particular index. These per-index filters are applied in conjunction with [cluster-wide allocation filtering](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md#cluster-shard-allocation-filtering) and [allocation awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md).

Shard allocation filters can be based on [custom node attributes](/reference/elasticsearch/configuration-reference/node-settings.md#custom-node-attributes) or the built-in `_name`, `_host_ip`, `_publish_ip`, `_ip`, `_host`, `_id`, `_tier` and `_tier_preference` attributes. [Index lifecycle management](docs-content://manage-data/lifecycle/index-lifecycle-management.md) uses filters based on custom node attributes to determine how to reallocate shards when moving between phases.

The `cluster.routing.allocation` settings are dynamic, enabling existing indices to be moved immediately from one set of nodes to another. Shards are only relocated if it is possible to do so without breaking another routing constraint, such as never allocating a primary and replica shard on the same node.

For example, you could use a custom node attribute to indicate a node’s performance characteristics and use shard allocation filtering to route shards for a particular index to the most appropriate class of hardware.


## Enabling index-level shard allocation filtering [index-allocation-filters]

To filter based on a custom node attribute:

1. Specify the filter characteristics with a custom node attribute in each node’s `elasticsearch.yml` configuration file. For example, if you have `small`, `medium`, and `big` nodes, you could add a `size` attribute to filter based on node size.

    ```yaml
    node.attr.size: medium
    ```

    You can also set custom attributes when you start a node:

    ```sh
    ./bin/elasticsearch -Enode.attr.size=medium
    ```

2. Add a routing allocation filter to the index. The `index.routing.allocation` settings support three types of filters: `include`, `exclude`, and `require`. For example, to tell {{es}} to allocate shards from the `test` index to either `big` or `medium` nodes, use `index.routing.allocation.include`:

    ```console
    PUT test/_settings
    {
      "index.routing.allocation.include.size": "big,medium"
    }
    ```

    If you specify multiple filters the following conditions must be satisfied simultaneously by a node in order for shards to be relocated to it:

    * If any `require` type conditions are specified, all of them must be satisfied
    * If any `exclude` type conditions are specified, none of them may be satisfied
    * If any `include` type conditions are specified, at least one of them must be satisfied

    For example, to move the `test` index to `big` nodes in `rack1`, you could specify:

    ```console
    PUT test/_settings
    {
      "index.routing.allocation.require.size": "big",
      "index.routing.allocation.require.rack": "rack1"
    }
    ```



## Index allocation filter settings [index-allocation-settings]

`index.routing.allocation.include.{{attribute}}`
:   Assign the index to a node whose `{{attribute}}` has at least one of the comma-separated values.

`index.routing.allocation.require.{{attribute}}`
:   Assign the index to a node whose `{{attribute}}` has *all* of the comma-separated values.

`index.routing.allocation.exclude.{{attribute}}`
:   Assign the index to a node whose `{{attribute}}` has *none* of the comma-separated values.

The index allocation settings support the following built-in attributes:

`_name`
:   Match nodes by node name

`_host_ip`
:   Match nodes by host IP address (IP associated with hostname)

`_publish_ip`
:   Match nodes by publish IP address

`_ip`
:   Match either `_host_ip` or `_publish_ip`

`_host`
:   Match nodes by hostname

`_id`
:   Match nodes by node id

`_tier`
:   Match nodes by the node’s [data tier](docs-content://manage-data/lifecycle/data-tiers.md) role. For more details see [data tier allocation filtering](/reference/elasticsearch/index-settings/data-tier-allocation.md)

::::{note}
`_tier` filtering is based on [node](/reference/elasticsearch/configuration-reference/node-settings.md) roles. Only a subset of roles are [data tier](docs-content://manage-data/lifecycle/data-tiers.md) roles, and the generic [data role](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#data-node-role) will match any tier filtering.
::::


You can use wildcards when specifying attribute values, for example:

```console
PUT test/_settings
{
  "index.routing.allocation.include._ip": "192.168.2.*"
}
```

