---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-cluster.html
applies_to:
  deployment:
    ess:
    self:
---

# Cluster-level shard allocation and routing settings [modules-cluster]

Shard allocation is the process of assigning shard copies to nodes. This can happen during initial recovery, replica allocation, rebalancing, when nodes are added to or removed from the cluster, or when cluster or index settings that impact allocation are updated.

One of the main roles of the master is to decide which shards to allocate to which nodes, and when to move shards between nodes in order to rebalance the cluster.

There are a number of settings available to control the shard allocation process:

* [Cluster-level shard allocation settings](#cluster-shard-allocation-settings) control allocation and rebalancing operations.
* [Disk-based shard allocation settings](#disk-based-shard-allocation) explains how Elasticsearch takes available disk space into account, and the related settings.
* [Shard allocation awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md) and [Forced awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md#forced-awareness) control how shards can be distributed across different racks or availability zones.
* [Cluster-level shard allocation filtering](#cluster-shard-allocation-filtering) allows certain nodes or groups of nodes excluded from allocation so that they can be decommissioned.
* [Cluster-level node allocation stats cache settings](#node-allocation-stats-cache) control the node allocation statistics cache on the master node.

Besides these, there are a few other [miscellaneous cluster-level settings](/reference/elasticsearch/configuration-reference/miscellaneous-cluster-settings.md).

## Cluster-level shard allocation settings [cluster-shard-allocation-settings]

You can use the following settings to control shard allocation and recovery:

$$$cluster-routing-allocation-enable$$$

`cluster.routing.allocation.enable`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Enable or disable allocation for specific kinds of shards:

* `all` -             (default) Allows shard allocation for all kinds of shards.
* `primaries` -       Allows shard allocation only for primary shards.
* `new_primaries` -   Allows shard allocation only for primary shards for new indices.
* `none` -            No shard allocations of any kind are allowed for any indices.

This setting only affects future allocations, and does not re-allocate or un-allocate currently allocated shards. It also does not affect the recovery of local primary shards when restarting a node. A restarted node that has a copy of an unassigned primary shard will recover that primary immediately, assuming that its allocation id matches one of the active allocation ids in the cluster state.


$$$cluster-routing-allocation-same-shard-host$$$

`cluster.routing.allocation.same_shard.host`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) If `true`, forbids multiple copies of a shard from being allocated to distinct nodes on the same host, i.e. which have the same network address. Defaults to `false`, meaning that copies of a shard may sometimes be allocated to nodes on the same host. This setting is only relevant if you run multiple nodes on each host.

`cluster.routing.allocation.node_concurrent_incoming_recoveries`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) How many concurrent incoming shard recoveries are allowed to happen on a node. Incoming recoveries are the recoveries where the target shard (most likely the replica unless a shard is relocating) is allocated on the node. Defaults to `2`. Increasing this setting may cause shard movements to have a performance impact on other activity in your cluster, but may not make shard movements complete noticeably sooner. We do not recommend adjusting this setting from its default of `2`.

`cluster.routing.allocation.node_concurrent_outgoing_recoveries`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) How many concurrent outgoing shard recoveries are allowed to happen on a node. Outgoing recoveries are the recoveries where the source shard (most likely the primary unless a shard is relocating) is allocated on the node. Defaults to `2`. Increasing this setting may cause shard movements to have a performance impact on other activity in your cluster, but may not make shard movements complete noticeably sooner. We do not recommend adjusting this setting from its default of `2`.

`cluster.routing.allocation.node_concurrent_recoveries`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) A shortcut to set both `cluster.routing.allocation.node_concurrent_incoming_recoveries` and `cluster.routing.allocation.node_concurrent_outgoing_recoveries`. The value of this setting takes effect only when the more specific setting is not configured.  Defaults to `2`. Increasing this setting may cause shard movements to have a performance impact on other activity in your cluster, but may not make shard movements complete noticeably sooner. We do not recommend adjusting this setting from its default of `2`.

`cluster.routing.allocation.node_initial_primaries_recoveries`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) While the recovery of replicas happens over the network, the recovery of an unassigned primary after node restart uses data from the local disk. These should be fast so more initial primary recoveries can happen in parallel on each node. Defaults to `4`. Increasing this setting may cause shard recoveries to have a performance impact on other activity in your cluster, but may not make shard recoveries complete noticeably sooner. We do not recommend adjusting this setting from its default of `4`.


## Shard rebalancing settings [shards-rebalancing-settings]

A cluster is *balanced* when it has an equal number of shards on each node, with all nodes needing equal resources, without having a concentration of shards from any index on any node. {{es}} runs an automatic process called *rebalancing* which moves shards between the nodes in your cluster to improve its balance. Rebalancing obeys all other shard allocation rules such as [allocation filtering](#cluster-shard-allocation-filtering) and [forced awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md#forced-awareness) which may prevent it from completely balancing the cluster. In that case, rebalancing strives to achieve the most balanced cluster possible within the rules you have configured. If you are using [data tiers](docs-content://manage-data/lifecycle/data-tiers.md) then {{es}} automatically applies allocation filtering rules to place each shard within the appropriate tier. These rules mean that the balancer works independently within each tier.

You can use the following settings to control the rebalancing of shards across the cluster:

`cluster.routing.allocation.allow_rebalance`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specify when shard rebalancing is allowed:

* `always` -                    (default) Always allow rebalancing.
* `indices_primaries_active` -  Only when all primaries in the cluster are allocated.
* `indices_all_active` -        Only when all shards (primaries and replicas) in the cluster are allocated.


`cluster.routing.rebalance.enable`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Enable or disable rebalancing for specific kinds of shards:

* `all` -         (default) Allows shard balancing for all kinds of shards.
* `primaries` -   Allows shard balancing only for primary shards.
* `replicas` -    Allows shard balancing only for replica shards.
* `none` -        No shard balancing of any kind are allowed for any indices.

Rebalancing is important to ensure the cluster returns to a healthy and fully resilient state after a disruption. If you adjust this setting, remember to set it back to `all` as soon as possible.


`cluster.routing.allocation.cluster_concurrent_rebalance`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Defines the number of concurrent shard rebalances are allowed across the whole cluster. Defaults to `2`. Note that this setting only controls the number of concurrent shard relocations due to imbalances in the cluster. This setting does not limit shard relocations due to [allocation filtering](#cluster-shard-allocation-filtering) or [forced awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md#forced-awareness). Increasing this setting may cause the cluster to use additional resources moving shards between nodes, so we generally do not recommend adjusting this setting from its default of `2`.

`cluster.routing.allocation.type`
:   Selects the algorithm used for computing the cluster balance. Defaults to `desired_balance` which selects the *desired balance allocator*. This allocator runs a background task which computes the desired balance of shards in the cluster. Once this background task completes, {{es}} moves shards to their desired locations.


:::{admonition} Deprecated in 8.8
May also be set to `balanced` to select the legacy *balanced allocator*. This allocator was the default allocator in versions of {{es}} before 8.6.0. It runs in the foreground, preventing the master from doing other work in parallel. It works by selecting a small number of shard movements which immediately improve the balance of the cluster, and when those shard movements complete it runs again and selects another few shards to move. Since this allocator makes its decisions based only on the current state of the cluster, it will sometimes move a shard several times while balancing the cluster.
:::


## Shard balancing heuristics settings [shards-rebalancing-heuristics]

Rebalancing works by computing a *weight* for each node based on its allocation of shards, and then moving shards between nodes to reduce the weight of the heavier nodes and increase the weight of the lighter ones. The cluster is balanced when there is no possible shard movement that can bring the weight of any node closer to the weight of any other node by more than a configurable threshold.

The weight of a node depends on the number of shards it holds and on the total estimated resource usage of those shards expressed in terms of the size of the shard on disk and the number of threads needed to support write traffic to the shard. {{es}} estimates the resource usage of shards belonging to data streams when they are created by a rollover. The estimated disk size of the new shard is the mean size of the other shards in the data stream. The estimated write load of the new shard is a weighted average of the actual write loads of recent shards in the data stream. Shards that do not belong to the write index of a data stream have an estimated write load of zero.

The following settings control how {{es}} combines these values into an overall measure of each node’s weight.

`cluster.routing.allocation.balance.threshold`
:   (float, [Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The minimum improvement in weight which triggers a rebalancing shard movement. Defaults to `1.0f`. Raising this value will cause {{es}} to stop rebalancing shards sooner, leaving the cluster in a more unbalanced state.

`cluster.routing.allocation.balance.shard`
:   (float, [Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Defines the weight factor for the total number of shards allocated to each node. Defaults to `0.45f`. Raising this value increases the tendency of {{es}} to equalize the total number of shards across nodes ahead of the other balancing variables.

`cluster.routing.allocation.balance.index`
:   (float, [Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Defines the weight factor for the number of shards per index allocated to each node. Defaults to `0.55f`. Raising this value increases the tendency of {{es}} to equalize the number of shards of each index across nodes ahead of the other balancing variables.

`cluster.routing.allocation.balance.disk_usage`
:   (float, [Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Defines the weight factor for balancing shards according to their predicted disk size in bytes. Defaults to `2e-11f`. Raising this value increases the tendency of {{es}} to equalize the total disk usage across nodes ahead of the other balancing variables.

`cluster.routing.allocation.balance.write_load`
:   (float, [Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Defines the weight factor for the write load of each shard, in terms of the estimated number of indexing threads needed by the shard. Defaults to `10.0f`. Raising this value increases the tendency of {{es}} to equalize the total write load across nodes ahead of the other balancing variables.

::::{note}
* If you have a large cluster, it may be unnecessary to keep it in a perfectly balanced state at all times. It is less resource-intensive for the cluster to operate in a somewhat unbalanced state rather than to perform all the shard movements needed to achieve the perfect balance. If so, increase the value of `cluster.routing.allocation.balance.threshold` to define the acceptable imbalance between nodes. For instance, if you have an average of 500 shards per node and can accept a difference of 5% (25 typical shards) between nodes, set `cluster.routing.allocation.balance.threshold` to `25`.
* We do not recommend adjusting the values of the heuristic weight factor settings. The default values work well in all reasonable clusters. Although different values may improve the current balance in some ways, it is possible that they will create unexpected problems in the future or prevent it from gracefully handling an unexpected disruption.
* Regardless of the result of the balancing algorithm, rebalancing might not be allowed due to allocation rules such as forced awareness and allocation filtering. Use the [Cluster allocation explain](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-allocation-explain) API to explain the current allocation of shards.

::::



## Disk-based shard allocation settings [disk-based-shard-allocation]

$$$disk-based-shard-allocation-description$$$
The disk-based shard allocator ensures that all nodes have enough disk space without performing more shard movements than necessary. It allocates shards based on a pair of thresholds known as the *low watermark* and the *high watermark*. Its primary goal is to ensure that no node exceeds the high watermark, or at least that any such overage is only temporary. If a node exceeds the high watermark then {{es}} will solve this by moving some of its shards onto other nodes in the cluster.

::::{note}
It is normal for nodes to temporarily exceed the high watermark from time to time.
::::


The allocator also tries to keep nodes clear of the high watermark by forbidding the allocation of more shards to a node that exceeds the low watermark. Importantly, if all of your nodes have exceeded the low watermark then no new shards can be allocated and {{es}} will not be able to move any shards between nodes in order to keep the disk usage below the high watermark. You must ensure that your cluster has enough disk space in total and that there are always some nodes below the low watermark.

Shard movements triggered by the disk-based shard allocator must also satisfy all other shard allocation rules such as [allocation filtering](#cluster-shard-allocation-filtering) and [forced awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md#forced-awareness). If these rules are too strict then they can also prevent the shard movements needed to keep the nodes' disk usage under control. If you are using [data tiers](docs-content://manage-data/lifecycle/data-tiers.md) then {{es}} automatically configures allocation filtering rules to place shards within the appropriate tier, which means that the disk-based shard allocator works independently within each tier.

If a node is filling up its disk faster than {{es}} can move shards elsewhere then there is a risk that the disk will completely fill up. To prevent this, as a last resort, once the disk usage reaches the *flood-stage* watermark {{es}} will block writes to indices with a shard on the affected node. It will also continue to move shards onto the other nodes in the cluster. When disk usage on the affected node drops below the high watermark, {{es}} automatically removes the write block. Refer to [Fix watermark errors](docs-content://troubleshoot/elasticsearch/fix-watermark-errors.md) to resolve persistent watermark errors.

::::{admonition} Max headroom settings
:class: note

Max headroom settings apply only when watermark settings are percentages or ratios.

A max headroom value is intended to cap the required free disk space before hitting the respective watermark. This is useful for servers with larger disks, where a percentage or ratio watermark could translate to an overly large free disk space requirement. In this case, the max headroom can be used to cap the required free disk space amount.

For example, where `cluster.routing.allocation.disk.watermark.flood_stage` is 95% and `cluster.routing.allocation.disk.watermark.flood_stage.max_headroom` is 100GB, this means that:

* For a smaller disk, e.g., of 100GB, the flood watermark will hit at 95%, meaning at 5GB of free space, since 5GB is smaller than the 100GB max headroom value.
* For a larger disk, e.g., of 100TB, the flood watermark will hit at 100GB of free space. That is because the 95% flood watermark alone would require 5TB of free disk space, but is capped by the max headroom setting to 100GB.

Max headroom settings have their default values only if their respective watermark settings are not explicitly set. If watermarks are explicitly set, then the max headroom settings do not have their default values, and need to be explicitly set if they are needed.

::::


::::{tip}
:name: disk-based-shard-allocation-does-not-balance

It is normal for the nodes in your cluster to be using very different amounts of disk space. The [balance](#shards-rebalancing-settings) of the cluster depends on a combination of factors which includes the number of shards on each node, the indices to which those shards belong, and the resource needs of each shard in terms of its size on disk and its CPU usage. {{es}} must trade off all of these factors against each other, and a cluster which is balanced when looking at the combination of all of these factors may not appear to be balanced if you focus attention on just one of them.

::::


You can use the following settings to control disk-based allocation:

$$$cluster-routing-disk-threshold$$$

`cluster.routing.allocation.disk.threshold_enabled` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Defaults to `true`. Set to `false` to disable the disk allocation decider. Upon disabling, it will also remove any existing `index.blocks.read_only_allow_delete` index blocks.

$$$cluster-routing-watermark-low$$$

`cluster.routing.allocation.disk.watermark.low` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls the low watermark for disk usage. It defaults to `85%`, meaning that {{es}} will not allocate shards to nodes that have more than 85% disk used. It can alternatively be set to a ratio value, e.g., `0.85`. It can also be set to an absolute byte value (like `500mb`) to prevent {{es}} from allocating shards if less than the specified amount of space is available. This setting has no effect on the primary shards of newly-created indices but will prevent their replicas from being allocated.

`cluster.routing.allocation.disk.watermark.low.max_headroom`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls the max headroom for the low watermark (in case of a percentage/ratio value). Defaults to 200GB when `cluster.routing.allocation.disk.watermark.low` is not explicitly set. This caps the amount of free space required.

$$$cluster-routing-watermark-high$$$

`cluster.routing.allocation.disk.watermark.high` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls the high watermark. It defaults to `90%`, meaning that {{es}} will attempt to relocate shards away from a node whose disk usage is above 90%. It can alternatively be set to a ratio value, e.g., `0.9`. It can also be set to an absolute byte value (similarly to the low watermark) to relocate shards away from a node if it has less than the specified amount of free space. This setting affects the allocation of all shards, whether previously allocated or not.

`cluster.routing.allocation.disk.watermark.high.max_headroom`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls the max headroom for the high watermark (in case of a percentage/ratio value). Defaults to 150GB when `cluster.routing.allocation.disk.watermark.high` is not explicitly set. This caps the amount of free space required.

`cluster.routing.allocation.disk.watermark.enable_for_single_data_node`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) In earlier releases, the default behaviour was to disregard disk watermarks for a single data node cluster when making an allocation decision. This is deprecated behavior since 7.14 and has been removed in 8.0. The only valid value for this setting is now `true`. The setting will be removed in a future release.

$$$cluster-routing-flood-stage$$$

`cluster.routing.allocation.disk.watermark.flood_stage` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls the flood stage watermark, which defaults to 95%. {{es}} enforces a read-only index block ([`index.blocks.read_only_allow_delete`](/reference/elasticsearch/index-settings/index-block.md)) on every index that has one or more shards allocated on the node, and that has at least one disk exceeding the flood stage. This setting is a last resort to prevent nodes from running out of disk space. The index block is automatically released when the disk utilization falls below the high watermark. Similarly to the low and high watermark values, it can alternatively be set to a ratio value, e.g., `0.95`, or an absolute byte value.


`cluster.routing.allocation.disk.watermark.flood_stage.max_headroom`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls the max headroom for the flood stage watermark (in case of a percentage/ratio value). Defaults to 100GB when `cluster.routing.allocation.disk.watermark.flood_stage` is not explicitly set. This caps the amount of free space required.

::::{note}
You can’t mix the usage of percentage/ratio values and byte values across the `cluster.routing.allocation.disk.watermark.low`, `cluster.routing.allocation.disk.watermark.high`, and `cluster.routing.allocation.disk.watermark.flood_stage` settings. Either all values must be set to percentage/ratio values, or all must be set to byte values. This is required so that {{es}} can validate that the settings are internally consistent, ensuring that the low disk threshold is less than the high disk threshold, and the high disk threshold is less than the flood stage threshold. A similar comparison check is done for the max headroom values.
::::


$$$cluster-routing-flood-stage-frozen$$$

`cluster.routing.allocation.disk.watermark.flood_stage.frozen` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls the flood stage watermark for dedicated frozen nodes, which defaults to 95%.

`cluster.routing.allocation.disk.watermark.flood_stage.frozen.max_headroom` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls the max headroom for the flood stage watermark (in case of a percentage/ratio value) for dedicated frozen nodes. Defaults to 20GB when `cluster.routing.allocation.disk.watermark.flood_stage.frozen` is not explicitly set. This caps the amount of free space required on dedicated frozen nodes.

`cluster.info.update.interval`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) How often {{es}} should check on disk usage for each node in the cluster. Defaults to `30s`.

::::{note}
Percentage values refer to used disk space, while byte values refer to free disk space. This can be confusing, because it flips the meaning of high and low. For example, it makes sense to set the low watermark to 10gb and the high watermark to 5gb, but not the other way around.
::::



## Shard allocation awareness settings [shard-allocation-awareness-settings]

You can use [custom node attributes](/reference/elasticsearch/configuration-reference/node-settings.md#custom-node-attributes) as *awareness attributes* to enable {{es}} to take your physical hardware configuration into account when allocating shards. If {{es}} knows which nodes are on the same physical server, in the same rack, or in the same zone, it can distribute the primary shard and its replica shards to minimize the risk of losing all shard copies in the event of a failure. [Learn more about shard allocation awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md).

`cluster.routing.allocation.awareness.attributes`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The node attributes that {{es}} should use as awareness attributes. For example, if you have a `rack_id` attribute that specifies the rack in which each node resides, you can set this setting to `rack_id` to ensure that primary and replica shards are not allocated on the same rack. You can specify multiple attributes as a comma-separated list.

`cluster.routing.allocation.awareness.force.*`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The shard allocation awareness values that must exist for shards to be reallocated in case of location failure. Learn more about [forced awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md#forced-awareness).


## Cluster-level shard allocation filtering [cluster-shard-allocation-filtering]

You can use cluster-level shard allocation filters to control where {{es}} allocates shards from any index. These cluster wide filters are applied in conjunction with [per-index allocation filtering](/reference/elasticsearch/index-settings/shard-allocation.md) and [allocation awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md).

Shard allocation filters can be based on [custom node attributes](/reference/elasticsearch/configuration-reference/node-settings.md#custom-node-attributes) or the built-in `_name`, `_host_ip`, `_publish_ip`, `_ip`, `_host`, `_id` and `_tier` attributes.

The `cluster.routing.allocation` settings are [Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), enabling live indices to be moved from one set of nodes to another. Shards are only relocated if it is possible to do so without breaking another routing constraint, such as never allocating a primary and replica shard on the same node.

The most common use case for cluster-level shard allocation filtering is when you want to decommission a node. To move shards off of a node prior to shutting it down, you could create a filter that excludes the node by its IP address:

```console
PUT _cluster/settings
{
  "persistent" : {
    "cluster.routing.allocation.exclude._ip" : "10.0.0.1"
  }
}
```

### Cluster routing settings [cluster-routing-settings]

`cluster.routing.allocation.include.{{attribute}}`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Allocate shards to a node whose `{{attribute}}` has at least one of the comma-separated values.

`cluster.routing.allocation.require.{{attribute}}`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Only allocate shards to a node whose `{{attribute}}` has *all* of the comma-separated values.

`cluster.routing.allocation.exclude.{{attribute}}`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Do not allocate shards to a node whose `{{attribute}}` has *any* of the comma-separated values.

The cluster allocation settings support the following built-in attributes:

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
:   Match nodes by the node’s [data tier](docs-content://manage-data/lifecycle/data-tiers.md) role

::::{note}
`_tier` filtering is based on [node](/reference/elasticsearch/configuration-reference/node-settings.md) roles. Only a subset of roles are [data tier](docs-content://manage-data/lifecycle/data-tiers.md) roles, and the generic [data role](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#data-node-role) will match any tier filtering. a subset of roles that are [data tier](docs-content://manage-data/lifecycle/data-tiers.md) roles, but the generic [data role](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#data-node-role) will match any tier filtering.
::::


You can use wildcards when specifying attribute values, for example:

```console
PUT _cluster/settings
{
  "persistent": {
    "cluster.routing.allocation.exclude._ip": "192.168.2.*"
  }
}
```


## Node Allocation Stats Cache [node-allocation-stats-cache]

`cluster.routing.allocation.stats.cache.ttl`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Calculating the node allocation stats for a [Get node statistics API call](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-stats) can become expensive on the master for clusters with a high number of nodes. To prevent overloading the master the node allocation stats are cached on the master for 1 minute `1m` by default.  This setting can be used to adjust the cache time to live value, if necessary, keeping in mind the tradeoff between the freshness of the statistics and the processing costs on the master.  The cache can be disabled (not recommended) by setting the value to `0s` (the minimum value). The maximum value is 10 minutes `10m`.
