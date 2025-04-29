---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/recovery.html
applies_to:
  deployment:
    self:
---

# Index recovery settings [recovery]

Peer recovery syncs data from a primary shard to a new or existing shard copy.

Peer recovery automatically occurs when {{es}}:

* Recreates a shard lost during node failure
* Relocates a shard to another node due to a cluster rebalance or changes to the [shard allocation settings](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md)

You can view a list of in-progress and completed recoveries using the [cat recovery API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-recovery).


## Recovery settings [recovery-settings]

`indices.recovery.max_bytes_per_sec`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)) Limits total inbound and outbound recovery traffic for each node. Applies to both peer recoveries as well as snapshot recoveries (i.e., restores from a snapshot). Defaults to `40mb` unless the node is a dedicated [cold](docs-content://manage-data/lifecycle/data-tiers.md#cold-tier) or [frozen](docs-content://manage-data/lifecycle/data-tiers.md#frozen-tier) node, in which case the default relates to the total memory available to the node:

    | Total memory | Default recovery rate on cold and frozen nodes |
    | --- | --- |
    | ≤ 4 GB | 40 MB/s |
    | > 4 GB and ≤ 8 GB | 60 MB/s |
    | > 8 GB and ≤ 16 GB | 90 MB/s |
    | > 16 GB and ≤ 32 GB | 125 MB/s |
    | > 32 GB | 250 MB/s |

    This limit applies to each node separately. If multiple nodes in a cluster perform recoveries at the same time, the cluster’s total recovery traffic may exceed this limit.

    If this limit is too high, ongoing recoveries may consume an excess of bandwidth and other resources, which can have a performance impact on your cluster and in extreme cases may destabilize it.

    This is a dynamic setting, which means you can set it in each node’s `elasticsearch.yml` config file and you can update it dynamically using the [cluster update settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings). If you set it dynamically then the same limit applies on every node in the cluster. If you do not set it dynamically then you can set a different limit on each node, which is useful if some of your nodes have better bandwidth than others. For example, if you are using [Index Lifecycle Management](docs-content://manage-data/lifecycle/index-lifecycle-management.md) then you may be able to give your hot nodes a higher recovery bandwidth limit than your warm nodes.



## Expert peer recovery settings [_expert_peer_recovery_settings]

You can use the following *expert* setting to manage resources for peer recoveries.

`indices.recovery.max_concurrent_file_chunks`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings), Expert) Number of file chunks sent in parallel for each recovery. Defaults to `2`.

    You can increase the value of this setting when the recovery of a single shard is not reaching the traffic limit set by `indices.recovery.max_bytes_per_sec`, up to a maximum of `8`.


`indices.recovery.max_concurrent_operations`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings), Expert) Number of operations sent in parallel for each recovery. Defaults to `1`.

    Concurrently replaying operations during recovery can be very resource-intensive and may interfere with indexing, search, and other activities in your cluster. Do not increase this setting without carefully verifying that your cluster has the resources available to handle the extra load that will result.


`indices.recovery.use_snapshots`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings), Expert) Enables snapshot-based peer recoveries.

    {{es}} recovers replicas and relocates primary shards using the *peer recovery* process, which involves constructing a new copy of a shard on the target node. When `indices.recovery.use_snapshots` is `false` {{es}} will construct this new copy by transferring the index data from the current primary. When this setting is `true` {{es}} will attempt to copy the index data from a recent snapshot first, and will only copy data from the primary if it cannot identify a suitable snapshot. Defaults to `true`.

    Setting this option to `true` reduces your operating costs if your cluster runs in an environment where the node-to-node data transfer costs are higher than the costs of recovering data from a snapshot. It also reduces the amount of work that the primary must do during a recovery.

    Additionally, repositories having the setting `use_for_peer_recovery=true` will be consulted to find a good snapshot when recovering a shard. If none of the registered repositories have this setting defined, index files will be recovered from the source node.


`indices.recovery.max_concurrent_snapshot_file_downloads`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings), Expert) Number of snapshot file downloads requests sent in parallel to the target node for each recovery. Defaults to `5`.

    Do not increase this setting without carefully verifying that your cluster has the resources available to handle the extra load that will result.


`indices.recovery.max_concurrent_snapshot_file_downloads_per_node`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings), Expert) Number of snapshot file downloads requests executed in parallel in the target node for all recoveries. Defaults to `25`.

    Do not increase this setting without carefully verifying that your cluster has the resources available to handle the extra load that will result.



## Recovery settings for managed services [recovery-settings-for-managed-services]

::::{note}
{{cloud-only}}
::::


When running {{es}} as a managed service, the following settings allow the service to specify absolute maximum bandwidths for disk reads, disk writes, and network traffic on each node, and permit you to control the maximum recovery bandwidth on each node in terms of these absolute maximum values. They have two effects:

1. They determine the bandwidth used for recovery if `indices.recovery.max_bytes_per_sec` is not set, overriding the default behaviour described above.
2. They impose a node-wide limit on recovery bandwidth which is independent of the value of `indices.recovery.max_bytes_per_sec`.

If you do not set `indices.recovery.max_bytes_per_sec` then the maximum recovery bandwidth is computed as a proportion of the absolute maximum bandwidth. The computation is performed separately for read and write traffic. The service defines the absolute maximum bandwidths for disk reads, disk writes, and network transfers using `node.bandwidth.recovery.disk.read`, `node.bandwidth.recovery.disk.write` and `node.bandwidth.recovery.network` respectively, and you can set the proportion of the absolute maximum bandwidth that may be used for recoveries by adjusting `node.bandwidth.recovery.factor.read` and `node.bandwidth.recovery.factor.write`. If the {{operator-feature}} is enabled then the service may also set default proportions using operator-only variants of these settings.

If you set `indices.recovery.max_bytes_per_sec` then {{es}} will use its value for the maximum recovery bandwidth, as long as this does not exceed the node-wide limit. {{es}} computes the node-wide limit by multiplying the absolute maximum bandwidths by the `node.bandwidth.recovery.operator.factor.max_overcommit` factor. If you set `indices.recovery.max_bytes_per_sec` in excess of the node-wide limit then the node-wide limit takes precedence.

The service should determine values for the absolute maximum bandwidths settings by experiment, using a recovery-like workload in which there are several concurrent workers each processing files sequentially in chunks of 512kiB.

`node.bandwidth.recovery.disk.read`
:   ([byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units) per second) The absolute maximum disk read speed for a recovery-like workload on the node. If set, `node.bandwidth.recovery.disk.write` and `node.bandwidth.recovery.network` must also be set.

`node.bandwidth.recovery.disk.write`
:   ([byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units) per second) The absolute maximum disk write speed for a recovery-like workload on the node. If set, `node.bandwidth.recovery.disk.read` and `node.bandwidth.recovery.network` must also be set.

`node.bandwidth.recovery.network`
:   ([byte value](/reference/elasticsearch/rest-apis/api-conventions.md#byte-units) per second) The absolute maximum network throughput for a recovery-like workload on the node, which applies to both reads and writes. If set, `node.bandwidth.recovery.disk.read` and `node.bandwidth.recovery.disk.write` must also be set.

`node.bandwidth.recovery.factor.read`
:   (float, [dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The proportion of the maximum read bandwidth that may be used for recoveries if `indices.recovery.max_bytes_per_sec` is not set. Must be greater than `0` and not greater than `1`. If not set, the value of `node.bandwidth.recovery.operator.factor.read` is used. If no factor settings are set then the value `0.4` is used.

`node.bandwidth.recovery.factor.write`
:   (float, [dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The proportion of the maximum write bandwidth that may be used for recoveries if `indices.recovery.max_bytes_per_sec` is not set. Must be greater than `0` and not greater than `1`. If not set, the value of `node.bandwidth.recovery.operator.factor.write` is used. If no factor settings are set then the value `0.4` is used.

`node.bandwidth.recovery.operator.factor.read`
:   (float, [dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The proportion of the maximum read bandwidth that may be used for recoveries if `indices.recovery.max_bytes_per_sec` and `node.bandwidth.recovery.factor.read` are not set. Must be greater than `0` and not greater than `1`. If not set, the value of `node.bandwidth.recovery.operator.factor` is used. If no factor settings are set then the value `0.4` is used. When the {{operator-feature}} is enabled, this setting can be updated only by operator users.

`node.bandwidth.recovery.operator.factor.write`
:   (float, [dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The proportion of the maximum write bandwidth that may be used for recoveries if `indices.recovery.max_bytes_per_sec` and `node.bandwidth.recovery.factor.write` are not set. Must be greater than `0` and not greater than `1`. If not set, the value of `node.bandwidth.recovery.operator.factor` is used. If no factor settings are set then the value `0.4` is used. When the {{operator-feature}} is enabled, this setting can be updated only by operator users.

`node.bandwidth.recovery.operator.factor`
:   (float, [dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The proportion of the maximum bandwidth that may be used for recoveries if neither `indices.recovery.max_bytes_per_sec` nor any other factor settings are set. Must be greater than `0` and not greater than `1`. Defaults to `0.4`. When the {{operator-feature}} is enabled, this setting can be updated only by operator users.

`node.bandwidth.recovery.operator.factor.max_overcommit`
:   (float, [dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The proportion of the absolute maximum bandwidth that may be used for recoveries regardless of any other settings. Must be greater than `0`. Defaults to `100`. When the {{operator-feature}} is enabled, this setting can be updated only by operator users.

