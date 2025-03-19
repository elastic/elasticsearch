---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/misc-cluster-settings.html
applies_to:
  deployment:
    self:
---

# Miscellaneous cluster settings [misc-cluster-settings]


## Cluster name setting [cluster-name]

A node can only join a cluster when it shares its `cluster.name` with all the other nodes in the cluster. The default name is `elasticsearch`, but you should change it to an appropriate name that describes the purpose of the cluster.

```yaml
cluster.name: logging-prod
```

::::{important}
Do not reuse the same cluster names in different environments. Otherwise, nodes might join the wrong cluster.
::::


::::{note}
Changing the name of a cluster requires a [full cluster restart](docs-content://deploy-manage/maintenance/start-stop-services/full-cluster-restart-rolling-restart-procedures.md#restart-cluster-full).
::::



## Metadata [cluster-read-only]

An entire cluster may be set to read-only with the following setting:

`cluster.blocks.read_only`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Make the whole cluster read only (indices do not accept write operations), metadata is not allowed to be modified (create or delete indices). Defaults to `false`.

`cluster.blocks.read_only_allow_delete`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Identical to `cluster.blocks.read_only` but allows to delete indices to free up resources. Defaults to `false`.

::::{warning}
Don’t rely on this setting to prevent changes to your cluster. Any user with access to the [cluster-update-settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings) API can make the cluster read-write again.
::::



## Cluster shard limits [cluster-shard-limit]

There is a limit on the number of shards in a cluster, based on the number of nodes in the cluster. This is intended to prevent a runaway process from creating too many shards which can harm performance and in extreme cases may destabilize your cluster.

::::{important}
These limits are intended as a safety net to protect against runaway shard creation and are not a sizing recommendation. The exact number of shards your cluster can safely support depends on your hardware configuration and workload, and may be smaller than the default limits.

We do not recommend increasing these limits beyond the defaults. Clusters with more shards may appear to run well in normal operation, but may take a very long time to recover from temporary disruptions such as a network partition or an unexpected node restart, and may encounter problems when performing maintenance activities such as a rolling restart or upgrade.

::::


If an operation, such as creating a new index, restoring a snapshot of an index, or opening a closed index would lead to the number of shards in the cluster going over this limit, the operation will fail with an error indicating the shard limit. To resolve this, either scale out your cluster by adding nodes, or [delete some indices](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete) to bring the number of shards below the limit.

If a cluster is already over the limit, perhaps due to changes in node membership or setting changes, all operations that create or open indices will fail.

The cluster shard limit defaults to 1000 shards per non-frozen data node for normal (non-frozen) indices and 3000 shards per frozen data node for frozen indices. Both primary and replica shards of all open indices count toward the limit, including unassigned shards. For example, an open index with 5 primary shards and 2 replicas counts as 15 shards. Closed indices do not contribute to the shard count.

You can dynamically adjust the cluster shard limit with the following setting:

$$$cluster-max-shards-per-node$$$

`cluster.max_shards_per_node`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Limits the total number of primary and replica shards for the cluster. {{es}} calculates the limit as follows:

`cluster.max_shards_per_node * number of non-frozen data nodes`

Shards for closed indices do not count toward this limit. Defaults to `1000`. A cluster with no data nodes is unlimited.

{{es}} rejects any request that creates more shards than this limit allows. For example, a cluster with a `cluster.max_shards_per_node` setting of `100` and three data nodes has a shard limit of 300. If the cluster already contains 296 shards, {{es}} rejects any request that adds five or more shards to the cluster.

Note that if `cluster.max_shards_per_node` is set to a higher value than the default, the limits for [mmap count](docs-content://deploy-manage/deploy/self-managed/vm-max-map-count.md) and [open file descriptors](docs-content://deploy-manage/deploy/self-managed/file-descriptors.md) might also require adjustment.

Notice that frozen shards have their own independent limit.


$$$cluster-max-shards-per-node-frozen$$$

`cluster.max_shards_per_node.frozen`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Limits the total number of primary and replica frozen shards for the cluster. {{es}} calculates the limit as follows:

`cluster.max_shards_per_node.frozen * number of frozen data nodes`

Shards for closed indices do not count toward this limit. Defaults to `3000`. A cluster with no frozen data nodes is unlimited.

{{es}} rejects any request that creates more frozen shards than this limit allows. For example, a cluster with a `cluster.max_shards_per_node.frozen` setting of `100` and three frozen data nodes has a frozen shard limit of 300. If the cluster already contains 296 shards, {{es}} rejects any request that adds five or more frozen shards to the cluster.


::::{note}
These limits only apply to actions which create shards and do not limit the number of shards assigned to each node. To limit the number of shards assigned to each node, use the [`cluster.routing.allocation.total_shards_per_node`](/reference/elasticsearch/index-settings/total-shards-per-node.md#cluster-total-shards-per-node) setting.
::::



## User-defined cluster metadata [user-defined-data]

User-defined metadata can be stored and retrieved using the Cluster Settings API. This can be used to store arbitrary, infrequently-changing data about the cluster without the need to create an index to store it. This data may be stored using any key prefixed with `cluster.metadata.`. For example, to store the email address of the administrator of a cluster under the key `cluster.metadata.administrator`, issue this request:

```console
PUT /_cluster/settings
{
  "persistent": {
    "cluster.metadata.administrator": "sysadmin@example.com"
  }
}
```

::::{important}
User-defined cluster metadata is not intended to store sensitive or confidential information. Any information stored in user-defined cluster metadata will be viewable by anyone with access to the [Cluster Get Settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-get-settings) API, and is recorded in the {{es}} logs.
::::



## Index tombstones [cluster-max-tombstones]

The cluster state maintains index tombstones to explicitly denote indices that have been deleted. The number of tombstones maintained in the cluster state is controlled by the following setting:

`cluster.indices.tombstones.size`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Index tombstones prevent nodes that are not part of the cluster when a delete occurs from joining the cluster and reimporting the index as though the delete was never issued. To keep the cluster state from growing huge we only keep the last `cluster.indices.tombstones.size` deletes, which defaults to 500. You can increase it if you expect nodes to be absent from the cluster and miss more than 500 deletes. We think that is rare, thus the default. Tombstones don’t take up much space, but we also think that a number like 50,000 is probably too big.

If {{es}} encounters index data that is absent from the current cluster state, those indices are considered to be dangling. For example, this can happen if you delete more than `cluster.indices.tombstones.size` indices while an {{es}} node is offline.

You can use the [Dangling indices API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-indices) to manage this situation.


## Logger [cluster-logger]

The settings which control logging can be updated [dynamically](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting) with the `logger.` prefix. For instance, to increase the logging level of the `indices.recovery` module to `DEBUG`, issue this request:

```console
PUT /_cluster/settings
{
  "persistent": {
    "logger.org.elasticsearch.indices.recovery": "DEBUG"
  }
}
```


## Persistent tasks allocation [persistent-tasks-allocation]

Plugins can create a kind of tasks called persistent tasks. Those tasks are usually long-lived tasks and are stored in the cluster state, allowing the tasks to be revived after a full cluster restart.

Every time a persistent task is created, the master node takes care of assigning the task to a node of the cluster, and the assigned node will then pick up the task and execute it locally. The process of assigning persistent tasks to nodes is controlled by the following settings:

`cluster.persistent_tasks.allocation.enable`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Enable or disable allocation for persistent tasks:

* `all` -             (default) Allows persistent tasks to be assigned to nodes
* `none` -            No allocations are allowed for any type of persistent task

This setting does not affect the persistent tasks that are already being executed. Only newly created persistent tasks, or tasks that must be reassigned (after a node left the cluster, for example), are impacted by this setting.


`cluster.persistent_tasks.allocation.recheck_interval`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The master node will automatically check whether persistent tasks need to be assigned when the cluster state changes significantly. However, there may be other factors, such as memory usage, that affect whether persistent tasks can be assigned to nodes but do not cause the cluster state to change. This setting controls how often assignment checks are performed to react to these factors. The default is 30 seconds. The minimum permitted value is 10 seconds.

