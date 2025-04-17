---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-node.html
applies_to:
  deployment:
    self:
---

# Node settings [modules-node]

Any time that you start an instance of {{es}}, you are starting a *node*. A collection of connected nodes is called a [cluster](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md). If you are running a single node of {{es}}, then you have a cluster of one node.

Every node in the cluster can handle [HTTP and transport](/reference/elasticsearch/configuration-reference/networking-settings.md) traffic by default. The transport layer is used exclusively for communication between nodes; the HTTP layer is used by REST clients.

$$$modules-node-description$$$
All nodes know about all the other nodes in the cluster and can forward client requests to the appropriate node.

::::{tip}
The performance of an {{es}} node is often limited by the performance of the underlying storage. Review our recommendations for optimizing your storage for [indexing](docs-content://deploy-manage/production-guidance/optimize-performance/indexing-speed.md#indexing-use-faster-hardware) and [search](docs-content://deploy-manage/production-guidance/optimize-performance/search-speed.md#search-use-faster-hardware).
::::


## Node name setting [node-name-settings]

{{es}} uses `node.name` as a human-readable identifier for a particular instance of {{es}}. This name is included in the response of many APIs. The node name defaults to the hostname of the machine when {{es}} starts, but can be configured explicitly in `elasticsearch.yml`:

```yaml
node.name: prod-data-2
```


## Node role settings [node-roles]

You define a node’s roles by setting `node.roles` in `elasticsearch.yml`. If you set `node.roles`, the node is only assigned the roles you specify. If you don’t set `node.roles`, the node is assigned the following roles:

* $$$master-node$$$`master`
* $$$data-node$$$`data`
* `data_content`
* `data_hot`
* `data_warm`
* `data_cold`
* `data_frozen`
* `ingest`
* $$$ml-node$$$`ml`
* `remote_cluster_client`
* $$$transform-node$$$`transform`

The following additional roles are available:

* `voting_only`

$$$coordinating-only-node$$$If If you set `node.roles` to an empty array (`node.roles: [ ]`), then the node is considered to be a [coordinating only node](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#coordinating-only-node-role).

::::{important}
If you set `node.roles`, ensure you specify every node role your cluster needs. Every cluster requires the following node roles:

* `master`
* `data_content` and `data_hot` OR `data`


Some {{stack}} features also require specific node roles:

* {{ccs-cap}} and {{ccr}} require the `remote_cluster_client` role.
* {{stack-monitor-app}} and ingest pipelines require the `ingest` role.
* {{fleet}}, the {{security-app}}, and {{transforms}} require the `transform` role. The `remote_cluster_client` role is also required to use {{ccs}} with these features.
* {{ml-cap}} features, such as {{anomaly-detect}}, require the `ml` role.

::::


As the cluster grows and in particular if you have large {{ml}} jobs or {{ctransforms}}, consider separating dedicated master-eligible nodes from dedicated data nodes, {{ml}} nodes, and {{transform}} nodes.

To learn more about the available node roles, see [*Node roles*](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md).


## Node data path settings [_node_data_path_settings]


## `path.data` [data-path]

Every data and master-eligible node requires access to a data directory where shards and index and cluster metadata will be stored. The `path.data` defaults to `$ES_HOME/data` but can be configured in the `elasticsearch.yml` config file an absolute path or a path relative to `$ES_HOME` as follows:

```yaml
path.data:  /var/elasticsearch/data
```

Like all node settings, it can also be specified on the command line as:

```sh
./bin/elasticsearch -Epath.data=/var/elasticsearch/data
```

The contents of the `path.data` directory must persist across restarts, because this is where your data is stored. {{es}} requires the filesystem to act as if it were backed by a local disk, but this means that it will work correctly on properly-configured remote block devices (e.g. a SAN) and remote filesystems (e.g. NFS) as long as the remote storage behaves no differently from local storage. You can run multiple {{es}} nodes on the same filesystem, but each {{es}} node must have its own data path.

::::{tip}
When using the `.zip` or `.tar.gz` distributions, the `path.data` setting should be configured to locate the data directory outside the {{es}} home directory, so that the home directory can be deleted without deleting your data! The RPM and Debian distributions do this for you already.
::::


::::{warning}
Don’t modify anything within the data directory or run processes that might interfere with its contents. If something other than {{es}} modifies the contents of the data directory, then {{es}} may fail, reporting corruption or other data inconsistencies, or may appear to work correctly having silently lost some of your data. Don’t attempt to take filesystem backups of the data directory; there is no supported way to restore such a backup. Instead, use [Snapshot and restore](docs-content://deploy-manage/tools/snapshot-and-restore.md) to take backups safely. Don’t run virus scanners on the data directory. A virus scanner can prevent {{es}} from working correctly and may modify the contents of the data directory. The data directory contains no executables so a virus scan will only find false positives.
::::



## Custom node attributes [custom-node-attributes]

If needed, you can add custom attributes to a node. These attributes can be used to [filter which nodes a shard can be allocated to](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md#cluster-routing-settings), or to group nodes together for [shard allocation awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md).

::::{tip}
You can also set a node attribute using the `-E` command line argument when you start a node:

```sh
./bin/elasticsearch -Enode.attr.rack_id=rack_one
```

::::


`node.attr.<attribute-name>`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) A custom attribute that you can assign to a node. For example, you might assign a `rack_id` attribute to each node to ensure that primary and replica shards are not allocated on the same rack. You can specify multiple attributes as a comma-separated list.


## Other node settings [other-node-settings]

More node settings can be found in [*Configuring {{es}}*](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md) and [Important {{es}} configuration](docs-content://deploy-manage/deploy/self-managed/important-settings-configuration.md), including:

* [`cluster.name`](/reference/elasticsearch/configuration-reference/miscellaneous-cluster-settings.md#cluster-name)
* [`node.name`](docs-content://deploy-manage/deploy/self-managed/important-settings-configuration.md#node-name)
* [network settings](/reference/elasticsearch/configuration-reference/networking-settings.md)


