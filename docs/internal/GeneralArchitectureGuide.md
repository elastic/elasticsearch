# General Architecture

## REST & Transport layers

[TransportAction]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/TransportAction.java
[ActionPlugin]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/plugins/ActionPlugin.java
[ActionModule]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/ActionModule.java

There are two main types of network communication used in Elasticsearch:
- External clients interact with the cluster via the public REST API over HTTP connections
- Cluster nodes communicate internally using a binary messaging format over TCP connections

> [!NOTE]
> Cross-cluster [replication](https://www.elastic.co/guide/en/elasticsearch/reference/current/xpack-ccr.html) and [search](https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-cross-cluster-search.html) use binary/TCP messaging for inter-cluster communication but this is out of scope for this section

The node that a REST request arrives at is called the "coordinating" node. Its job is to coordinate the execution of the request with the other nodes in the cluster and when the requested action is completed, return the response the REST client via HTTP.

By default, all nodes will act as coordinating nodes, but by specifying `node.roles` to be empty you can create a [coordinating-only node](https://www.elastic.co/guide/en/elasticsearch/reference/current/node-roles-overview.html#coordinating-only-node-role).

### REST Layer

[BaseRestHandler]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/rest/BaseRestHandler.java
[RestHandler]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/rest/RestHandler.java
[Route]:https://github.com/elastic/elasticsearch/blob/0b09506b543231862570c7c1ee623c1af139bd5a/server/src/main/java/org/elasticsearch/rest/RestHandler.java#L134
[getRestHandlers]:https://github.com/elastic/elasticsearch/blob/0b09506b543231862570c7c1ee623c1af139bd5a/server/src/main/java/org/elasticsearch/plugins/ActionPlugin.java#L76
[RestBulkAction]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/rest/action/document/RestBulkAction.java

Each REST endpoint is defined by a [RestHandler] instance. [RestHandler] implementations define the list of [Route]s that they handle, the request handling logic, and some other runtime characteristics such as path/query parameters that are supported and the content type(s) it accepts. There are many built-in REST endpoints configured statically in [ActionModule], additional endpoints can be contributed by [ActionPlugin]s via the [getRestHandlers] method.

[BaseRestHandler] is the base class for almost all REST endpoints in Elasticsearch. It validates the request parameters against those which are supported, delegates to its sub-classes to set up the execution of the requested action, then delivers the request content to the action either as a single parsed payload or a stream of binary chunks. Actions such as the [RestBulkAction] use the streaming capability to process large payloads incrementally and apply back-pressure when overloaded.

The sub-classes of [BaseRestHandler], usually named `Rest*Action`, are the entry-points to the cluster, where HTTP requests from outside the cluster are translated into internal [TransportAction] invocations.

### Transport Layer

[ActionRequest]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/ActionRequest.java
[NodeClient]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/client/internal/node/NodeClient.java
[TransportMasterNodeAction]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/master/TransportMasterNodeAction.java
[TransportLocalClusterStateAction]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/local/TransportLocalClusterStateAction.java
[TransportReplicationAction]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/replication/TransportReplicationAction.java
[TransportNodesAction]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/nodes/TransportNodesAction.java
[TransportSingleShardAction]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/single/shard/TransportSingleShardAction.java
[getActions]:https://github.com/elastic/elasticsearch/blob/0b09506b543231862570c7c1ee623c1af139bd5a/server/src/main/java/org/elasticsearch/plugins/ActionPlugin.java#L55
[ActionType]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/ActionType.java

When `Rest*Action` handlers receive a request, they typically translate the request into a [ActionRequest] and dispatch it via the provided [NodeClient]. The [NodeClient] is the entrypoint into the "transport layer" over which internal cluster actions are coordinated.

Elasticsearch contains many built-in [TransportAction]s, configured statically in [ActionModule], additional actions can be contributed by [ActionPlugin]s via the [getActions] method. [TransportAction]s define the request and response types used to invoke the action and the logic for performing the action. [TransportAction]s are registered against an [ActionType] which uniquely identifies the action.

The [NodeClient] executes all actions locally on the invoking node, the actions themselves contain logic for dispatching downstream actions to other nodes in the cluster via the transport layer.

There are a few common patterns for [TransportAction] execution which are present in the codebase. Some prominent examples include...

- [TransportMasterNodeAction]: Executes an action on the master node. Typically used to perform cluster state updates, as these can only be performed on the master. The base class contains logic for locating the master node and delegating to it to execute the specified logic.
- [TransportNodesAction]: Executes an action on many nodes then collates the responses.
- [TransportLocalClusterStateAction]: Waits for a cluster state that optionally meets some criteria and performs a read action on it on the coordinating node.
- [TransportReplicationAction]: Execute an action on a primary shard followed by all replicas that exist for that shard. The base class implements logic for locating the primary and replica shards in the cluster and delegating to the relevant nodes. Often used for index updates in stateful Elasticsearch.
- [TransportSingleShardAction]: Executes a read operation on a specific shard, the base class contains logic for locating an available copy of the nominated shard and delegating to the relevant node to execute the action. On a failure, the action is retried on a different copy.

## Serializations

## Settings

Elasticsearch supports [cluster-level settings][] and [index-level settings][], configurable via [node-level file settings][]
(e.g. `elasticsearch.yml` file), command line arguments and REST APIs.

### Declaring a Setting

[cluster-level settings]: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html
[index-level settings]: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html
[node-level file settings]: https://www.elastic.co/guide/en/elasticsearch/reference/current/settings.html

The [Setting][] class is the building block for Elasticsearch server settings. Each `Setting` can take multiple [Property][]
declarations to define setting characteristics. All setting values first come from the node-local `elasticsearch.yml` file,
if they are set therein, before falling back to the default specified in their `Setting` declaration. [A setting][] with
`Property.Dynamic` can be updated during runtime, but must be paired with a [local volatile variable like this one][] and
registered in the `ClusterSettings` via a utility like [ClusterSettings#initializeAndWatch()][] to catch and immediately
apply dynamic changes. NB that a common dynamic Setting bug is always reading the value directly from [Metadata#settings()][],
which holds the default and dynamically updated values, but _not_ the node-local `elasticsearch.yml` value. The scope of a
Setting must also be declared, such as `Property.IndexScope` for a setting that applies to indexes, or `Property.NodeScope`
for a cluster-level setting.

[Setting]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/Setting.java#L57-L80
[Property]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/Setting.java#L82
[A setting]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/routing/allocation/allocator/BalancedShardsAllocator.java#L111-L117
[local volatile variable like this one]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/routing/allocation/allocator/BalancedShardsAllocator.java#L123
[ClusterSettings#initializeAndWatch()]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/routing/allocation/allocator/BalancedShardsAllocator.java#L145
[Metadata#settings()]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/metadata/Metadata.java#L713-L715

[ClusterSettings][] tracks the [core Elasticsearch settings][]. Ultimately the `ClusterSettings` get loaded via the
[SettingsModule][]. Additional settings from the various plugins are [collected during node construction] and passed into the
[SettingsModule constructor][]. The Plugin interface has a [getSettings()][] method via which each plugin can declare additional
settings.

[ClusterSettings]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/ClusterSettings.java#L138
[core Elasticsearch settings]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/ClusterSettings.java#L204-L586
[SettingsModule]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/SettingsModule.java#L54
[collected during node construction]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/node/NodeConstruction.java#L483
[SettingsModule constructor]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/node/NodeConstruction.java#L491-L495
[getSettings()]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/plugins/Plugin.java#L203-L208

### Dynamically updating a Setting

Externally, [TransportClusterUpdateSettingsAction][] and [TransportUpdateSettingsAction][] (and the corresponding REST endpoints)
allow users to dynamically change cluster and index settings, respectively. Internally, `AbstractScopedSettings` (parent class
of `ClusterSettings`) has various helper methods to track dynamic changes: it keeps a [registry of `SettingUpdater`][] consumer
lambdas to run updates when settings are changed in the cluster state. The `ClusterApplierService` [sends setting updates][]
through to the `AbstractScopedSettings`, invoking the consumers registered therein for each updated setting.

[TransportClusterUpdateSettingsAction]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/action/admin/cluster/settings/TransportClusterUpdateSettingsAction.java#L154-L160
[TransportUpdateSettingsAction]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/action/admin/indices/settings/put/TransportUpdateSettingsAction.java#L96-L101
[registry of `SettingUpdater`]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/AbstractScopedSettings.java#L379-L381
[sends setting updates]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/service/ClusterApplierService.java#L490-L494

Index settings are always persisted. They can only be modified on an existing index, and setting values are persisted as part
of the `IndexMetadata`. Cluster settings, however, can be either persisted or transient depending on how they are tied to
[Metadata][] ([applied here][]). Changes to persisted cluster settings will survive a full cluster restart; whereas changes
made to transient cluster settings will reset to their default values, or the `elasticsearch.yml` values, if the cluster
state must ever be reloaded from persisted state.

[Metadata]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/metadata/Metadata.java#L212-L213
[applied here]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/metadata/Metadata.java#L2437

## Deprecations

## Backwards Compatibility

major releases are mostly about breaking compatibility and dropping deprecated functionality.

Elasticsearch versions are composed of three pieces of information: the major version, the minor version, and the patch version,
in that order (major.minor.patch). Patch releases are typically bug fixes; minor releases contain improvements / new features;
and major releases essentially break compatibility and enable removal of deprecated functionality. As an example, each of 8.0.0,
8.3.0 and 8.3.1 specifies an exact release version. They all have the same major version (8) and the last two have the same minor
version (8.3). Multiversion compatibility within a cluster, or backwards compatibility with older version nodes, is guaranteed
across specific versions.

### Transport Layer Backwards Compatibility

Elasticsearch nodes can communicate over the network with all node versions within the same major release. All versions within
one major version X are also compatible with the last minor version releases of the previous major version, i.e. (X-1).last.
More concretely, all 8.x.x version nodes can communicate with all 7.17.x version nodes.

### Index Format Backwards Compatibility

Index data format backwards compatibility is guaranteed with all versions of the previous major release. All 8.x.x version nodes,
for example, can read index data written by any 7.x.x version node. 9.x.x versions, however, will not be able to read 7.x.x format
data files.

Elasticsearch does not have an upgrade process to convert from older to newer index data formats. The user is expected to run
`reindex` on any remaining untouched data from a previous version upgrade before upgrading to the next version. There is a good
chance that older version index data will age out and be deleted before the user does the next upgrade, but `reindex` can be used
if that is not the case.

### Snapshot Backwards Compatibility

Snapshots taken by a cluster of version X cannot be read by a cluster running older version nodes. However, snapshots taken by an
older version cluster can continue to be read from and written to by newer version clusters: this compatibility goes back many
major versions. If a newer version cluster writes to a snapshot repository containing snapshots from an older version, then it
will do so in a way that leaves the repository format (metadata and file layout) readable by those older versions.

Restoring indexes that have different and no longer supported data formats can be tricky: see the
[public snapshot compatibility docs][] for details.

[public snapshot compatibility docs]: https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshot-restore.html#snapshot-index-compatibility

### Upgrade

See the [public upgrade docs][] for the upgrade process.

[public upgrade docs]: https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-upgrade.html

## Plugins

(what warrants a plugin?)

(what plugins do we have?)

## Testing

(Overview of our testing frameworks. Discuss base test classes.)

### Unit Testing

### REST Testing

### Integration Testing
