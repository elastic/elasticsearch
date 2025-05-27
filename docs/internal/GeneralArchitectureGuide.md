# General Architecture

# REST and Transport Layers

In general, there are two types of network communication used in Elasticsearch:
- External clients interact with the cluster via the public REST API over HTTP connections, this is referred to as the "REST layer"
- Cluster nodes communicate internally using a binary message format over TCP connections, this is referred to as the "Transport layer"

Cross-cluster [replication](https://www.elastic.co/guide/en/elasticsearch/reference/current/xpack-ccr.html) (CCR)
and [search](https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-cross-cluster-search.html) (CCS) also use
transport messaging for inter-cluster communication.
More information on CCR/CCS can be found in the [Distributed architecture guide](./DistributedArchitectureGuide.md#cross-cluster-replication-ccr)

## REST Layer

### Handler registration

All REST handlers exposed by Elasticsearch are registered in [ActionModule#initRestHandlers]. This method registers all the
rest actions with the [RestController] using [#registerHandler(...)][RestController#registerHandler]. These registrations populate
a map of [routes][RestHandler#routes] to [RestHandler]s to allow routing of incoming HTTP requests to their respective handlers.
There are many built-in REST endpoints configured statically in [ActionModule][ActionModule#initRestHandlers], and additional
endpoints can be contributed by [ActionPlugin]s by implementing the [getRestHandlers] method.

Typically, REST actions follow the class naming convention `Rest*Action`, which makes them easier to find, but not always; the
[#routes()][RestHandler#routes] definition can also be helpful in finding a REST action.

When a [RestRequest] is received, [RestController#dispatchRequest] uses the request path to identify the destination handler and calls
[#handleRequest][RestHandler#handleRequest] on it. [BaseRestHandler] is a common base class extended by most `Rest*Action` implementations.

### Handler invocation

The usual flow of a REST request being handled is as follows
1. [RestController#dispatchRequest] inspects the [RestRequest] and matches it to a handler using its map of paths to handlers.
2. [BaseRestHandler#handleRequest] performs some basic parameter validation.
3. [BaseRestHandler] calls into [BaseRestHandler#prepareRequest], which `Rest*Action` subclasses implement to define the behavior
for a particular action. [prepareRequest][BaseRestHandler#prepareRequest] processes the request parameters to produce a
[RestChannelConsumer] that is ready to execute the action and return the response on a [RestChannel].
4. [BaseRestHandler] validates that the handler consumed all the request parameters, throwing an exception if any
were left unconsumed.
5. [BaseRestHandler] then supplies the channel to the [RestChannelConsumer] to begin executing the action. Some handlers, such as the
[RestBulkAction], consume the request as a stream of chunks to allow incremental processing of large requests.
6. The response is written to the [RestChannel], either as a [single payload][RestToXContentListener] or a
[stream of chunks][RestChunkedToXContentListener].

### Request interceptor

The [RestController] allows the configuration of an interceptor that determines which [RestRequest]s are allowed to be processed. A single
[RestServerActionPlugin] can provide a [RestInterceptor] implementation, through which all requests are passed. The
[Security][Security#getRestHandlerInterceptor] plugin uses this capability to authorize access to endpoints that require operator privileges
and do secondary authentication.

### HTTP server infrastructure

HTTP traffic is handled by an implementation of a [HttpServerTransport]. The [HttpServerTransport] is responsible for binding to a
port, handling REST client connections, parsing received requests into [RestRequest] instances and dispatching those
requests to a [HttpServerTransport.Dispatcher]. The [RestController] is an implementation of [HttpServerTransport.Dispatcher].
The [HttpServerTransport] is pluggable. There is only a single [Netty-based implementation][Netty4HttpServerTransport]
of [HttpServerTransport], but some plugins such as [Security][Security#getHttpTransports] supply instances of it with
additional configuration to implement features such as IP filtering or TLS.

[ActionModule#initRestHandlers]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/ActionModule.java#L814
[ActionModule]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/ActionModule.java
[ActionPlugin]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/plugins/ActionPlugin.java
[BaseRestHandler#handleRequest]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/BaseRestHandler.java#L79
[BaseRestHandler#prepareRequest]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/BaseRestHandler.java#L247
[BaseRestHandler]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/BaseRestHandler.java
[HttpServerTransport.Dispatcher]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/http/HttpServerTransport.java#L36
[HttpServerTransport]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/http/HttpServerTransport.java
[Netty4HttpServerTransport]:https://github.com/elastic/elasticsearch/blob/v9.0.1/modules/transport-netty4/src/main/java/org/elasticsearch/http/netty4/Netty4HttpServerTransport.java
[RestBulkAction]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/action/document/RestBulkAction.java
[RestChannelConsumer]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/BaseRestHandler.java#L204
[RestChannel]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/RestChannel.java
[RestChunkedToXContentListener]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/action/RestChunkedToXContentListener.java
[RestController#dispatchRequest]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/RestController.java#L304
[RestController#registerHandler]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/RestController.java#L299
[RestController]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/RestController.java
[RestHandler#handleRequest]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/RestHandler.java#L37
[RestHandler#routes]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/RestHandler.java#L75
[RestHandler]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/RestHandler.java
[RestInterceptor]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/RestInterceptor.java
[RestRequest]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/RestRequest.java
[RestServerActionPlugin]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/plugins/interceptor/RestServerActionPlugin.java
[RestToXContentListener]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/action/RestToXContentListener.java
[Route]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/rest/RestHandler.java#L123
[Security#getHttpTransports]:https://github.com/elastic/elasticsearch/blob/v9.0.1/x-pack/plugin/security/src/main/java/org/elasticsearch/xpack/security/Security.java#L1959
[Security#getRestHandlerInterceptor]:https://github.com/elastic/elasticsearch/blob/v9.0.1/x-pack/plugin/security/src/main/java/org/elasticsearch/xpack/security/Security.java#L2140
[TransportAction]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/support/TransportAction.java
[getRestHandlers]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/plugins/ActionPlugin.java#L76

## Transport Layer

`Rest*Action` implementations typically translate received requests into an [ActionRequest] which is dispatched via the [NodeClient]
passed in by the [RestController]. The [NodeClient] is the entrypoint into the "transport layer" over which internal cluster actions
are coordinated.

> [!NOTE]
> `Rest*Action` classes usually have a corresponding `Transport*Action`, this naming convention makes it easy to locate the corresponding
> [RestHandler] for a [TransportAction]. (e.g. `RestGetAction` calls `TransportGetAction`). There are actions for which this pattern
> does not hold, in those cases you can locate the transport action for a REST action by looking at the `NodeClient` invocation in the
> `Rest*Action`'s `prepareRequest` implementation, it should specify the `ActionType` being invoked which can then be used to locate
> the `Transport*Action` class that handles it.

### Action registration
Elasticsearch contains many built-in [TransportAction]s, configured statically in [ActionModule#setupActions]. [ActionPlugin]s can
contribute additional actions via the [getActions][ActionPlugin#getActions] method. [TransportAction]s define the request and response
types used to invoke the action and the logic for performing the action.

[TransportAction]s that are registered in [ActionModule#setupActions] (including those supplied by plugins) are locally bound to their
[ActionType]. This map of `type -> action` bindings is what [NodeClient] instances use to locate actions in [NodeClient#executeLocally].

The actions themselves sometimes dispatch downstream actions to other nodes in the cluster via the transport layer (see
[TransportService#sendRequest]). To be callable in this way, actions must register themselves with the [TransportService] by calling
[TransportService#registerRequestHandler]. [HandledTransportAction] is a common parent class that registers an action with the
[TransportService].

> [!NOTE]
> The name [TransportAction] can be misleading, as it suggests they are all invoke-able and invoked via the TCP transport. In fact,
> a majority of transport actions are only ever invoked locally via the [NodeClient]. The two key features of a [TransportAction] are:
> - Their constructor parameters are provided via dependency injection at runtime rather than direct instantiation.
> - They represent a security boundary; we check that the calling user is authorized to call the action they're calling using action
>   interceptors, which are described below.

### Action invocation
The [NodeClient] executes all actions locally on the invoking node using the [NodeClient#executeLocally] method. This method invokes
[TaskManager#registerAndExecute] to register a task, execute the action, then unregister the task once the action completes.
There is more information about task management in the [Distributed architecture guide](./DistributedArchitectureGuide.md#task-management--tracking)

There are a few common patterns for [TransportAction] execution that are present in the codebase. Some prominent examples include...

- [TransportMasterNodeAction]: Executes an action on the master node. Typically used to perform cluster state updates, as these can only
be performed on the master. The base class contains logic for locating the master node and delegating to it to execute the specified logic.
- [TransportNodesAction]: Executes an action on many nodes then collates the responses.
- [TransportLocalClusterStateAction]: Waits for a cluster state that optionally meets some criteria and performs a read action on it on the
coordinating node.
- [TransportReplicationAction]: Execute an action on a primary shard followed by all replicas that exist for that shard. The base class
implements logic for locating the primary and replica shards in the cluster and delegating to the relevant nodes. Often used for index
updates in stateful Elasticsearch.
- [TransportSingleShardAction]: Executes a read operation on a specific shard, the base class contains logic for locating an available copy
of the nominated shard and delegating to the relevant node to execute the action. On a failure, the action is retried on a different copy.

### Action interceptors

The transport action infrastructure allows the configuration of interceptors which can implement cross-cutting concerns like security around
action invocations. Implementations of [TransportInterceptor] interface are able to intercept action requests by wrapping
[TransportRequestHandler]s, or by intercepting requests before they are sent. Plugins that implement the [NetworkPlugin] interface are able
to register interceptors by implementing the [getTransportInterceptors][NetworkPlugin#getTransportInterceptors] method.

### Transport infrastructure

The transport infrastructure is pluggable and implementations can be provided by [NetworkPlugin#getTransports]. The role of the [Transport]
is to establish connections between nodes over which [TransportRequest]s can be sent, maintain a registry of [TransportRequestHandler]s for
routing inbound requests and maintain state to correlate inbound responses with the original requests. There is a single Netty-based TCP
transport used in production Elasticsearch, the [Netty4Transport], but the security plugin extends that to add SSL and IP filtering
capabilities.

[ActionModule#setupActions]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/ActionModule.java#L600
[ActionPlugin#getActions]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/plugins/ActionPlugin.java#L55
[ActionRequest]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/ActionRequest.java
[ActionType]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/ActionType.java
[HandledTransportAction]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/support/HandledTransportAction.java
[Netty4Transport]:https://github.com/elastic/elasticsearch/blob/v9.0.1/modules/transport-netty4/src/main/java/org/elasticsearch/transport/netty4/Netty4Transport.java
[NetworkPlugin#getTransportInterceptors]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/plugins/NetworkPlugin.java#L47
[NetworkPlugin#getTransports]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/plugins/NetworkPlugin.java#L58
[NetworkPlugin]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/plugins/NetworkPlugin.java
[NodeClient#executeLocally]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/client/internal/node/NodeClient.java#L101
[NodeClient]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/client/internal/node/NodeClient.java
[TaskManager#registerAndExecute]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/tasks/TaskManager.java#L175
[TransportInterceptor]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/transport/TransportInterceptor.java
[TransportLocalClusterStateAction]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/support/local/TransportLocalClusterStateAction.java
[TransportMasterNodeAction]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/support/master/TransportMasterNodeAction.java
[TransportNodesAction]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/support/nodes/TransportNodesAction.java
[TransportReplicationAction]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/support/replication/TransportReplicationAction.java
[TransportRequestHandler]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/transport/TransportRequestHandler.java
[TransportRequest]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/transport/TransportRequest.java
[TransportService#registerRequestHandler]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/transport/TransportService.java#L1208
[TransportService#sendRequest]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/transport/TransportService.java#L769
[TransportService]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/transport/TransportService.java
[TransportSingleShardAction]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/action/support/single/shard/TransportSingleShardAction.java
[Transport]:https://github.com/elastic/elasticsearch/blob/v9.0.1/server/src/main/java/org/elasticsearch/transport/Transport.java

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
