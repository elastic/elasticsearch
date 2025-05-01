# General Architecture

# REST and Transport Layers

### REST Layer

The REST and Transport layers are bound together through the `ActionModule`. `ActionModule#initRestHandlers` registers all the
rest actions with a `RestController` that matches incoming requests to particular REST actions. `RestController#registerHandler`
uses each `Rest*Action`'s `#routes()` implementation to match HTTP requests to that particular `Rest*Action`. Typically, REST
actions follow the class naming convention `Rest*Action`, which makes them easier to find, but not always; the `#routes()`
definition can also be helpful in finding a REST action. `RestController#dispatchRequest` eventually calls `#handleRequest` on a
`RestHandler` implementation. `RestHandler` is the base class for `BaseRestHandler`, which most `Rest*Action` instances extend to
implement a particular REST action.

`BaseRestHandler#handleRequest` calls into `BaseRestHandler#prepareRequest`, which children `Rest*Action` classes extend to
define the behavior for a particular action. `RestController#dispatchRequest` passes a `RestChannel` to the `Rest*Action` via
`RestHandler#handleRequest`: `Rest*Action#prepareRequest` implementations return a `RestChannelConsumer` defining how to execute
the action and reply on the channel (usually in the form of completing an ActionListener wrapper). `Rest*Action#prepareRequest`
implementations are responsible for parsing the incoming request, and verifying that the structure of the request is valid.
`BaseRestHandler#handleRequest` will then check that all the request parameters have been consumed: unexpected request parameters
result in an error.

### How REST Actions Connect to Transport Actions

The Rest layer uses an implementation of `AbstractClient`. `BaseRestHandler#prepareRequest` takes a `NodeClient`: this client
knows how to connect to a specified TransportAction. A `Rest*Action` implementation will return a `RestChannelConsumer` that
most often invokes a call into a method on the `NodeClient` to pass through to the TransportAction. Along the way from
`BaseRestHandler#prepareRequest` through the `AbstractClient` and `NodeClient` code, `NodeClient#executeLocally` is called: this
method calls into `TaskManager#registerAndExecute`, registering the operation with the `TaskManager` so it can be found in Task
API requests, before moving on to execute the specified TransportAction.

`NodeClient` has a `NodeClient#actions` map from `ActionType` to `TransportAction`. `ActionModule#setupActions` registers all the
core TransportActions, as well as those defined in any plugins that are being used: plugins can override `Plugin#getActions()` to
define additional TransportActions. Note that not all TransportActions will be mapped back to a REST action: many TransportActions
are only used for internode operations/communications.

### Transport Layer

(Managed by the TransportService, TransportActions must be registered there, too)

(Executing a TransportAction (either locally via NodeClient or remotely via TransportService) is where most of the authorization & other security logic runs)

(What actions, and why, are registered in TransportService but not NodeClient?)

### Direct Node to Node Transport Layer

(TransportService maps incoming requests to TransportActions)

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
