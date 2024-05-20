# General Architecture

## Transport Actions

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

## Plugins

(what warrants a plugin?)

(what plugins do we have?)

## Testing

(Overview of our testing frameworks. Discuss base test classes.)

### Unit Testing

### REST Testing

### Integration Testing
