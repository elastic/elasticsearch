# General Architecture

## Transport Actions

## Serializations

## Settings

Elasticsearch supports [node-level configuration file settings][], [cluster-level settings][] and [index-level settings][].

### Declaring a Setting

[node-level configuration file settings]: https://www.elastic.co/guide/en/elasticsearch/reference/current/settings.html
[cluster-level settings]: https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html
[index-level settings]: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html

The [Setting][] class is the building block for Elasticsearch server settings. Each `Setting` can take multiple [Property][]
declarations to define setting characteristics, like allowing [dynamic updates][] while the node is running, or the scope of
the setting to apply at the individual [index-level][] vs [cluster-level][]. A [dynamic Setting][] is often paired with a
[local volatile variable][] and [ClusterSettings#initializeAndWatch()][] to catch and immediately apply dynamic changes.

[Setting]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/Setting.java#L57-L80
[Property]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/Setting.java#L82
[dynamic updates]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/Setting.java#L88-L91
[index-level]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/Setting.java#L124-L127
[cluster-level]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/Setting.java#L114-L117
[dynamic Setting]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/routing/allocation/allocator/BalancedShardsAllocator.java#L111-L117
[local volatile variable]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/routing/allocation/allocator/BalancedShardsAllocator.java#L123
[ClusterSettings#initializeAndWatch()]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/routing/allocation/allocator/BalancedShardsAllocator.java#L145

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

`AbstractScopedSettings` has various helper methods to track dynamic changes: it keeps a [registry of `SettingUpdater`][]
lambdas to run updates when settings are changed in the cluster state. The `ClusterApplierService` also [sends setting updates][]
through to the `AbstractScopedSettings` that invokes the registered consumers for each setting.

[registry of `SettingUpdater`]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/common/settings/AbstractScopedSettings.java#L379-L381
[sends setting updates]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/service/ClusterApplierService.java#L490-L494

[TransportClusterUpdateSettingsAction][] and [TransportUpdateSettingsAction][] (and the corresponding REST endpoints) allow users
to update cluster and index settings, respectively. Settings can be either persisted or transient depending on how they are tied
to [Metadata][] ([applied here][]) and [IndexMetadata][]. Changes to persisted settings will survive server restart; whereas a
change made to a transient setting will be reset to the default value if the server is ever restarted.

[TransportClusterUpdateSettingsAction]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/action/admin/cluster/settings/TransportClusterUpdateSettingsAction.java#L154-L160
[TransportUpdateSettingsAction]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/action/admin/indices/settings/put/TransportUpdateSettingsAction.java#L96-L101
[Metadata]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/metadata/Metadata.java#L212-L213
[applied here]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/metadata/Metadata.java#L2437
[IndexMetadata]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/metadata/IndexMetadata.java#L90

## Deprecations

## Plugins

(what warrants a plugin?)

(what plugins do we have?)

## Testing

(Overview of our testing frameworks. Discuss base test classes.)

### Unit Testing

### REST Testing

### Integration Testing
