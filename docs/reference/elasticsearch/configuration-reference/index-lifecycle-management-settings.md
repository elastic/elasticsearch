---
navigation_title: "{{ilm-cap}} settings"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ilm-settings.html
applies_to:
  deployment:
    self:
---

# {{ilm-cap}} settings in {{es}} [ilm-settings]


These are the settings available for configuring [{{ilm}}](docs-content://manage-data/lifecycle/index-lifecycle-management.md) ({{ilm-init}}).

## Cluster level settings [_cluster_level_settings_3]

`xpack.ilm.enabled`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), Boolean)

    :::{admonition} Deprecated in 7.8.0
    This deprecated setting has no effect and will be removed in Elasticsearch 8.0.
    :::

`indices.lifecycle.history_index_enabled`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), Boolean) Whether ILM’s history index is enabled. If enabled, ILM will record the history of actions taken as part of ILM policies to the `ilm-history-*` indices. Defaults to `true`.

$$$indices-lifecycle-poll-interval$$$

`indices.lifecycle.poll_interval`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), [time unit value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) How often {{ilm}} checks for indices that meet policy criteria. Defaults to `10m`.

$$$indices-lifecycle-rollover-only-if-has-documents$$$

`indices.lifecycle.rollover.only_if_has_documents`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), Boolean) Whether ILM will only roll over non-empty indices. If enabled, ILM will only roll over indices as long as they contain at least one document. Defaults to `true`.


## Index level settings [_index_level_settings_2]

These index-level {{ilm-init}} settings are typically configured through index templates. For more information, see [Create a lifecycle policy](docs-content://manage-data/lifecycle/index-lifecycle-management/tutorial-automate-rollover.md#ilm-gs-create-policy).

`index.lifecycle.indexing_complete`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), Boolean) Indicates whether or not the index has been rolled over. Automatically set to `true` when {{ilm-init}} completes the rollover action. You can explicitly set it to [skip rollover](docs-content://manage-data/lifecycle/index-lifecycle-management/skip-rollover.md). Defaults to `false`.

$$$index-lifecycle-name$$$

`index.lifecycle.name`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), string) The name of the policy to use to manage the index. For information about how {{es}} applies policy changes, see [Policy updates](docs-content://manage-data/lifecycle/index-lifecycle-management/policy-updates.md). If you are restoring an index from snapshot that was previously managed by {{ilm}}, you can override this setting to null during the restore operation to disable further management of the index. See also [Index level settings](#index-lifecycle-rollover-alias).

$$$index-lifecycle-origination-date$$$

`index.lifecycle.origination_date`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), long) If specified, this is the timestamp used to calculate the index age for its phase transitions. Use this setting if you create a new index that contains old data and want to use the original creation date to calculate the index age. Specified as a Unix epoch value in milliseconds.

$$$index-lifecycle-parse-origination-date$$$

`index.lifecycle.parse_origination_date`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), Boolean) Set to `true` to parse the origination date from the index name. This origination date is used to calculate the index age for its phase transitions. The index name must match the pattern `^.*-{{date_format}}-\\d+`, where the `date_format` is `yyyy.MM.dd` and the trailing digits are optional. An index that was rolled over would normally match the full format, for example `logs-2016.10.31-000002`). If the index name doesn’t match the pattern, index creation fails.

$$$index-lifecycle-step-wait-time-threshold$$$

`index.lifecycle.step.wait_time_threshold`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Time to wait for the cluster to resolve allocation issues during an {{ilm-init}} [`shrink`](/reference/elasticsearch/index-lifecycle-actions/ilm-shrink.md) action. Must be greater than `1h` (1 hour). Defaults to `12h` (12 hours). See [Shard allocation for shrink](/reference/elasticsearch/index-lifecycle-actions/ilm-shrink.md#ilm-shrink-shard-allocation).

$$$index-lifecycle-rollover-alias$$$

`index.lifecycle.rollover_alias`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), string) The index alias to update when the index rolls over. Specify when using a policy that contains a rollover action. When the index rolls over, the alias is updated to reflect that the index is no longer the write index. For more information about rolling indices, see [Rollover](docs-content://manage-data/lifecycle/index-lifecycle-management/rollover.md). If you are restoring an index from snapshot that was previously managed by {{ilm}}, you can override this setting to null during the restore operation to disable further management of future indices. See also [Index level settings](#index-lifecycle-name).


