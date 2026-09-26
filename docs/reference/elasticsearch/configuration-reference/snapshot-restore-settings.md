---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshot-settings.html
applies_to:
  deployment:
    ess:
    self:
---

# Snapshot and restore settings [snapshot-settings]

The following cluster settings configure [snapshot and restore](docs-content://deploy-manage/tools/snapshot-and-restore.md) and [snapshot lifecycle management (SLM)](docs-content://deploy-manage/tools/snapshot-and-restore/create-snapshots.md#automate-snapshots-slm).

:::{tip}
This page covers general snapshot and restore settings. For client connection and per-repository settings specific to each repository type, refer to:

- [S3 repository settings](/reference/elasticsearch/configuration-reference/s3-repository-settings.md)
- [Azure repository settings](/reference/elasticsearch/configuration-reference/azure-repository-settings.md)
- [GCS repository settings](/reference/elasticsearch/configuration-reference/gcs-repository-settings.md)
- [Shared file system repository settings](/reference/elasticsearch/configuration-reference/fs-repository-settings.md)
- [Read-only URL repository settings](/reference/elasticsearch/configuration-reference/url-repository-settings.md)
- [Source-only repository settings](/reference/elasticsearch/configuration-reference/source-repository-settings.md)
- [Hadoop HDFS repository](/reference/elasticsearch-plugins/repository-hdfs.md)
:::

$$$snapshot-max-concurrent-ops$$$

`snapshot.max_concurrent_operations`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), integer) Maximum number of concurrent snapshot operations. Defaults to `1000`.

    This limit applies in total to all ongoing snapshot creation, cloning, and deletion operations. {{es}} will reject any operations that would exceed this limit.

$$$repositories-default-repository$$$

`repositories.default_repository` {applies_to}`stack: ga 9.4`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), string) A default repository to be used for snapshot and restore operations. Currently, this is only used for DLM Frozen Tier transitions but may be used by other features in the future. By default, this setting is unset, in which case DLM frozen tier transitions will not occur.

    The repository must already be registered before it can be set as the default, and a read-only repository cannot be set as the default. A repository that is currently configured as the default cannot be deleted or marked as read-only until you change the setting to a different repository or set it to an empty string.


## {{slm-init}} settings [_slm_init_settings]

The following cluster settings configure [{{slm}} ({{slm-init}})](docs-content://deploy-manage/tools/snapshot-and-restore/create-snapshots.md#automate-snapshots-slm).

$$$slm-history-index-enabled$$$

`slm.history_index_enabled`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), Boolean) Controls whether {{slm-init}} records the history of actions taken as part of {{slm-init}} policies to the `slm-history-*` indices. Defaults to `true`.

$$$slm-retention-schedule$$$

`slm.retention_schedule`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), [cron scheduler value](docs-content://explore-analyze/alerts-cases/watcher/schedule-types.md#schedule-cron)) Controls when the [retention task](docs-content://deploy-manage/tools/snapshot-and-restore/create-snapshots.md#slm-retention-task) runs. Can be a periodic or absolute time schedule. Supports all values supported by the [cron scheduler](docs-content://explore-analyze/alerts-cases/watcher/schedule-types.md#schedule-cron). Defaults to daily at 1:30am UTC: `0 30 1 * * ?`.

$$$slm-retention-duration$$$

`slm.retention_duration`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Limits how long {{slm-init}} should spend deleting old snapshots. Defaults to one hour: `1h`.

$$$slm-health-failed-snapshot-warn-threshold$$$

`slm.health.failed_snapshot_warn_threshold`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), Long) The number of failed invocations since last successful snapshot that indicate a problem with the policy in the health api. Defaults to a health api warning after five repeated failures: `5L`.
