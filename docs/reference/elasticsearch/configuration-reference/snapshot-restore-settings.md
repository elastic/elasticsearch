---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshot-settings.html
applies_to:
  deployment:
    ess:
    self:
---

# Snapshot and restore settings [snapshot-settings]

The following cluster settings configure [snapshot and restore](docs-content://deploy-manage/tools/snapshot-and-restore.md).

$$$snapshot-max-concurrent-ops$$$

`snapshot.max_concurrent_operations`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), integer) Maximum number of concurrent snapshot operations. Defaults to `1000`.

    This limit applies in total to all ongoing snapshot creation, cloning, and deletion operations. {{es}} will reject any operations that would exceed this limit.

`azure.client.CLIENT_NAME.endpoint_suffix` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   Allows providing the [endpoint_suffix client setting](docs-content://deploy-manage/tools/snapshot-and-restore/azure-repository.md#repository-azure-client-settings) for a non-internal Azure client used for snapshot/restore. Note that `CLIENT_NAME` should be replaced with the name of the created client.


## {{slm-init}} settings [_slm_init_settings]

The following cluster settings configure [{{slm}} ({{slm-init}})](docs-content://deploy-manage/tools/snapshot-and-restore/create-snapshots.md#automate-snapshots-slm).

$$$slm-history-index-enabled$$$

`slm.history_index_enabled`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), Boolean) Controls whether {{slm-init}} records the history of actions taken as part of {{slm-init}} policies to the `slm-history-*` indices. Defaults to `true`.

$$$slm-retention-schedule$$$

`slm.retention_schedule`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), [cron scheduler value](docs-content://explore-analyze/alerts-cases/watcher/schedule-types.md#schedule-cron)) Controls when the [retention task](docs-content://deploy-manage/tools/snapshot-and-restore/create-snapshots.md#slm-retention-task) runs. Can be a periodic or absolute time schedule. Supports all values supported by the [cron scheduler](docs-content://explore-analyze/alerts-cases/watcher/schedule-types.md#schedule-cron). Defaults to daily at 1:30am UTC: `0 30 1 * * ?`.

$$$slm-retention-duration$$$

`slm.retention_duration`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Limits how long {{slm-init}} should spend deleting old snapshots. Defaults to one hour: `1h`.

$$$slm-health-failed-snapshot-warn-threshold$$$

`slm.health.failed_snapshot_warn_threshold`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), Long) The number of failed invocations since last successful snapshot that indicate a problem with the policy in the health api. Defaults to a health api warning after five repeated failures: `5L`.

$$$repositories-url-allowed$$$

`repositories.url.allowed_urls` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on Elastic Cloud Hosted")
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Specifies the [read-only URL repositories](docs-content://deploy-manage/tools/snapshot-and-restore/read-only-url-repository.md) that snapshots can be restored from.


