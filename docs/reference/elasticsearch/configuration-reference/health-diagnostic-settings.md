---
navigation_title: "Health Diagnostic settings"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/health-diagnostic-settings.html
applies_to:
  deployment:
    self:
---

# Health diagnostic settings in {{es}} [health-diagnostic-settings]


The following are the *expert-level* settings available for configuring an internal diagnostics service. The output of this service is currently exposed through the Health API [Health API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-health-report). It is not recommended to change any of these from their default values.

## Cluster level settings [_cluster_level_settings_2]

`health.master_history.has_master_lookup_timeframe`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The amount of time a node looks back to see if it has observed a master at all, before moving on with other checks. Defaults to `30s` (30 seconds).

`master_history.max_age`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The timeframe we record the master history to be used for diagnosing the cluster health. Master node changes older than this time will not be considered when diagnosing the cluster health. Defaults to `30m` (30 minutes).

`health.master_history.identity_changes_threshold`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The number of master identity changes witnessed by a node that indicates the cluster is not healthy. Defaults to `4`.

`health.master_history.no_master_transitions_threshold`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) The number of transitions to no master witnessed by a node that indicates the cluster is not healthy. Defaults to `4`.

`health.node.enabled`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Enables the health node, which allows the health API to provide indications about cluster wide health aspects such as disk space.

`health.reporting.local.monitor.interval`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Determines the interval in which each node of the cluster monitors aspects that comprise its local health such as its disk usage.

`health.ilm.max_time_on_action`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The minimum amount of time an index has to be in an {{ilm}} ({{ilm-init}}) action before it is considered stagnant. Defaults to `1d` (1 day).

`health.ilm.max_time_on_step`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The minimum amount of time an index has to be in an {{ilm-init}} step before it is considered stagnant. Defaults to `1d` (1 day).

`health.ilm.max_retries_per_step`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The minimum amount of times an index has retried by an {{ilm-init}} step before it is considered stagnant. Defaults to `100`

`health.periodic_logger.enabled`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) Enables the health periodic logger, which logs the health statuses of each health indicator along with the top level one as observed by the Health API. Defaults to `false`.

`health.periodic_logger.poll_interval`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting), [time unit value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) How often {{es}} logs the health status of the cluster and of each health indicator as observed by the Health API. Defaults to `60s` (60 seconds).


