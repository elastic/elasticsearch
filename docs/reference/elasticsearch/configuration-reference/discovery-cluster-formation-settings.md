---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-discovery-settings.html
applies_to:
  deployment:
    self:
---

# Discovery and cluster formation settings [modules-discovery-settings]

[Discovery and cluster formation](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation.md) are affected by the following settings:

`discovery.seed_hosts`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Provides a list of the addresses of the master-eligible nodes in the cluster. May also be a single string containing the addresses separated by commas. Each address has the format `host:port` or `host`. The `host` is either a host name to be resolved by DNS, an IPv4 address, or an IPv6 address. IPv6 addresses must be enclosed in square brackets. If a host name resolves via DNS to multiple addresses, {{es}} uses all of them. DNS lookups are subject to [JVM DNS caching](docs-content://deploy-manage/deploy/self-managed/networkaddress-cache-ttl.md). If the `port` is not given then it is determined by checking the following settings in order:
    1. `transport.profiles.default.port`
    2. `transport.port`

    If neither of these is set then the default port is `9300`. The default value for `discovery.seed_hosts` is `["127.0.0.1", "[::1]"]`. See [`discovery.seed_hosts`](docs-content://deploy-manage/deploy/self-managed/important-settings-configuration.md#unicast.hosts).


`discovery.seed_providers`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies which types of [seed hosts provider](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation/discovery-hosts-providers.md#built-in-hosts-providers) to use to obtain the addresses of the seed nodes used to start the discovery process. By default, it is the [settings-based seed hosts provider](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation/discovery-hosts-providers.md#settings-based-hosts-provider) which obtains the seed node addresses from the `discovery.seed_hosts` setting.

`discovery.type`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies whether {{es}} should form a multiple-node cluster. Defaults to `multi-node`, which means that {{es}} discovers other nodes when forming a cluster and allows other nodes to join the cluster later. If set to `single-node`, {{es}} forms a single-node cluster and suppresses the timeout set by `cluster.publish.timeout`. For more information about when you might use this setting, see [Single-node discovery](docs-content://deploy-manage/deploy/self-managed/bootstrap-checks.md#single-node-discovery).

`cluster.initial_master_nodes`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets the initial set of master-eligible nodes in a brand-new cluster. By default this list is empty, meaning that this node expects to join a cluster that has already been bootstrapped. Remove this setting once the cluster has formed, and never set it again for this cluster. Do not configure this setting on master-ineligible nodes. Do not configure this setting on nodes joining an existing cluster. Do not configure this setting on nodes which are restarting. Do not configure this setting when performing a full-cluster restart. See [`cluster.initial_master_nodes`](docs-content://deploy-manage/deploy/self-managed/important-settings-configuration.md#initial_master_nodes).


## Expert settings [_expert_settings]

Discovery and cluster formation are also affected by the following *expert-level* settings, although it is not recommended to change any of these from their default values.

::::{warning}
If you adjust these settings then your cluster may not form correctly or may become unstable or intolerant of certain failures.
::::


`discovery.cluster_formation_warning_timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long a node will try to form a cluster before logging a warning that the cluster did not form. Defaults to `10s`. If a cluster has not formed after `discovery.cluster_formation_warning_timeout` has elapsed then the node will log a warning message that starts with the phrase `master not discovered` which describes the current state of the discovery process.

`discovery.find_peers_interval`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long a node will wait before attempting another discovery round. Defaults to `1s`.

`discovery.probe.connect_timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long to wait when attempting to connect to each address. Defaults to `30s`.

`discovery.probe.handshake_timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long to wait when attempting to identify the remote node via a handshake. Defaults to `30s`.

`discovery.request_peers_timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long a node will wait after asking its peers again before considering the request to have failed. Defaults to `3s`.

`discovery.find_peers_warning_timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long a node will attempt to discover its peers before it starts to log verbose messages describing why the connection attempts are failing. Defaults to `3m`.

`discovery.seed_resolver.max_concurrent_resolvers`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies how many concurrent DNS lookups to perform when resolving the addresses of seed nodes. Defaults to `10`.

`discovery.seed_resolver.timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Specifies how long to wait for each DNS lookup performed when resolving the addresses of seed nodes. Defaults to `5s`.

`cluster.auto_shrink_voting_configuration`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls whether the [voting configuration](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation/modules-discovery-voting.md) sheds departed nodes automatically, as long as it still contains at least 3 nodes. The default value is `true`. If set to `false`, the voting configuration never shrinks automatically and you must remove departed nodes manually with the [voting configuration exclusions API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-post-voting-config-exclusions).

$$$master-election-settings$$$`cluster.election.back_off_time`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets the amount to increase the upper bound on the wait before an election on each election failure. Note that this is *linear* backoff. This defaults to `100ms`. Changing this setting from the default may cause your cluster to fail to elect a master node.

`cluster.election.duration`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long each election is allowed to take before a node considers it to have failed and schedules a retry. This defaults to `500ms`. Changing this setting from the default may cause your cluster to fail to elect a master node.

`cluster.election.initial_timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets the upper bound on how long a node will wait initially, or after the elected master fails, before attempting its first election. This defaults to `100ms`. Changing this setting from the default may cause your cluster to fail to elect a master node.

`cluster.election.max_timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets the maximum upper bound on how long a node will wait before attempting an first election, so that an network partition that lasts for a long time does not result in excessively sparse elections. This defaults to `10s`. Changing this setting from the default may cause your cluster to fail to elect a master node.

$$$fault-detection-settings$$$`cluster.fault_detection.follower_check.interval`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long the elected master waits between follower checks to each other node in the cluster. Defaults to `1s`. Changing this setting from the default may cause your cluster to become unstable.

`cluster.fault_detection.follower_check.timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long the elected master waits for a response to a follower check before considering it to have failed. Defaults to `10s`. Changing this setting from the default may cause your cluster to become unstable.

`cluster.fault_detection.follower_check.retry_count`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how many consecutive follower check failures must occur to each node before the elected master considers that node to be faulty and removes it from the cluster. Defaults to `3`. Changing this setting from the default may cause your cluster to become unstable.

`cluster.fault_detection.leader_check.interval`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long each node waits between checks of the elected master. Defaults to `1s`. Changing this setting from the default may cause your cluster to become unstable.

`cluster.fault_detection.leader_check.timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long each node waits for a response to a leader check from the elected master before considering it to have failed. Defaults to `10s`. Changing this setting from the default may cause your cluster to become unstable.

`cluster.fault_detection.leader_check.retry_count`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how many consecutive leader check failures must occur before a node considers the elected master to be faulty and attempts to find or elect a new master. Defaults to `3`. Changing this setting from the default may cause your cluster to become unstable.

`cluster.follower_lag.timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long the master node waits to receive acknowledgements for cluster state updates from lagging nodes. The default value is `90s`. If a node does not successfully apply the cluster state update within this period of time, it is considered to have failed and is removed from the cluster. See [Publishing the cluster state](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation/cluster-state-overview.md#cluster-state-publishing).

`cluster.max_voting_config_exclusions`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Sets a limit on the number of voting configuration exclusions at any one time. The default value is `10`. See [*Add and remove nodes in your cluster*](docs-content://deploy-manage/maintenance/add-and-remove-elasticsearch-nodes.md).

`cluster.publish.info_timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long the master node waits for each cluster state update to be completely published to all nodes before logging a message indicating that some nodes are responding slowly. The default value is `10s`.

`cluster.publish.timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets how long the master node waits for each cluster state update to be completely published to all nodes, unless `discovery.type` is set to `single-node`. The default value is `30s`. See [Publishing the cluster state](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation/cluster-state-overview.md#cluster-state-publishing).

`cluster.discovery_configuration_check.interval`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Sets the interval of some checks that will log warnings about an incorrect discovery configuration. The default value is `30s`.

`cluster.join_validation.cache_timeout`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) When a node requests to join the cluster, the elected master node sends it a copy of a recent cluster state to detect certain problems which might prevent the new node from joining the cluster. The master caches the state it sends and uses the cached state if another node joins the cluster soon after. This setting controls how long the master waits until it clears this cache. Defaults to `60s`.

$$$no-master-block$$$

`cluster.no_master_block`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Specifies which operations are rejected when there is no active master in a cluster. This setting has three valid values:

    `all`
    :   All operations on the node (both read and write operations) are rejected. This also applies for API cluster state read or write operations, like the get index settings, update mapping, and cluster state API.

    `write`
    :   (default) Write operations are rejected. Read operations succeed, based on the last known cluster configuration. This situation may result in partial reads of stale data as this node may be isolated from the rest of the cluster.

    `metadata_write`
    :   Only metadata write operations (e.g. mapping updates, routing table changes) are rejected but regular indexing operations continue to work. Read and write operations succeed, based on the last known cluster configuration. This situation may result in partial reads of stale data as this node may be isolated from the rest of the cluster.

    ::::{note}
    * The `cluster.no_master_block` setting doesn’t apply to nodes-based APIs (for example, cluster stats, node info, and node stats APIs). Requests to these APIs are not be blocked and can run on any available node.
    * For the cluster to be fully operational, it must have an active master.

    ::::


`monitor.fs.health.enabled`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) If `true`, the node runs periodic [filesystem health checks](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation/cluster-fault-detection.md#cluster-fault-detection-filesystem-health). Defaults to `true`.

`monitor.fs.health.refresh_interval`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Interval between successive [filesystem health checks](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation/cluster-fault-detection.md#cluster-fault-detection-filesystem-health). Defaults to `2m`.

`monitor.fs.health.slow_path_logging_threshold`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) If a [filesystem health checks](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation/cluster-fault-detection.md#cluster-fault-detection-filesystem-health) takes longer than this threshold then {{es}} logs a warning. Defaults to `5s`.

