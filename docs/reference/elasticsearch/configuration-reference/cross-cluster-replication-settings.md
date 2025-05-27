---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ccr-settings.html
applies_to:
  deployment:
    self:
---

# Cross-cluster replication settings [ccr-settings]

These {{ccr}} settings can be dynamically updated on a live cluster with the [cluster update settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings).


## Remote recovery settings [ccr-recovery-settings]

The following setting can be used to rate-limit the data transmitted during [remote recoveries](docs-content://deploy-manage/tools/cross-cluster-replication.md#ccr-remote-recovery):

`ccr.indices.recovery.max_bytes_per_sec` ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting))
:   Limits the total inbound and outbound remote recovery traffic on each node. Since this limit applies on each node, but there may be many nodes performing remote recoveries concurrently, the total amount of remote recovery bytes may be much higher than this limit. If you set this limit too high then there is a risk that ongoing remote recoveries will consume an excess of bandwidth (or other resources) which could destabilize the cluster. This setting is used by both the leader and follower clusters. For example if it is set to `20mb` on a leader, the leader will only send `20mb/s` to the follower even if the follower is requesting and can accept `60mb/s`. Defaults to `40mb`.


## Advanced remote recovery settings [ccr-advanced-recovery-settings]

The following *expert* settings can be set to manage the resources consumed by remote recoveries:

`ccr.indices.recovery.max_concurrent_file_chunks` ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting))
:   Controls the number of file chunk requests that can be sent in parallel per recovery. As multiple remote recoveries might already running in parallel, increasing this expert-level setting might only help in situations where remote recovery of a single shard is not reaching the total inbound and outbound remote recovery traffic as configured by `ccr.indices.recovery.max_bytes_per_sec`. Defaults to `5`. The maximum allowed value is `10`.

`ccr.indices.recovery.chunk_size`([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting))
:   Controls the chunk size requested by the follower during file transfer. Defaults to `1mb`.

`ccr.indices.recovery.recovery_activity_timeout`([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting))
:   Controls the timeout for recovery activity. This timeout primarily applies on the leader cluster. The leader cluster must open resources in-memory to supply data to the follower during the recovery process. If the leader does not receive recovery requests from the follower for this period of time, it will close the resources. Defaults to 60 seconds.

`ccr.indices.recovery.internal_action_timeout` ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting))
:   Controls the timeout for individual network requests during the remote recovery process. An individual action timing out can fail the recovery. Defaults to 60 seconds.

