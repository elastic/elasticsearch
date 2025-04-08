---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-gateway.html
applies_to:
  deployment:
    self:
---

# Local gateway [modules-gateway]

$$$dangling-indices$$$
The local gateway stores the cluster state and shard data across full cluster restarts.

The following *static* settings, which must be set on every [master-eligible node](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#master-node-role), control how long a freshly elected master should wait before it tries to recover the [cluster state](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-state) and the cluster’s data.

::::{note}
These settings only take effect during a [full cluster restart](docs-content://deploy-manage/maintenance/start-stop-services/full-cluster-restart-rolling-restart-procedures.md#restart-cluster-full).
::::


`gateway.expected_data_nodes`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Number of data nodes expected in the cluster. Recovery of local shards begins when the expected number of data nodes join the cluster. Defaults to `0`.

`gateway.recover_after_time`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) If the expected number of nodes is not achieved, the recovery process waits for the configured amount of time before trying to recover. Defaults to `5m`.

    Once the `recover_after_time` duration has timed out, recovery will start as long as the following condition is met:


`gateway.recover_after_data_nodes`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Recover as long as this many data nodes have joined the cluster.

These settings can be configured in `elasticsearch.yml` as follows:

```yaml
gateway.expected_data_nodes: 3
gateway.recover_after_time: 600s
gateway.recover_after_data_nodes: 3
```

