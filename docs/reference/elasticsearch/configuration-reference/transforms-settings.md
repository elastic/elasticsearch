---
navigation_title: "{{transforms-cap}} settings"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/transform-settings.html
applies_to:
  deployment:
    self:
---

# {{transforms-cap}}  settings in Elasticsearch [transform-settings]


You do not need to configure any settings to use {{transforms}}. It is enabled by default.


## General {{transforms}} settings [general-transform-settings]

`node.roles: [ transform ]`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting)) Set `node.roles` to contain `transform` to identify the node as a *transform node*. If you want to run {{transforms}}, there must be at least one {{transform}} node in your cluster.

    If you set `node.roles`, you must explicitly specify all the required roles for the node. To learn more, refer to [Node settings](/reference/elasticsearch/configuration-reference/node-settings.md).

    ::::{important}
    It is strongly recommended that dedicated {{transform}} nodes also have the `remote_cluster_client` role; otherwise, {{ccs}} fails when used in {{transforms}}. See [Remote-eligible node](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#remote-node).
    ::::


`xpack.transform.enabled`
:   :::{admonition} Deprecated in 7.8.0
    This deprecated setting no longer has any effect.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting))

`xpack.transform.num_transform_failure_retries`
:   ([Dynamic](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#dynamic-cluster-setting)) The number of times that a {{transform}} retries when it experiences a non-fatal error. Once the number of retries is exhausted, the {{transform}} task is marked as `failed`. The default value is `10` with a valid minimum of `0` and maximum of `100`. If a {{transform}} is already running, it has to be restarted to use the changed setting. The `num_failure_retries` setting can also be specified on an individual {{transform}} level. Specifying this setting for each {{transform}} individually is recommended.

