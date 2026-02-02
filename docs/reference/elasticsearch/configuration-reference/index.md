---
navigation_title: "Configuration"
applies_to:
  stack:
---

# Elasticsearch configuration reference

Configuration settings enable you to customize the behavior of {{es}} features.
This reference provides details about each setting, such as its purpose, default behavior, and availability in various deployment contexts.

To learn how to update these settings on your cluster, including on ECH, ECE, ECK, and self-managed deployments, refer to [Elastic Stack settings](docs-content://deploy-manage/stack-settings.md).

This section provides detailed **reference information** for {{es}} configuration. Refer to the [Deploy and manage](docs-content://deploy-manage/index.md) section to get started with deploying and configuring {{es}}, and to learn when and how to use some of these settings.

:::{admonition} Serverless manages these settings for you
In {{serverless-full}}, cluster-level settings and node-level settings are not required by end users, and are fully managed by Elastic.

Certain [project settings](docs-content://deploy-manage/deploy/elastic-cloud/project-settings.md) allow you to customize your project’s performance and general data retention, and enable or disable project features.
:::

For information about index-level settings, refer to [](/reference/elasticsearch/index-settings/index.md).

The settings are grouped by feature or purpose, for example:

- [Auditing](/reference/elasticsearch/configuration-reference/auding-settings.md)
- [Circuit breaker](/reference/elasticsearch/configuration-reference/circuit-breaker-settings.md)
- [Cluster formation and discovery](/reference/elasticsearch/configuration-reference/discovery-cluster-formation-settings.md)
- [Cross-cluster replication](/reference/elasticsearch/configuration-reference/cross-cluster-replication-settings.md)
- [Data stream lifecycle](/reference/elasticsearch/configuration-reference/data-stream-lifecycle-settings.md)
- [Enrich settings](/reference/elasticsearch/configuration-reference/enrich-settings.md)
- [Field data cache](/reference/elasticsearch/configuration-reference/field-data-cache-settings.md)
- [Health diagnostic](/reference/elasticsearch/configuration-reference/health-diagnostic-settings.md)
- [Index lifecycle management](/reference/elasticsearch/configuration-reference/index-lifecycle-management-settings.md), 
- [Index management](/reference/elasticsearch/configuration-reference/index-management-settings.md)
- [Index recovery](/reference/elasticsearch/configuration-reference/index-recovery-settings.md)
- [Index buffer](/reference/elasticsearch/configuration-reference/indexing-buffer-settings.md)
- [Inference](/reference/elasticsearch/configuration-reference/inference-settings.md)
- [License](/reference/elasticsearch/configuration-reference/license-settings.md)
- [Local gateway](/reference/elasticsearch/configuration-reference/local-gateway.md)
- [Machine learning](/reference/elasticsearch/configuration-reference/machine-learning-settings.md)
- [Monitoring](/reference/elasticsearch/configuration-reference/monitoring-settings.md)
- [Networking](/reference/elasticsearch/configuration-reference/networking-settings.md)
- [Nodes](/reference/elasticsearch/configuration-reference/node-settings.md)
- [Node query cache](/reference/elasticsearch/configuration-reference/node-query-cache-settings.md)
- [Remote clusters](/reference/elasticsearch/configuration-reference/remote-clusters.md)
- [Search](/reference/elasticsearch/configuration-reference/search-settings.md)
- [Security](/reference/elasticsearch/configuration-reference/security-settings.md)
- [Shard request cache](/reference/elasticsearch/configuration-reference/shard-request-cache-settings.md)
- [Shard routing](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md)
- [Snapshot and restore](/reference/elasticsearch/configuration-reference/snapshot-restore-settings.md)
- [Transforms](/reference/elasticsearch/configuration-reference/transforms-settings.md)
- [Thread pools](/reference/elasticsearch/configuration-reference/thread-pool-settings.md)
- [Watcher](/reference/elasticsearch/configuration-reference/watcher-settings.md)
