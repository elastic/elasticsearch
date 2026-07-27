---
navigation_title: "Data Federation"
description: "Query data stored in external cloud storage using ES|QL without ingesting it into Elasticsearch."
applies_to:
  stack: experimental =9.5
  serverless: experimental
products:
  - id: elasticsearch
---

# {{esql}} Data Federation

::::{admonition} Requirements
:applies_to: { ess:, ece:, eck:, self: }
For {{ech}}, {{ece}}, and {{eck}} deployments or self-managed clusters, ES|QL Data Federation requires an [Enterprise subscription](https://www.elastic.co/subscriptions).
::::

Query data stored in compatible external storage systems using the same {{esql}} syntax you use for native indices, without any ingestion into {{es}}.

[Overview](esql-data-federation-overview.md)
: What federated data is, why you would use it, and what data source types and file formats are supported.

[Quickstart](esql-data-federation-quickstart.md)
: Register a data source, create a dataset, and run your first query against external data.

[Connect data sources](esql-data-federation-sources.md)
: Connect to external storage, configure S3 settings, and set up authentication.

[Add datasets](esql-data-federation-datasets.md)
: Select which files to query, configure format settings, and control schema inference.

[Query datasets](esql-data-federation-querying.md)
: Learn how the engine reduces storage reads, review current limitations, and troubleshoot common issues.

[Manage access](esql-data-federation-security.md)
: Control access to data sources and datasets, encrypt credentials, and configure privileges.

[Cluster settings](esql-data-federation-cluster-settings.md)
: Tune object limits, control request concurrency, and adjust file discovery and caching behavior.
