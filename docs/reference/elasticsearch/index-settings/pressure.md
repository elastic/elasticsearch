---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-indexing-pressure.html
navigation_title: Indexing pressure
---

# Indexing pressure settings [index-modules-indexing-pressure]

Indexing documents into {{es}} introduces system load in the form of memory and CPU load. Each indexing operation includes coordinating, primary, and replica stages. These stages can be performed across multiple nodes in a cluster.

Indexing pressure can build up through external operations, such as indexing requests, or internal mechanisms, such as recoveries and {{ccr}}. If too much indexing work is introduced into the system, the cluster can become saturated. This can adversely impact other operations, such as search, cluster coordination, and background processing.

To prevent these issues, {{es}} internally monitors indexing load. When the load exceeds certain limits, new indexing work is rejected


## Indexing stages [indexing-stages]

External indexing operations go through three stages: coordinating, primary, and replica. See [Basic write model](docs-content://deploy-manage/distributed-architecture/reading-and-writing-documents.md#basic-write-model).


## Memory limits [memory-limits]

The `indexing_pressure.memory.limit` node setting restricts the number of bytes available for outstanding indexing requests. This setting defaults to 10% of the heap.

At the beginning of each indexing stage, {{es}} accounts for the bytes consumed by an indexing request. This accounting is only released at the end of the indexing stage. This means that upstream stages will account for the request overheard until all downstream stages are complete. For example, the coordinating request will remain accounted for until primary and replica stages are complete. The primary request will remain accounted for until each in-sync replica has responded to enable replica retries if necessary.

A node will start rejecting new indexing work at the coordinating or primary stage when the number of outstanding coordinating, primary, and replica indexing bytes exceeds the configured limit.

A node will start rejecting new indexing work at the replica stage when the number of outstanding replica indexing bytes exceeds 1.5x the configured limit. This design means that as indexing pressure builds on nodes, they will naturally stop accepting coordinating and primary work in favor of outstanding replica work.

The `indexing_pressure.memory.limit` setting’s 10% default limit is generously sized. You should only change it after careful consideration. Only indexing requests contribute to this limit. This means there is additional indexing overhead (buffers, listeners, etc) which also require heap space. Other components of {{es}} also require memory. Setting this limit too high can deny operating memory to other operations and components.


## Monitoring [indexing-pressure-monitoring]

You can use the [node stats API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-stats) to retrieve indexing pressure metrics.


## Indexing pressure settings [indexing-pressure-settings]

`indexing_pressure.memory.limit` ![logo cloud](https://doc-icons.s3.us-east-2.amazonaws.com/logo_cloud.svg "Supported on {{ess}}")
:   Number of outstanding bytes that may be consumed by indexing requests. When this limit is reached or exceeded, the node will reject new coordinating and primary operations. When replica operations consume 1.5x this limit, the node will reject new replica operations. Defaults to 10% of the heap.

