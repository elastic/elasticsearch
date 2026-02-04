---
navigation_title: "Scalability"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-scalability.html
---

# Connector Scalability [es-connectors-scalability]



## Redundancy [es-connectors-scalability-redundancy]

Users can create a backup (secondary) server with an identical connector service setup (settings, code, etc..). If the primary server running the connector service fails, users can start up the connector service on the secondary server and restart the sync jobs. Because connector definitions and job status information are all stored in Elasticsearch, there is no risk of data loss or corruption when switching servers.

However, note that any in-progress syncs will need to be restarted from scratch, and cannot be resumed where they were interrupted from.


## Failover [es-connectors-scalability-failover]

There is currently no automatic failover or transfer of workload in case of failure. If the server running the connector service fails, all outstanding connector sync jobs will go into a suspended state. When the connector service returns (or if a replacement connector service is launched), it will identify any jobs that need to be cleaned up and automatically restart (from scratch) the suspended jobs.


## Workload balancing [es-connectors-scalability-balancing]

There is currently no division/balancing of workload for a single sync job across multiple connector service deployments. Once a sync job is claimed by a connector service, it will run the job to completion - unless the connector service instance fails. In that case, another connector service instance will pick up the suspended job and restart it (from scratch).

In 8.8.0+, the Connector Service provides concurrency control when there are multiple connector services connected to the same Elasticsearch cluster, with the following expectations:

* Multiple sync jobs can be scheduled for a given search index but only 1 sync job can be executed for a search index at any single time.
* Each sync job can only be claimed by 1 connector service.
* Only 1 connector service can perform management tasks at a time, for example: populating service types and configurations, validating filters, etc.


## Horizontal Scalability [es-connectors-scalability-horizontal]

Horizontal scaling can work if there are multiple connector services running and are configured to allow concurrent syncs via their `service.max_concurrent_syncs` settings.

Hypothetically, multiple Connector Services would naturally load balance to some extent even though we do not currently have explicit load balancing functionality.

