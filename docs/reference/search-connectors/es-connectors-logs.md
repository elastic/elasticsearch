---
navigation_title: "Logs"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-logs.html
---

# Connector logs [es-connectors-logs]


This document describes logs for connectors.


## Enable logs [es-connectors-logs-enable]

### Self-managed connectors [es-connectors-logs-enable-self-managed]

Note that self-managed deployments and self-managed connector logs are written to `STDOUT`.

Self-managed connectors have the following logging options:

* Use the `service.log_level` setting in your connector service configuration file to specify the log level for the service.

    * Enable `elasticsearch.bulk.enable_operations_logging` to log the result of sending documents to Elasticsearch from connectors, for auditing and debugging. This setting depends on the `service.log_level` and will be logged at `DEBUG` level .

* Use the `elasticsearch.log_level` setting to specify the log level for the Elasticsearch *client* used by the connector service.


## View connector logs [es-connectors-logs-view]

You can view logs in Kibana.

You can filter by `service.type`:

* `enterprise-search`
* `connectors`


## Logs reference [es-connectors-logs-reference]

Logs use Elastic Common Schema (ECS), without extensions. See [the ECS Reference^](ecs://reference/index.md) for more information.

The fields logged are:

* `@timestamp`
* `log.level`
* `ecs.version`
* `labels.index_date`
* `tags`
* `log.logger`
* `service.type`
* `service.version`
* `process.name`
* `process.pid`
* `process.thread.id`

See [Logging^](docs-content://deploy-manage/monitor/logging-configuration/update-elasticsearch-logging-levels.md) in the Elasticsearch documentation for more information.

