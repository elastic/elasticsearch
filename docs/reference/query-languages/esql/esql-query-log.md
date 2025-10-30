---
applies_to:
  stack:
  serverless:
navigation_title: "Query log"
---

# {{esql}} Query log [esql-query-log]


The {{esql}} query log allows to log {{esql}} queries based on their execution time.

You can use these logs to investigate, analyze or troubleshoot your cluster’s historical {{esql}} performance.

{{esql}} query log reports task duration at coordinator level, but might not encompass the full task execution time observed on the client. For example, logs don’t surface HTTP network delays.

Events that meet the specified threshold are emitted into  [{{es}} server logs](docs-content://deploy-manage/monitor/logging-configuration/update-elasticsearch-logging-levels.md).

These logs can be found in local {{es}} service logs directory. Slow log files have a suffix of `_esql_querylog.json`.

## Query log format [query-log-format]

The following is an example of a successful query event in the query log:

```js
{
    "@timestamp": "2025-03-11T08:39:50.076Z",
    "log.level": "TRACE",
    "auth.type": "REALM",
    "elasticsearch.querylog.planning.took": 3108666,
    "elasticsearch.querylog.planning.took_millis": 3,
    "elasticsearch.querylog.query": "from index | limit 100",
    "elasticsearch.querylog.search_type": "ESQL",
    "elasticsearch.querylog.success": true,
    "elasticsearch.querylog.took": 8050416,
    "elasticsearch.querylog.took_millis": 8,
    "user.name": "elastic-admin",
    "user.realm": "default_file",
    "ecs.version": "1.2.0",
    "service.name": "ES_ECS",
    "event.dataset": "elasticsearch.esql_querylog",
    "process.thread.name": "elasticsearch[runTask-0][esql_worker][T#12]",
    "log.logger": "esql.querylog.query",
    "elasticsearch.cluster.uuid": "KZo1V7TcQM-O6fnqMm1t_g",
    "elasticsearch.node.id": "uPgRE2TrSfa9IvnUpNT1Uw",
    "elasticsearch.node.name": "runTask-0",
    "elasticsearch.cluster.name": "runTask"
}
```
% NOTCONSOLE

The following is an example of a failing query event in the query log:

```js
{
    "@timestamp": "2025-03-11T08:41:54.172Z",
    "log.level": "TRACE",
    "auth.type": "REALM",
    "elasticsearch.querylog.error.message": "line 1:15: mismatched input 'limitxyz' expecting {DEV_CHANGE_POINT, 'enrich', 'dissect', 'eval', 'grok', 'limit', 'sort', 'stats', 'where', DEV_INLINESTATS, DEV_FORK, 'lookup', DEV_JOIN_LEFT, DEV_JOIN_RIGHT, DEV_LOOKUP, 'mv_expand', 'drop', 'keep', DEV_INSIST, 'rename'}",
    "elasticsearch.querylog.error.type": "org.elasticsearch.xpack.esql.parser.ParsingException",
    "elasticsearch.querylog.query": "from person | limitxyz 100",
    "elasticsearch.querylog.search_type": "ESQL",
    "elasticsearch.querylog.success": false,
    "elasticsearch.querylog.took": 963750,
    "elasticsearch.querylog.took_millis": 0,
    "user.name": "elastic-admin",
    "user.realm": "default_file",
    "ecs.version": "1.2.0",
    "service.name": "ES_ECS",
    "event.dataset": "elasticsearch.esql_querylog",
    "process.thread.name": "elasticsearch[runTask-0][search][T#16]",
    "log.logger": "esql.querylog.query",
    "elasticsearch.cluster.uuid": "KZo1V7TcQM-O6fnqMm1t_g",
    "elasticsearch.node.id": "uPgRE2TrSfa9IvnUpNT1Uw",
    "elasticsearch.node.name": "runTask-0",
    "elasticsearch.cluster.name": "runTask"
}
```
% NOTCONSOLE

## Enable query logging [enable-query-log]

You can enable query logging at cluster level.

By default, all thresholds are set to `-1`, which results in no events being logged.

Query log thresholds can be enabled for the four logging levels: `trace`, `debug`, `info`, and `warn`.

To view the current query log settings, use the [get cluster settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-get-settings):

```console
GET _cluster/settings?filter_path=*.esql.querylog.*
```

You can use the `esql.querylog.include.user` setting to append `user.*` and `auth.type` fields to slow log entries. These fields contain information about the user who triggered the request.

The following snippet adjusts all available {{esql}} query log settings [update cluster settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings):

```console
PUT /_cluster/settings
{
  "transient": {
    "esql.querylog.threshold.warn": "10s",
    "esql.querylog.threshold.info": "5s",
    "esql.querylog.threshold.debug": "2s",
    "esql.querylog.threshold.trace": "500ms",
    "esql.querylog.include.user": true
  }
}
```



## Best practices for query logging [troubleshoot-query-log]

Logging slow requests can be resource intensive to your {{es}} cluster depending on the qualifying traffic’s volume. For example, emitted logs might increase the index disk usage of your [{{es}} monitoring](docs-content://deploy-manage/monitor/stack-monitoring.md) cluster. To reduce the impact of slow logs, consider the following:

* Set high thresholds to reduce the number of logged events.
* Enable slow logs only when troubleshooting.

If you aren’t sure how to start investigating traffic issues, consider enabling the `warn` threshold with a high `30s` threshold at the index level using the [update cluster settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings):

Here is an example of how to change cluster settings to enable query logging at `warn` level, for queries taking more than 30 seconds, and include user information in the logs:

```console
PUT /_cluster/settings
{
  "transient": {
    "esql.querylog.include.user": true,
    "esql.querylog.threshold.warn": "30s"
  }
}
```

