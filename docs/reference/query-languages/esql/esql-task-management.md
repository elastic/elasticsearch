---
navigation_title: List running queries
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-task-management.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---



# Find long-running {{esql}} queries [esql-task-management]


You can list running {{esql}} queries with the [task management APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks):

$$$esql-task-management-get-all$$$

```console
GET /_tasks?pretty&detailed&group_by=parents&human&actions=*data/read/esql
```

Which returns a list of statuses like this:

```js
{
  "node" : "2j8UKw1bRO283PMwDugNNg",
  "id" : 5326,
  "type" : "transport",
  "action" : "indices:data/read/esql",
  "description" : "FROM test | STATS MAX(d) by a, b",  <1>
  "start_time" : "2023-07-31T15:46:32.328Z",
  "start_time_in_millis" : 1690818392328,
  "running_time" : "41.7ms",                           <2>
  "running_time_in_nanos" : 41770830,
  "cancellable" : true,
  "cancelled" : false,
  "headers" : { }
}
```
% NOTCONSOLE

1. The user submitted query.
2. Time the query has been running.


You can use this to find long running queries and, if you need to, cancel them with the [task cancellation API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks#task-cancellation):

$$$esql-task-management-cancelEsqlQueryRequestTests$$$

```console
POST _tasks/2j8UKw1bRO283PMwDugNNg:5326/_cancel
```

It may take a few seconds for the query to be stopped.

