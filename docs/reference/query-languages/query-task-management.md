---
type: how-to
navigation_title: Find running queries
description: Find, inspect, and cancel long-running Elasticsearch queries with the task management APIs, including Query DSL, ES|QL, EQL, and SQL queries.
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-task-management.html
applies_to:
  stack: ga
  serverless: unavailable
products:
  - id: elasticsearch
---

# Find and cancel long-running queries [query-task-management]

Use the [task management APIs]({{es-apis}}group/endpoint-tasks) to find queries that are still running, inspect their source and elapsed time, and cancel a query when needed. You can inspect Query DSL, {{esql}}, EQL, and SQL queries, as well as multi-search and async search requests.

{applies_to}`stack: preview 9.4+` For a visual interface, use [Query activity](docs-content://deploy-manage/monitor/query-activity.md) in {{kib}}. Query activity is also available in {{serverless-short}}, where the task management APIs aren't available for direct use.

## Before you begin

When {{es}} security features are enabled, you need the following [cluster privileges](/reference/elasticsearch/security-privileges.md#privileges-list-cluster):

- `monitor` to list and inspect tasks.
- `cancel_task` to cancel tasks.

The task list is a snapshot of running work, not a query history. A query that finishes between requests might no longer appear in the list.

## Find a running query [query-task-management-list]

1. Use the [list tasks API]({{es-apis}}operation/operation-tasks-list) with `detailed=true` to include task descriptions. Filter by the query actions you're investigating:

   ```console
   GET /_tasks?detailed=true&human=true&group_by=none&actions=indices:data/read/search*,indices:data/read/esql*,indices:data/read/eql*,indices:data/read/sql*,indices:data/read/msearch*,indices:data/read/async_search*
   ```

   `group_by=none` returns a flat `tasks` array. To narrow the request to one query family, replace `actions` with a filter from this table:

   | Query or request | Action filter |
   | --- | --- |
   | Query DSL search | `indices:data/read/search*` |
   | {{esql}} | `indices:data/read/esql*` |
   | EQL | `indices:data/read/eql*` |
   | SQL | `indices:data/read/sql*` |
   | Multi-search | `indices:data/read/msearch*` |
   | Async search | `indices:data/read/async_search*` |

   These wildcard filters include related work, such as child tasks, asynchronous query execution, and requests to retrieve async results. A query can create multiple tasks, so the number of tasks isn't a count of user queries. Use `group_by=parents` to view matching child tasks grouped under their matching parent tasks.

2. Inspect the response for tasks with a high `running_time_in_nanos`. For example, the following response contains an {{esql}} task that has been running for about 42 seconds:

   ```console-result
   {
     "tasks": [
       {
         "node": "2j8UKw1bRO283PMwDugNNg",
         "id": 5326,
         "type": "transport",
         "action": "indices:data/read/esql",
         "description": "FROM test | STATS MAX(d) BY a, b",
         "start_time": "2026-04-01T10:00:00.000Z",
         "start_time_in_millis": 1775037600000,
         "running_time": "41.7s",
         "running_time_in_nanos": 41700000000,
         "cancellable": true,
         "cancelled": false,
         "headers": {}
       }
     ]
   }
   ```

   Use these fields to identify the query:

   - `action`: The operation the task is performing.
   - `description`: Details of the request. For {{esql}}, this contains the query text. For a Query DSL search, it includes the indices, search type, and search source.
   - `running_time_in_nanos`: Elapsed runtime, not CPU time. With `human=true`, `running_time` also shows a readable duration.
   - `parent_task_id`, when present: The task that started this work. Use it to distinguish a query request from its child tasks.
   - `headers`: Request identifiers, such as `X-Opaque-Id`, when supplied. Refer to [API conventions](/reference/elasticsearch/rest-apis/api-conventions.md#x-opaque-id) and [Trace an Elasticsearch query to its origin in Kibana](docs-content://troubleshoot/kibana/trace-elasticsearch-query-to-the-origin-in-kibana.md).

   Combine `node` and `id` with a colon to form the task ID. In this example, the task ID is `2j8UKw1bRO283PMwDugNNg:5326`.

## Cancel the query [query-task-management-cancel]

1. Confirm that the task belongs to the query you want to stop and that `cancellable` is `true`. When the query has child tasks, identify the parent query task so you can cancel the query rather than one part of its work.

2. Use the [cancel task API]({{es-apis}}operation/operation-tasks-cancel) with that task ID. Replace the example ID with the ID from your response:

   ```console
   POST /_tasks/2j8UKw1bRO283PMwDugNNg:5326/_cancel
   ```

   Cancellation can take time. A task with `cancelled: true` has received the cancellation request but might still be stopping its work.

3. Repeat the list request and check whether the task is still present. When it no longer appears in a successful response, it is no longer running. Check for node or task failures in the response before concluding that the task has stopped.

   If a canceled task continues running, use the [nodes hot threads API]({{es-apis}}operation/operation-nodes-hot-threads) and the [task queue backlog](docs-content://troubleshoot/elasticsearch/task-queue-backlog.md) guidance to investigate what is delaying it.

## Investigate recurring slow queries

- For {{esql}} queries, refer to [Optimize {{esql}} query performance](/reference/query-languages/esql/esql-query-performance.md).
- For search requests, refer to [Tune for search speed](docs-content://deploy-manage/production-guidance/optimize-performance/search-speed.md).
- {applies_to}`stack: preview 9.4+` For historical query data, configure [query logging](docs-content://deploy-manage/monitor/logging-configuration/query-logs.md). Query logs let you investigate queries after they finish.
- For clusters without query logging, use [search slow logs](/reference/elasticsearch/index-settings/slow-log.md) or the [{{esql}} query log](/reference/query-languages/esql/esql-query-log.md), according to the query type and version.
