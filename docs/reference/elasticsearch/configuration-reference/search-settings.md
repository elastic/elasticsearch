---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-settings.html
applies_to:
  deployment:
    self:
---

# Search settings [search-settings]

The following expert settings can be set to manage global search and aggregation limits.

$$$indices-query-bool-max-clause-count$$$

`indices.query.bool.max_clause_count`
:   :::{admonition} Deprecated in 8.0.0
    This deprecated setting has no effect.
    :::

    ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) {{es}} will now dynamically set the maximum number of allowed clauses in a query, using a heuristic based on the size of the search thread pool and the size of the heap allocated to the JVM. This limit has a minimum value of 1024 and will in most cases be larger (for example, a node with 30Gb RAM and 48 CPUs will have a maximum clause count of around 27,000). Larger heaps lead to higher values, and larger thread pools result in lower values.

    Queries with many clauses should be avoided whenever possible. If you previously bumped this setting to accommodate heavy queries, you might need to increase the amount of memory available to {{es}}, or to reduce the size of your search thread pool so that more memory is available to each concurrent search.

    In previous versions of Lucene you could get around this limit by nesting boolean queries within each other, but the limit is now based on the total number of leaf queries within the query as a whole and this workaround will no longer help.


$$$search-settings-max-buckets$$$

`search.max_buckets`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), integer) Maximum number of [aggregation buckets](/reference/aggregations/bucket.md) allowed in a single response. Defaults to 65,536.

    Requests that attempt to return more than this limit will return an error.


$$$search-settings-only-allowed-scripts$$$

`search.aggs.only_allowed_metric_scripts`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), boolean) Configures whether only explicitly allowed scripts can be used in [scripted metrics aggregations](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md). Defaults to `false`.

    Requests using scripts not contained in either [`search.aggs.allowed_inline_metric_scripts`](/reference/elasticsearch/configuration-reference/search-settings.md#search-settings-allowed-inline-scripts) or [`search.aggs.allowed_stored_metric_scripts`](/reference/elasticsearch/configuration-reference/search-settings.md#search-settings-allowed-stored-scripts) will return an error.


$$$search-settings-allowed-inline-scripts$$$

`search.aggs.allowed_inline_metric_scripts`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), list of strings) List of inline scripts that can be used in scripted metrics aggregations when [`search.aggs.only_allowed_metric_scripts`](#search-settings-only-allowed-scripts) is set to `true`. Defaults to an empty list.

    Requests using other inline scripts will return an error.


$$$search-settings-allowed-stored-scripts$$$

`search.aggs.allowed_stored_metric_scripts`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), list of strings) List of ids of stored scripts that can be used in scripted metrics aggregations when [`search.aggs.only_allowed_metric_scripts`](#search-settings-only-allowed-scripts) is set to `true`. Defaults to an empty list.

    Requests using other stored scripts will return an error.


$$$indices-query-bool-max-nested-depth$$$

`indices.query.bool.max_nested_depth`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting), integer) Maximum nested depth of queries. Defaults to `30`.

    This setting limits the nesting depth of queries. Deep nesting of queries may lead to stack overflow errors.

The following search settings are supported:

* `search.aggs.rewrite_to_filter_by_filter`


## Search task watchdog settings [search-task-watchdog-settings]
```{applies_to}
stack: ga 9.4
```

The search task watchdog monitors long-running search tasks and logs hot threads when thresholds are exceeded.
This helps diagnose slow searches by capturing threads activity while the search is still running, rather
than just logging after completion.

On [data nodes](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#data-node-role),
the watchdog logs hot threads when a shard-level search operation (query/fetch phase) exceeds the
data node threshold. On [coordinator nodes](docs-content://deploy-manage/distributed-architecture/clusters-nodes-shards/node-roles.md#coordinating-only-node-role),
it logs hot threads for long-running coordinator search tasks only when they have no outstanding
shard child requests. This avoids redundant logging when the coordinator is simply waiting for
slow shards, which log their own hot threads.

The hot threads output is gzip compressed and base64-encoded. To decode it, use:

```sh
echo "<base64-data>" | base64 --decode | gzip --decompress
```

If the output is split across multiple log lines, concatenate them first.

$$$search-task-watchdog-enabled$$$

`search.task_watchdog.enabled`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), boolean) Enables or disables the search task watchdog. Defaults to `false`.

$$$search-task-watchdog-coordinator-threshold$$$

`search.task_watchdog.coordinator_threshold`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Threshold for coordinator tasks. When a search task on the coordinator node exceeds
this duration and has no outstanding shard child requests, hot threads are logged. Set to `-1ms`
to disable coordinator task monitoring.
Defaults to `3s`.

$$$search-task-watchdog-data-node-threshold$$$

`search.task_watchdog.data_node_threshold`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Threshold for data node shard tasks. When a shard-level search operation (query or fetch phase) exceeds this duration, hot threads are logged.
Set to `-1ms` to disable data node task monitoring. Defaults to `3s`.

$$$search-task-watchdog-interval$$$

`search.task_watchdog.interval`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) How frequently the watchdog checks for slow tasks. Lower values detect slow tasks sooner but consume more resources. Minimum value is `100ms`. Defaults to `1s`.

$$$search-task-watchdog-cooldown-period$$$

`search.task_watchdog.cooldown_period`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) Minimum time between hot threads logging on this node. This prevents flooding the logs when many tasks are slow simultaneously. Defaults to `30s`.


