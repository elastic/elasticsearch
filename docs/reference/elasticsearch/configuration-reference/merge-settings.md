---
navigation_title: "Merge settings"
applies_to:
  stack: all
---

# Merge resource limits settings

{{es}} continuously merges small segments into larger ones in the background. Merging keeps shards efficient, but it uses disk space, memory, CPU, and disk I/O on the node. The settings on this page limit how much of a node's resources merging can use, so that merging doesn't fill the disk or exhaust the memory of a node. You can use them to do any of the following:

* Stop {{es}} from starting merges when the node is low on disk space, using the `indices.merge.disk.*` settings.
* Lower peak memory usage during merging, for example on nodes that run out of memory while merging large HNSW vector graphs, using `indices.merge.thread_pool.max_size_factor`.

These settings apply to all shards on a node.

## How node-level and index-level merge settings work together [merge-settings-interaction]

Every shard schedules its own merges, and all merges on a node run on one shared [`merge` thread pool](/reference/elasticsearch/configuration-reference/thread-pool-settings.md#merge-threadpool). Smaller merges run before larger ones, across all shards on the node. A merge runs only when each of the following limits allows it:

| Limit | Applies to | Setting |
| --- | --- | --- |
| Number of merges a single shard can run at once | One shard | [`index.merge.scheduler.max_thread_count`](/reference/elasticsearch/index-settings/merge.md#index-merge-scheduler-max-thread-count) |
| Number of merges that can run at once across all shards | The whole node | The `merge` thread pool size, which you can reduce with [`indices.merge.thread_pool.max_size_factor`](#merge-thread-pool-settings) |
| Estimated temporary disk space required by the merge | The whole node | [`indices.merge.disk.watermark.high`](#merge-disk-settings) |

Keep the following in mind when you tune these settings together:

* Both the per-shard limit and the node-wide thread pool size apply. Raising `index.merge.scheduler.max_thread_count` on an index doesn't let merges on that index exceed the number of merges that the node's `merge` thread pool can run. Lowering `indices.merge.thread_pool.max_size_factor` limits the total number of merges on the node.
* A merge that is waiting for a free thread or for disk space stays queued. Queued merges count toward the per-shard backlog that is limited by [`index.merge.scheduler.max_merge_count`](/reference/elasticsearch/index-settings/merge.md#index-merge-scheduler-max-merge-count). If merging falls too far behind, indexing on the affected shard is throttled until merging catches up.
* Lowering the size of the `merge` thread pool reduces peak memory usage during merging, at the cost of merge throughput.

For more information about how merges are scheduled and throttled for each shard, refer to [Merge scheduling](/reference/elasticsearch/index-settings/merge.md#merge-scheduling).

## Merge disk usage settings [merge-disk-settings]

Merging needs temporary disk space for the new segment, in addition to the space used by the segments being merged. {{es}} checks the available disk space on the node every `indices.merge.disk.check_interval`. A queued merge starts only if the estimated disk space it still needs fits below the merge disk watermark. Merges that have already started continue to run, even if the watermark is exceeded while they run.

By default, the merge watermark uses the same value as the [flood stage disk watermark](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md#cluster-routing-flood-stage) (`cluster.routing.allocation.disk.watermark.flood_stage`), and the max headroom uses the value of `cluster.routing.allocation.disk.watermark.flood_stage.max_headroom`. As a result, merging stops at the same point where {{es}} would make indices on the node read-only. To keep more free disk space on the node, set `indices.merge.disk.watermark.high` to a lower value.

The merge scheduler supports the following settings:

`indices.merge.disk.check_interval`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting), [time value](/reference/elasticsearch/rest-apis/api-conventions.md#time-units)) The time interval for checking the available disk space. Defaults to `5s`.

`indices.merge.disk.watermark.high`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls the disk usage watermark, beyond which no merge tasks can start execution. Defaults to the value of [`cluster.routing.allocation.disk.watermark.flood_stage`](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md#cluster-routing-flood-stage), which is `95%` by default.
The disk usage tally includes the estimated temporary disk space still required by all the currently executing merge tasks.
Any merge task scheduled *before* the limit is reached continues execution, even if the limit is exceeded while executing
(merge tasks are not aborted).

`indices.merge.disk.watermark.high.max_headroom`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Controls the max headroom for the merge disk usage watermark, in case it is specified as percentage or ratio values.
When `indices.merge.disk.watermark.high` is not explicitly set, it defaults to the value of [`cluster.routing.allocation.disk.watermark.flood_stage.max_headroom`](/reference/elasticsearch/configuration-reference/cluster-level-shard-allocation-routing-settings.md#cluster-routing-flood-stage), which is `100GB` by default.
This caps the amount of free disk space before merge scheduling is blocked.

## Merge thread pool settings [merge-thread-pool-settings]

Merges run on the dedicated [`merge` thread pool](/reference/elasticsearch/configuration-reference/thread-pool-settings.md#merge-threadpool), which is shared by **all** the shards on the node.

The merge scheduler supports the following node-level setting:

`indices.merge.thread_pool.max_size_factor` {applies_to}`stack: ga 9.6`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) A factor in the range `(0, 1]` that reduces the maximum size of the [`merge` thread pool](/reference/elasticsearch/configuration-reference/thread-pool-settings.md#merge-threadpool), which runs merges for all the shards on the node. The result is rounded to the nearest integer and never lower than `1`. Lowering the factor, for example, to `0.5`, reduces how many merges can run concurrently across all the shards on the node, which lowers peak merge memory usage at the cost of merge throughput. Defaults to `1`.

    If `thread_pool.merge.max` is explicitly configured, that value is used regardless of this factor. When neither is set, the pool maximum is the number of [allocated processors](/reference/elasticsearch/configuration-reference/thread-pool-settings.md#node.processors).
