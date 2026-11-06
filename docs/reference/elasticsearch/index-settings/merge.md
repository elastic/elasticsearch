---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-merge.html
navigation_title: Merge
applies_to:
  stack: all
---

# Merge settings [index-modules-merge]

:::{include} _snippets/serverless-availability.md
:::

A shard in Elasticsearch is a Lucene index, and a Lucene index is broken down into segments. Segments are internal storage elements in the index where the index data is stored, and are immutable. Smaller segments are periodically merged into larger segments to keep the index size at bay and to expunge deletes.

The merge process uses auto-throttling to balance the use of hardware resources between merging and other activities like search.


## Merge scheduling [merge-scheduling]

The merge scheduler controls the execution of merge operations when they are needed.
Merges run on the dedicated `merge` thread pool.
Smaller merges are prioritized over larger ones, across all shards on the node.
Merges are disk IO throttled so that bursts, while merging activity is otherwise low, are smoothed out in order to not impact indexing throughput.
There is no limit on the number of merges that can be enqueued for execution on the thread pool.
However, beyond a certain per-shard limit, after merging is completely disk IO un-throttled, indexing for the shard will itself be throttled until merging catches up.

:::{note}
The `merge` thread pool and the periodic disk space check, which prevents new merges from starting when the available disk space is low, apply to all shards on a node. They are controlled by node-level settings. Refer to [](/reference/elasticsearch/configuration-reference/merge-settings.md).
:::

The merge scheduler supports the following *dynamic* index settings:

$$$index-merge-scheduler-max-thread-count$$$

`index.merge.scheduler.max_thread_count`
:   The maximum number of threads on a **single** shard that may be merging at once. Defaults to half the number of processors the JVM sees on the node, with a minimum of 1, which works well for a good solid-state-disk (SSD). If your index is on spinning platter drives instead, decrease this to 1. This default is independent of [`node.processors`](/reference/elasticsearch/configuration-reference/thread-pool-settings.md#node.processors). With `include_defaults`, the reported default is the unclamped value computed on the responding node; it may differ from the value a data node actually uses after clamping to `max_merge_count`, and from data nodes with a different processor count.

    If `max_thread_count` would exceed `index.merge.scheduler.max_merge_count`, Elasticsearch reduces it to `max_merge_count` when the index is opened or its settings change, and logs a warning. That can happen when `max_thread_count` is left unset and `max_merge_count` is set explicitly, especially across nodes with different processor counts, or when both are set with `max_thread_count` greater than `max_merge_count`.

$$$index-merge-scheduler-max-merge-count$$$

`index.merge.scheduler.max_merge_count`
:   The maximum number of merges that may be running or waiting on a **single** shard at once. Defaults to `index.merge.scheduler.max_thread_count + 5`.

## Merge multi-threading [merge-multithreading]

In general, merges are completed concurrently using multiple threads. Generally, each individual merge is single-threaded, but multiple merges can be run in parallel.
There are some exceptions and certain merges can utilize multiple threads within the merging operation itself. These threads will be assigned from the same pool as the background merge threads.

The following *dynamic* settings control multithreaded merging:

$$$intra-merge-parallelism-enabled-setting$$$

`index.merge.intra_merge_parallelism_enabled` {applies_to}`stack: preview`
:   Enables or disables intra-merge parallelism for data types that support it (e.g. `dense_vector`). Defaults to `false`.
