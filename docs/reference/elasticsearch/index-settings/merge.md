---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-merge.html
navigation_title: Merge
---

# Merge settings [index-modules-merge]

A shard in Elasticsearch is a Lucene index, and a Lucene index is broken down into segments. Segments are internal storage elements in the index where the index data is stored, and are immutable. Smaller segments are periodically merged into larger segments to keep the index size at bay and to expunge deletes.

The merge process uses auto-throttling to balance the use of hardware resources between merging and other activities like search.


## Merge scheduling [merge-scheduling]

The merge scheduler controls the execution of merge operations when they are needed.
Merges run on the dedicated `merge` thread pool.
Smaller merges are prioritized over larger ones, across all shards on the node.
Merges are disk IO throttled so that bursts, while merging activity is otherwise low, are smoothed out in order to not impact indexing throughput.
There is no limit on the number of merges that can be enqueued for execution on the thread pool.
However, beyond a certain per-shard limit, after merging is completely disk IO un-throttled, indexing for the shard will itself be throttled until merging catches up.

The available disk space is periodically monitored, such that no new merge tasks are scheduled for execution when the available disk space is low.
This is in order to prevent that the temporary disk space, which is required while merges are executed, completely fills up the disk space on the node.

The merge scheduler supports the following *dynamic* settings:

`index.merge.scheduler.max_thread_count`
:   The maximum number of threads on a **single** shard that may be merging at once. Defaults to `Math.max(1, Math.min(4, <<node.processors, node.processors>> / 2))` which works well for a good solid-state-disk (SSD). If your index is on spinning platter drives instead, decrease this to 1.
`indices.merge.disk.check_interval`
:   The time interval for checking the available disk space. Defaults to `5s`.
`indices.merge.disk.watermark.high`
:   Controls the disk usage watermark, which defaults to `95%`, beyond which no merge tasks can start execution.
Any merge tasks scheduled *before* the limit is reached continue executing, even though the limit is exceeded in the meantime.

