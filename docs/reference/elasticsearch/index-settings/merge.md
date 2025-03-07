---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-merge.html
navigation_title: Merge
---

# Merge settings [index-modules-merge]

A shard in Elasticsearch is a Lucene index, and a Lucene index is broken down into segments. Segments are internal storage elements in the index where the index data is stored, and are immutable. Smaller segments are periodically merged into larger segments to keep the index size at bay and to expunge deletes.

The merge process uses auto-throttling to balance the use of hardware resources between merging and other activities like search.


## Merge scheduling [merge-scheduling]

The merge scheduler (ConcurrentMergeScheduler) controls the execution of merge operations when they are needed. Merges run in separate threads, and when the maximum number of threads is reached, further merges will wait until a merge thread becomes available.

The merge scheduler supports the following *dynamic* setting:

`index.merge.scheduler.max_thread_count`
:   The maximum number of threads on a single shard that may be merging at once. Defaults to `Math.max(1, Math.min(4, <<node.processors, node.processors>> / 2))` which works well for a good solid-state-disk (SSD). If your index is on spinning platter drives instead, decrease this to 1.

