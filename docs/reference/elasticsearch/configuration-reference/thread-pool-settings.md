---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-threadpool.html
applies_to:
  deployment:
    self:
---

# Thread pool settings [modules-threadpool]

A node uses several thread pools to manage memory consumption. Queues associated with many of the thread pools enable pending requests to be held instead of discarded.

There are several thread pools, but the important ones include:

`generic`
:   For generic operations (for example, background node discovery). Thread pool type is `scaling`.

$$$search-threadpool$$$

`search`
:   For count/search operations at the shard level. Used also by fetch and other search related operations  Thread pool type is `fixed` with a size of `int((`[`# of allocated processors`](#node.processors)` * 3) / 2) + 1`, and queue_size of `1000`.

$$$search-throttled$$$`search_throttled`
:   For count/search/suggest/get operations on `search_throttled indices`. Thread pool type is `fixed` with a size of `1`, and queue_size of `100`.

`search_coordination`
:   For lightweight search-related coordination operations. Thread pool type is `fixed` with a size of `(`[`# of allocated processors`](#node.processors)`) / 2`, and queue_size of `1000`.

`get`
:   For get operations. Thread pool type is `fixed` with a size of `int((`[`# of allocated processors`](#node.processors)` * 3) / 2) + 1`, and queue_size of `1000`.

`analyze`
:   For analyze requests. Thread pool type is `fixed` with a size of `1`, queue size of `16`.

`write`
:   For single-document index/delete/update, ingest processors, and bulk requests. Thread pool type is `fixed` with a size of [`# of allocated processors`](#node.processors), queue_size of `10000`. The maximum size for this pool is `1 + `[`# of allocated processors`](#node.processors).

`snapshot`
:   For snapshot/restore operations. Thread pool type is `scaling` with a keep-alive of `5m`. On nodes with at least 750MB of heap the maximum size of this pool is `10` by default. On nodes with less than 750MB of heap the maximum size of this pool is `min(5, (`[`# of allocated processors`](#node.processors)`) / 2)` by default.

`snapshot_meta`
:   For snapshot repository metadata read operations. Thread pool type is `scaling` with a keep-alive of `5m` and a max of `min(50, (`[`# of allocated processors`](#node.processors)`* 3))`.

`warmer`
:   For segment warm-up operations. Thread pool type is `scaling` with a keep-alive of `5m` and a max of `min(5, (`[`# of allocated processors`](#node.processors)`) / 2)`.

`refresh`
:   For refresh operations. Thread pool type is `scaling` with a keep-alive of `5m` and a max of `min(10, (`[`# of allocated processors`](#node.processors)`) / 2)`.

`fetch_shard_started`
:   For listing shard states. Thread pool type is `scaling` with keep-alive of `5m` and a default maximum size of `2 * `[`# of allocated processors`](#node.processors).

`fetch_shard_store`
:   For listing shard stores. Thread pool type is `scaling` with keep-alive of `5m` and a default maximum size of `2 * `[`# of allocated processors`](#node.processors).

`flush`
:   For [flush](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-flush) and [translog](/reference/elasticsearch/index-settings/translog.md) `fsync` operations. Thread pool type is `scaling` with a keep-alive of `5m` and a default maximum size of `min(5, (`[`# of allocated processors`](#node.processors)`) / 2)`.

`force_merge`
:   For [force merge](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-forcemerge) operations. Thread pool type is `fixed` with a size of `max(1, (`[`# of allocated processors`](#node.processors)`) / 8)` and an unbounded queue size.

`management`
:   For cluster management. Thread pool type is `scaling` with a keep-alive of `5m` and a default maximum size of `5`.

`system_read`
:   For read operations on system indices. Thread pool type is `fixed` with a default maximum size of `min(5, (`[`# of allocated processors`](#node.processors)`) / 2)`.

`system_write`
:   For write operations on system indices. Thread pool type is `fixed` with a default maximum size of `min(5, (`[`# of allocated processors`](#node.processors)`) / 2)`.

`system_critical_read`
:   For critical read operations on system indices. Thread pool type is `fixed` with a default maximum size of `min(5, (`[`# of allocated processors`](#node.processors)`) / 2)`.

`system_critical_write`
:   For critical write operations on system indices. Thread pool type is `fixed` with a default maximum size of `min(5, (`[`# of allocated processors`](#node.processors)`) / 2)`.

`watcher`
:   For [watch executions](docs-content://explore-analyze/alerts-cases/watcher.md). Thread pool type is `fixed` with a default maximum size of `min(5 * (`[`# of allocated processors`](#node.processors)`), 50)` and queue_size of `1000`.

$$$modules-threadpool-esql$$$`esql_worker`
:   Executes [{{esql}}](docs-content://explore-analyze/query-filter/languages/esql.md) operations. Thread pool type is `fixed` with a size of `int((`[`# of allocated processors`](#node.processors) ` * 3) / 2) + 1`, and queue_size of `1000`.

Thread pool settings are [Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting) and can be changed by editing `elasticsearch.yml`. Changing a specific thread pool can be done by setting its type-specific parameters; for example, changing the number of threads in the `write` thread pool:

```yaml
thread_pool:
    write:
        size: 30
```

## Thread pool types [thread-pool-types]

The following are the types of thread pools and their respective parameters:

### `fixed` [fixed-thread-pool]

The `fixed` thread pool holds a fixed size of threads to handle the requests with a queue (optionally bounded) for pending requests that have no threads to service them.

The `size` parameter controls the number of threads.

The `queue_size` allows to control the size of the queue of pending requests that have no threads to execute them. By default, it is set to `-1` which means its unbounded. When a request comes in and the queue is full, it will abort the request.

```yaml
thread_pool:
    write:
        size: 30
        queue_size: 1000
```


### `scaling` [scaling-thread-pool]

The `scaling` thread pool holds a dynamic number of threads. This number is proportional to the workload and varies between the value of the `core` and `max` parameters.

The `keep_alive` parameter determines how long a thread should be kept around in the thread pool without it doing any work.

```yaml
thread_pool:
    warmer:
        core: 1
        max: 8
        keep_alive: 2m
```



## Allocated processors setting [node.processors]

The number of processors is automatically detected, and the thread pool settings are automatically set based on it. In some cases it can be useful to override the number of detected processors. This can be done by explicitly setting the `node.processors` setting. This setting is bounded by the number of available processors and accepts floating point numbers, which can be useful in environments where the {{es}} nodes are configured to run with CPU limits, such as cpu shares or quota under `Cgroups`.

```yaml
node.processors: 2
```

There are a few use-cases for explicitly overriding the `node.processors` setting:

1. If you are running multiple instances of {{es}} on the same host but want {{es}} to size its thread pools as if it only has a fraction of the CPU, you should override the `node.processors` setting to the desired fraction, for example, if you’re running two instances of {{es}} on a 16-core machine, set `node.processors` to 8. Note that this is an expert-level use case and there’s a lot more involved than just setting the `node.processors` setting as there are other considerations like changing the number of garbage collector threads, pinning processes to cores, and so on.
2. Sometimes the number of processors is wrongly detected and in such cases explicitly setting the `node.processors` setting will workaround such issues.

In order to check the number of processors detected, use the nodes info API with the `os` flag.


