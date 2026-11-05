# Downsample plugin

Downsampling reduces the storage footprint of a time series (TSDB) index by replacing its raw data points with one
summary document per time series (`_tsid`) per fixed time interval. The result is a new, read-only index that keeps the
same dimensions, but stores far fewer documents.

This plugin (`x-pack-downsample`) contains the downsample API, the cluster-level orchestration, and the shard-level
indexer. The request/response classes, the task class and the persistent task state live elsewhere:

| Piece | Location |
|---|---|
| `DownsampleAction`, `DownsampleConfig` | `server/.../action/downsample/` |
| `DownsampleShardTask`, `DownsampleShardPersistentTaskState`, `DownsampleShardIndexerStatus` | `x-pack/plugin/core/.../core/downsample/` |
| ILM `DownsampleAction` (lifecycle step) | `x-pack/plugin/core/.../core/ilm/` |
| Data stream lifecycle integration | `modules/data-streams` |

User-facing documentation: `docs/reference/elasticsearch/index-lifecycle-actions/ilm-downsample.md` and the
`indices.downsample` REST API spec.

## Contents

- [Using the feature](#using-the-feature)
- [How downsampling works](#how-downsampling-works)
- [The transport action step by step](#the-transport-action-step-by-step)
- [How the persistent task framework is used](#how-the-persistent-task-framework-is-used)
- [The shard indexer](#the-shard-indexer)
- [Failure handling and retries](#failure-handling-and-retries)
- [Code map](#code-map)
- [Testing](#testing)

## Using the feature

### Prerequisites

The source index must:

1. Be a time series index (`index.mode: time_series`) with at least one dimension field.
2. Have a write block (`index.blocks.write: true`). Downsampling never modifies the source, and the block guarantees
   that it does not change while it is being read.
3. Not use document level security or field level security for the user making the request.

### The API

```
POST /<source_index>/_downsample/<target_index>
{
  "fixed_interval": "1h",
  "sampling_method": "aggregate",
  "wait_timeout": "1d"
}
```

| Parameter | Description |
|---|---|
| `fixed_interval` | Required. The size of the downsampling bucket, e.g. `10m`, `1h`, `1d`. Only fixed intervals are supported. |
| `sampling_method` | Optional. `aggregate` (default) or `last_value`. See [sampling methods](#sampling-methods). |
| `wait_timeout` | Optional. How long the request waits for the shard tasks to finish. Defaults to 1 day (`DownsampleAction.DEFAULT_WAIT_TIMEOUT`). |

The response is an acknowledged response once the target index is complete.

### Downsampling an already downsampled index

A downsampled index can be downsampled again, with these restrictions (validated in `validateDownsamplingConfiguration`):

- The new interval must be a multiple of the source interval (see `DownsampleConfig.validateSourceAndTargetIntervals`).
- The sampling method must match the one used for the source index.
- The time zone must match.

### Sampling methods

| Method | Metrics in the target index |
|---|---|
| `aggregate` | Gauges become `aggregate_metric_double` fields (`min`, `max`, `sum`, `value_count`, restricted to what the source supports). Counters keep their last value, plus extra reset data points. Histograms are merged. |
| `last_value` | The last value of every metric in the bucket is stored, the mapping of the field is unchanged. |

Dimensions are copied as they are. Labels (non-dimension, non-metric fields) always use the last value, regardless of
the sampling method.

### Typical callers

Most users never call the API directly:

- **ILM** — the `downsample` action in the warm/cold phases. ILM sets the write block, calls the API, and swaps the
  source index for the downsampled one.
- **Data stream lifecycle** — downsampling rounds configured on a data stream.

Both rely on the API being idempotent, see [Failure handling and retries](#failure-handling-and-retries).

## How downsampling works

At a high level, for a source index with `N` primary shards:

```
 POST /source/_downsample/target
            │
            ▼
 TransportDownsampleAction (elected master)
   validate, create target index (N shards, hidden, refresh disabled)
            │
            ├── persistent task for shard 0 ──► runs on the node holding shard 0 of the source
            ├── persistent task for shard 1 ──► runs on the node holding shard 1 of the source
            └── …
                      each task: scan the shard in _tsid / @timestamp order,
                      build one document per (_tsid, rounded @timestamp),
                      bulk index it into the same shard number of the target
            │
            ▼
   wait for all tasks → set replicas, write block → refresh → mark `success` → flush
```

Key properties:

- **One task per source shard.** The target index has the same number of shards as the source and the data of a source
  shard ends up in the same shard number of the target.
- **Data is read locally.** Each task is assigned to a node that holds a started, searchable copy of its source shard.
- **A bucket is `(_tsid, rounded @timestamp)`.** The timestamp is rounded down to the `fixed_interval` using the
  configured time zone, but never below the start of the index time bounds.
- **`doc_count`** of each target document is the sum of the `_doc_count` of the source documents, so repeated
  downsampling keeps counts closer to the raw count.
- **A bucket can produce extra documents.** With the `aggregate` sampling method, when a counter (or cumulative
  histogram) resets inside a bucket, the downsampler emits additional reset documents next to the bucket document
  (see `ResetDataPoints` and `DownsampleBucketBuilder#flushResetDocumentsIfNeeded`). They carry the dimensions, a
  timestamp and the reset values, so that rate calculations on the downsampled data stay correct.

### The target index

The target index is created with settings copied from the source (settings marked `NotCopyableOnResize`, and
`index.default_pipeline`, `index.final_pipeline`, `index.blocks.write` and `index.lifecycle.name` are not copied, see
`TransportDownsampleAction.copyIndexMetadata`). On top of that it gets:

| Setting | Value while building | Final value |
|---|---|---|
| `index.number_of_shards` | same as source | same |
| `index.number_of_replicas` | `0` (`1` in stateless, see below) | restored from the source |
| `index.refresh_interval` | `-1` | the refreshed state is done explicitly at the end |
| `index.hidden` | `true` | restored to the source value |
| `index.downsample.status` | `started` | `success` |
| `index.downsample.interval` / `.method` | requested interval / sampling method | same |
| `index.downsample.origin.name` / `.uuid` | first non-downsampled ancestor | same |
| `index.downsample.source.name` / `.uuid` | the source index | same |
| `index.blocks.write` | not set | `true` |

The mapping is derived from the source mapping by `createDownsampleIndexMapping`: the timestamp field gets a `meta` entry
with the interval and time zone, gauges are rewritten to `aggregate_metric_double` for the `aggregate` method, and every
other field is copied.

> **Stateless.** The target gets one replica from the start.

## The transport action step by step

`TransportDownsampleAction` is a master node action (`AcknowledgedTransportMasterNodeAction`). Its `masterOperation`
chains asynchronous steps; each step's listener starts the next one.

### Validation (synchronous, in `masterOperation`)

1. Reject data selectors (`::data`/`::failures`) in the source or target names.
2. Reject the request if the user has document level or field level security on the source index.
3. The source index must exist, be a time series index, and have the write block.
4. **Short circuit** (`canShortCircuit`) if the target already exists, see
   [Failure handling and retries](#failure-handling-and-retries).
5. Validate the target index name.

Failures in this phase are recorded with the `INVALID_CONFIGURATION` metric status.

### Steps

| # | Step | Where in code | Notes |
|---|---|---|---|
| 1 | Get the source mappings | `masterOperation` | via `GetMappingsRequest` |
| 2 | Validate the config and collect fields | `masterOperation` | Builds a `MapperService` for the source, runs `validateDownsamplingConfiguration` (re-downsampling rules), and `TimeSeriesFields.Collector` to split fields into dimensions, metrics, labels and multi-field sources. Fails if there are no dimension fields. |
| 3 | Create the target index | `createDownsampleIndex` | Submitted to the `downsample` master task queue (priority `URGENT`) as a cluster state update that calls `MetadataCreateIndexService.applyCreateIndexRequest`, waiting for one active shard. If the index already exists the step does not fail; it goes to `canShortCircuit` and then to step 4 (a retry of an earlier invocation). |
| 4 | Run the shard tasks | `performShardDownsampling` | Starts one persistent task per source shard and waits for all of them. See [below](#how-the-persistent-task-framework-is-used). |
| 5 | Finalize the target settings | `updateTargetIndexSettingStep` | Sets `index.blocks.write`, restores the replica count (stateful only) and `index.hidden`. |
| 6 | Refresh | `UpdateDownsampleIndexSettingsActionListener` → `RefreshDownsampleIndexActionListener` | Makes the data visible, a partial refresh failure is logged but not fatal. |
| 7 | Mark success | `RefreshDownsampleIndexActionListener.onResponse` | Cluster state update: `index.downsample.status` becomes `success`. This is the commit point of the whole operation. |
| 8 | Flush | `FlushActionListener` | Best effort. A failure is logged, the response is still acknowledged. |

Metrics (`DownsampleMetrics`) are recorded for the overall operation (`SUCCESS`, `FAILED`, `INVALID_CONFIGURATION`) by
the `MeasurementActionListener` and the failure paths.

If anything fails before step 7, the request fails and the target index is left in place with
`index.downsample.status: started`. Calling the API again picks up the work (see below); it is the caller's
responsibility (ILM and the data stream lifecycle do this) to retry or delete the target index.

## How the persistent task framework is used

Downsampling a shard can take a long time, must survive the loss of the node running it, and must not be tied to the
lifetime of one HTTP request. That is what persistent tasks provide: the task definition is stored in the cluster state
(`PersistentTasksCustomMetadata`), the master assigns it to a node, and it is reassigned if that node leaves.

### The pieces

| Element | Class | Role |
|---|---|---|
| Task name | `DownsampleShardTask.TASK_NAME` (`rollup-shard`) | Identifies the executor. |
| Params | `DownsampleShardTaskParams` | Immutable description of the work: the config, target index, source index time bounds, the shard id, and the metric / label / dimension field names and multi-field sources. Stored in the cluster state. |
| State | `DownsampleShardPersistentTaskState` | Stored in the cluster state, it holds the `DownsampleShardIndexerStatus` and an optional `_tsid`: the resume watermark, below which every bucket is known to be in the target index. See [The resume watermark](#the-resume-watermark). |
| Executor | `DownsampleShardPersistentTaskExecutor` | Decides where the task runs and how it starts. |
| Task | `DownsampleShardTask` (`AllocatedPersistentTask`) | The running task, exposes progress counters in the task status API (docs received / sent / indexed / skipped / failed, last bulk info and timestamps). |

Everything is registered in `Downsample` (`PersistentTaskPlugin#getPersistentTasksExecutor`, plus the named `Writeables` and
named `XContent` entries for the params and state).

### Task identity and idempotent start

The persistent task id is deterministic:

```
downsample-<target_index>-<shard_id>-<interval>
```

(`createPersistentTaskId`, using `DOWNSAMPLED_INDEX_PREFIX` from the ILM action.) `sendStartRequest` fails with
`ResourceAlreadyExistsException` if the task is already in the cluster state. The transport action treats this as
"someone is already running it" and simply waits for the existing task. Two concurrent API calls for the same
source/target/interval therefore share the same tasks.

### Lifecycle of one shard task

```
 master                                          node holding the source shard
 ──────                                          ─────────────────────────────
 sendStartRequest(id, "rollup-shard", params)
   executor.validate(...)  ── shard exists?
   cluster state: task added, unassigned
   executor.doGetAssignment(...)  ── pick node
   cluster state: task assigned ────────────────► executor.createTask(...)
                                                  executor.nodeOperation(task, params, state)
                                                    1. DelegatingAction (local transport action), with the
                                                       resume _tsid of the previous run's state, if any
                                                    2. realNodeOperation → "downsample_indexing" thread pool
                                                    3. DownsampleShardIndexer.execute()
   task state updated (STARTED, …) ◄──────────── updatePersistentTaskState
   task state updated (COMPLETED) ◄───────────── updatePersistentTaskState
   task removed from the cluster state ◄──────── markAsCompleted
```

**Assignment (`doGetAssignment`).** The task goes to a node holding an active, started and searchable copy of the source
shard (primary or replica). When several nodes qualify, the least loaded one is chosen, because tasks are long running.
If no node qualifies the assignment is `NO_NODE_FOUND` and the master retries on cluster state changes. If the source
shard routing no longer exists (e.g. the index was deleted), a data node is returned deliberately so that the task
starts, fails, and is then removed, rather than sitting unassigned forever.

`validate` is a cheap pre-check at start time that the shard exists. `automaticReassignmentOnShutdown()` returns `false`,
so a task is not moved while its node shuts down gracefully.

**Resuming from `_tsid`.** `nodeOperation` resumes a reassigned task from the `_tsid` in its persisted state, the resume
watermark published by the previous run, and starts a task without a state from the beginning of the shard. The target
index is never consulted for a resume point: its highest `_tsid` is not safe, because bulks complete in any order and a
higher `_tsid` can be present while a lower one is missing ([#164498](https://github.com/elastic/elasticsearch/issues/164498)).
See [The resume watermark](#the-resume-watermark).

**The delegating action.** `nodeOperation` does not run the indexer directly; it executes the local-only
`indices:data/read/downsample_delegate` transport action (`DelegatingAction`), whose request implements
`IndicesRequest.RemoteClusterShardRequest` and never leaves the node. This exists purely for security: when a searcher
is acquired straight from a persistent task, the security thread context has no `_indices_permissions`. Going through
a proper indices action makes the security filter populate them.

The executor receives the node's plain `Client` from the plugin framework (`getPersistentTasksExecutor`), not one with
an origin set, unlike `TransportDownsampleAction` which wraps its client with `ClientHelper.ROLLUP_ORIGIN`. The DLS/FLS
restriction is only enforced at request time, by `TransportDownsampleAction#masterOperation`, and only when the
indices permissions are present in the thread context and cover the exact source index name.

**Running.** `realNodeOperation` forks to the dedicated fixed `downsample_indexing` thread pool
(`xpack.downsample.thread_pool`, one eighth of the allocated processors, queue of 256), builds a
`DownsampleShardIndexer` with an `INITIALIZED` state (the resume `_tsid` is currently always `null`), and runs it.

### Task state and what the master waits for

The task state is the contract between the shard task and the transport action. The shard indexer publishes it with
`updatePersistentTaskState`:

| Status | Meaning |
|---|---|
| `INITIALIZED` | Created by the executor, never published by itself. |
| `STARTED` | The indexer is scanning the shard. |
| `COMPLETED` | All buckets have been indexed. |
| `FAILED` | Any failure: non retriable errors, or bulk failures noticed while scanning. |
| `CANCELLED` | The task was cancelled. |

`DownsampleShardPersistentTaskState.done()` is true for `COMPLETED`, `FAILED` and `CANCELLED`.

`performShardDownsampling` uses `PersistentTasksService.waitForPersistentTaskCondition` with a predicate that is
satisfied when:

- the task has been **removed** from the cluster state (it completed and was cleaned up), or
- its state is `done()`.

When the condition is met, `FAILED` and `CANCELLED` states fail the whole request; otherwise a counter is decremented
and the request proceeds to step 5 when all shards have reported. The wait is bounded by the request `wait_timeout`.
The start request itself has a fixed 30 seconds timeout.

Failures are reported through the state before the task is stopped (`markAsFailed` publishes `FAILED` and then
calls `task.markAsFailed(e)`), so that the waiting transport action can observe it even though the task is going away.

## The shard indexer

`DownsampleShardIndexer` does the actual work for one shard.

1. **Acquire a searcher** on the source shard (`acquireSearcher("downsampling")`) and build a `SearchExecutionContext`.
2. **Create field downsamplers** (`AbstractFieldDownsampler.create`), one per field, according to its type and the sampling
   method. For example `NumericMetricFieldDownsampler` (gauges, aggregate counters), `LastValueFieldDownsampler`,
   `DimensionFieldDownsampler`, `ExponentialHistogramFieldDownsampler`, `TDigestHistogramFieldDownsampler` and
   `AggregateMetricDoubleFieldDownsampler`.
3. **Build the query.** If the state has a `_tsid`, only time series from that `_tsid` onwards are read, including that
   one (`XSortedSetDocValuesRangeQuery` on `_tsid` with an inclusive lower bound and no upper bound); otherwise all
   documents. If the query matches nothing, the shard is already finished.
4. **Scan in time series order** using `TimeSeriesIndexSearcher`, which delivers documents sorted by `_tsid` ascending
   and `@timestamp` descending. This ordering is what allows the indexer to hold only a single open bucket at a time
   (`DownsampleBucketBuilder`).
5. **Bucket documents.** For each document the timestamp is rounded; when the `_tsid` or rounded timestamp changes, the
   current bucket is serialized (`buildDownsampleDocument`) and handed to the bulk processor, and the builder is reset.
   Doc values are read in batches of 8096 doc ids per segment.
6. **Counters.** With the `aggregate` method, counter resets inside a bucket generate additional documents
   (`ResetDataPoints`) so that rate calculations on the downsampled data stay correct.
7. **Index.** Buckets go through a `BulkProcessor2` (10,000 actions or 1MB per bulk, at most 50MB in flight, 3 retries)
   with backpressure. Documents are `create` operations, so a bucket that already exists in the target is rejected with a
   version conflict. `afterBulk` counts these conflicts as *skipped already indexed* (`getNumSkippedAlreadyIndexed`), not as
   failures.
8. **Cancellation / abort.** `checkCancelled` is called by the searcher and before/after collection: it stops on task
   cancellation (publishing `CANCELLED`, non retriable) and when an earlier bulk had failures (publishing `FAILED`,
   retriable).
9. **Verify.** After the scan (which includes closing the bulk processor, so every bulk has settled unless closing timed
   out) the indexer checks the `abort` flag and compares sent and indexed counts. A bulk failure, whether it stopped the
   scan or was only noticed while closing, and a close that timed out with bulks still in flight, both publish `FAILED`
   with the watermark and throw the retriable exception. The failure count is checked as a safety net and makes it throw a
   non-retriable `DownsampleShardIndexerException`. On success it publishes `COMPLETED`. Note that `afterBulk` adds the size of every completed bulk to the indexed count before it
   inspects the items, so failed items are counted as indexed too; failures are caught through the failure count and the
   `abort` flag, not through the sent/indexed comparison.

Progress counters on the task are visible through `GET _tasks?actions=*rollup-shard*&detailed`.

## Failure handling and retries

The design assumes that any step can be interrupted (master failover, node loss, timeouts) and that the same API call
can simply be repeated.

**Re-running the API.** If the target index already exists, `canShortCircuit` decides what to do, based on the target:

| Target index state | Action |
|---|---|
| Not a downsample index (`index.downsample.status` unknown) | Fail with `ResourceAlreadyExistsException`. |
| `index.downsample.status: success` | Return acknowledged immediately. |
| Has `index.blocks.write` but is not `success` | The shard work finished in an earlier call but a later step failed; resume at the refresh step (step 6). The write block is the marker for this. |
| Otherwise (`started`) | Go to step 4 again: start or join the shard tasks. |

**Task reassigned or restarted.** A reassigned task resumes from the watermark in its persisted state, see
[The resume watermark](#the-resume-watermark); a recreated task, whose predecessor was removed, rescans the whole source
shard. Either way every bucket from the resume point onwards is recomputed, and the ones that are already in the target
are rejected as version conflicts and counted as skipped, so nothing is indexed twice and no missing bucket is lost.

### The resume watermark

The indexer sends bulks concurrently, so they complete in any order. The highest `_tsid` in the target therefore says
nothing about whether everything below it was written: when a middle bulk fails, the target holds the time series of the
bulks before and after it with a hole in between, and a hole can even sit inside one time series whose buckets span several
bulks. The target alone cannot distinguish such a hole from a time series that never existed, so the writer records the
safe point itself.

`ResumeWatermark` tracks it: the first `_tsid` of the **oldest bulk that is not confirmed yet**, where confirmed means every
item was indexed or rejected as a version conflict. Bulks are cut in `_tsid` order, so every bucket before that bulk
belongs to a bulk that succeeded. The scan thread reports every bucket it adds (`added`) and every bulk it cuts (`sent`,
from `beforeBulk`, which runs synchronously inside the add call), so the tracker knows the first `_tsid` of each bulk;
confirmations arrive from any thread in any order and simply drop the bulk from the unconfirmed set. A failed bulk is never
confirmed, so the resume point never moves past it. When every sent bulk is confirmed, the resume point is the first bucket
of the bulk under construction, or the last `_tsid` sent. The resume query is inclusive, because a time series may span
bulks; its already indexed buckets are rejected as conflicts.

A bulk failure sets the `abort` flag, and the scan stops at the next `checkCancelled` or, usually earlier, when the bulk
processor rejects the next bucket. `execute` swallows that exception, closes the processor, which waits for every bulk in
flight to settle, and only then publishes `FAILED` with the watermark, so the watermark is exact. The publication is
waited for before the task lets itself be reassigned, because the reassignment invalidates the task's allocation and a
state update that lands after it is rejected. The reassigned task resumes from the published `_tsid`. The watermark lives
in the persistent task, so it is lost when the task is removed, for example after a non retriable failure; a recreated
task starts from the beginning of the shard.

A node loss gives the indexer no chance to publish, so a running task also publishes the watermark periodically from
`checkCancelled`, at most once per `downsample.resume_watermark.publish_interval` (ten minutes by default; tests set it to
zero to observe the publication), only when it moved, with at most one publication in flight, and without waiting for it.
Every publication is a cluster state update, hence the throttling: a task reassigned after a node loss loses at most an
interval's worth of work. A periodic publication that lands after the `FAILED` one is harmless, it carries a watermark
that is at most as high as the exact one.

In the persisted cluster state the `_tsid` is held under `encoded_tsid`, in the encoding of the `_tsid` field of search
hits. The `tsid` field of earlier versions held it as UTF-8 text, which mangles a hash; it is accepted, so that a node
can read the cluster state it persisted before an upgrade, but ignored.

The state also records the UUID of the target index the watermark was reached on. `nodeOperation` only resumes from a
watermark whose UUID matches the current target index: if the target was deleted and re-created under the same name
while the task was waiting to be reassigned, the new index holds none of the buckets below the watermark. The UUID is
also the mixed cluster guard: nodes before `downsample_resume_watermark_target_uuid` drop the UUID when they serialize
the state, so a watermark published while the cluster has such nodes could not be resumed from. `nodeOperation`
therefore leaves the UUID out while the cluster has such nodes, and the indexer publishes no watermark without a UUID.

**Retriable vs non-retriable.** `DownsampleShardIndexerException.isRetriable()` separates the two. A retriable failure
(a bulk failure) calls `markAsLocallyAborted`, which lets the framework reassign the task; everything else, including
a missing source index, marks the task `FAILED` and removes it.

A bulk failure (a failed item other than a version conflict, or a failed bulk request) sets the indexer's `abort` flag.
If the next `checkCancelled` call happens during the scan, it publishes the `FAILED` task state (with the resume watermark)
**before** it throws the retriable exception. The transport action's wait predicate treats a `done()` state as finished and
`performShardDownsampling` fails the whole API call when that state is `FAILED`. So a bulk failure fails the request,
even though the exception is retriable. It does not cancel anything: the failed shard's persistent task is locally
aborted and reassigned, and resumes from the watermark (skipping buckets already indexed); the tasks of the other
shards keep running. A retried API call joins them (see
[Task identity and idempotent start](#task-identity-and-idempotent-start)). A call retried before the reassigned task has
published its `STARTED` state observes the stale `FAILED` state and fails immediately; a later retry joins the restarted
task. Nothing limits how often a task whose bulks keep failing is reassigned.

A bulk failure that only surfaces while the bulk processor is closing, after the last `checkCancelled`, is handled the same
way once the processor is closed: the task publishes `FAILED` with the watermark and throws the retriable exception, so it
is reassigned and resumes from the watermark.

**Node loss.** The persistent task is reassigned to another node holding a copy of the shard and restarts as described
above. The target index is built without replicas in stateful mode, so losing the node that holds a target shard
fails the downsample.

**Cleanup.** The completed persistent tasks are removed from the cluster state by the framework after
`markAsCompleted`. A failed target index is not automatically deleted by the plugin.

## Code map

| Class | Responsibility |
|---|---|
| `Downsample` | Plugin entry point: thread pool, actions, REST handler, persistent task executor and registries. |
| `RestDownsampleAction` | `POST /{index}/_downsample/{target_index}`. |
| `TransportDownsampleAction` | Master action: validation, target index creation, starting and waiting for shard tasks, finalization. |
| `DownsampleShardPersistentTaskExecutor` | Assignment, task creation, and the delegating action. |
| `DownsampleShardTaskParams` | Persistent task params. |
| `DownsampleShardIndexer` | Shard scan, bucketing and bulk indexing. |
| `ResumeWatermark` | Tracks the `_tsid` a shard task can safely be resumed from. |
| `DownsampleShardIndexerException` | Failure carrying a retriable flag. |
| `TimeSeriesFields` | Splits the source mapping into dimensions, metrics, labels and multi-field sources. |
| `AbstractFieldDownsampler` and subclasses | Per field type aggregation within a bucket. |
| `DownsampleFieldSerializer` | Writes one downsampled field into the target document. |
| `ResetDataPoints`, `Temporality` | Counter / histogram reset handling for cumulative vs delta metrics. |
| `DownsampleMetrics`, `DownsamplingOperationsMonitor` | APM metrics for operations and shards. |

## Testing

| Location | What |
|---|---|
| `src/test` | Unit tests: field downsamplers, serializers, task params, executor assignment (`DownsampleShardPersistentTaskExecutorTests`), transport action helpers (`TransportDownsampleActionTests`). |
| `src/internalClusterTest` | Multi node integration tests. |
| `src/test/.../DownsampleActionSingleNodeTests` | End to end downsampling on a single node. |
| `qa/rest`, `qa/with-security`, `qa/mixed-cluster` | YAML REST tests, security (DLS/FLS), and mixed version clusters. |

```
./gradlew :x-pack:plugin:downsample:test
./gradlew :x-pack:plugin:downsample:internalClusterTest
./gradlew :x-pack:plugin:downsample:qa:rest:yamlRestTest
```
