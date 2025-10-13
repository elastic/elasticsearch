# Distributed Area Internals

The Distributed Area contains indexing and coordination systems.

The index path stretches from the user REST command through shard routing down to each individual shard's translog and storage
engine. Reindexing is effectively reading from a source index and writing to a destination index (perhaps on different nodes).
The coordination side includes cluster coordination, shard allocation, cluster autoscaling stats, task management, and cross
cluster replication. Less obvious coordination systems include networking, the discovery plugin system, the snapshot/restore
logic, and shard recovery.

A guide to the general Elasticsearch components can be found [here](https://github.com/elastic/elasticsearch/blob/main/docs/internal/GeneralArchitectureGuide.md).

# Networking

### ThreadPool

(We have many thread pools, what and why)

### ActionListener

See the [Javadocs for `ActionListener`](https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/ActionListener.java)

(TODO: add useful starter references and explanations for a range of Listener classes. Reference the Netty section.)

### Chunk Encoding

#### XContent

### Performance

### Netty

(long running actions should be forked off of the Netty thread. Keep short operations to avoid forking costs)

### Work Queues

### RestClient

The `RestClient` is primarily used in testing, to send requests against cluster nodes in the same format as would users. There
are some uses of `RestClient`, via `RestClientBuilder`, in the production code. For example, remote reindex leverages the
`RestClient` internally as the REST client to the remote elasticsearch cluster, and to take advantage of the compatibility of
`RestClient` requests with much older elasticsearch versions. The `RestClient` is also used externally by the `Java API Client`
to communicate with Elasticsearch.

# Cluster Coordination

(Sketch of important classes? Might inform more sections to add for details.)

(A node can coordinate a search across several other nodes, when the node itself does not have the data, and then return a result to the caller. Explain this coordinating role)

### Node Roles

### Master Nodes

### Master Elections

(Quorum, terms, any eligibility limitations)

### Cluster Formation / Membership

(Explain joining, and how it happens every time a new master is elected)

#### Discovery

### Master Transport Actions

### Cluster State

[ClusterState]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/cluster/ClusterState.java
[Metadata]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/cluster/metadata/Metadata.java
[ProjectMetadata]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/cluster/metadata/ProjectMetadata.java

The [Metadata] of a [ClusterState] is persisted on disk and comprises information from two categories:
1. Cluster scope information such as `clusterUUID`, `CoordinationMetadata`
2. Project scope information ([ProjectMetadata]) such as indices and templates belong to each project.

Some concepts are applicable to both cluster and project scopes, e.g. [persistent tasks](#persistent-tasks). The state of a persistent task is therefore stored accordingly depending on the task's scope.

#### Master Service

#### Cluster State Publication

(Majority concensus to apply, what happens if a master-eligible node falls behind / is incommunicado.)

#### Cluster State Application

(Go over the two kinds of listeners -- ClusterStateApplier and ClusterStateListener?)

#### Persistence

(Sketch ephemeral vs persisted cluster state.)

(what's the format for persisted metadata)

# Replication

(More Topics: ReplicationTracker concepts / highlights.)

### What is a Shard

### Primary Shard Selection

(How a primary shard is chosen)

#### Versioning

(terms and such)

### How Data Replicates

(How an index write replicates across shards -- TransportReplicationAction?)

### Consistency Guarantees

(What guarantees do we give the user about persistence and readability?)

# Locking

(rarely use locks)

### ShardLock

### Translog / Engine Locking

### Lucene Locking

# Engine

(What does Engine mean in the distrib layer? Distinguish Engine vs Directory vs Lucene)

(High level explanation of how translog ties in with Lucene)

(contrast Lucene vs ES flush / refresh / fsync)

### Refresh for Read

(internal vs external reader manager refreshes? flush vs refresh)

### Reference Counting

### Store

(Data lives beyond a high level IndexShard instance. Continue to exist until all references to the Store go away, then Lucene data is removed)

### Translog

[Basic write model]:https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-replication.html
[`Translog`]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/index/translog/Translog.java
[`InternalEngine`]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/index/engine/InternalEngine.java

It is important to understand first the [Basic write model] of documents:
documents are written to Lucene in-memory buffers, then "refreshed" to searchable segments which may not be persisted on disk, and finally "flushed" to a durable Lucene commit on disk.
If this was the only way we stored the data, we would have to delay the response to every write request until after the data had been flushed to disk, which could take many seconds or longer. If we didn't, it would mean that we would lose newly ingested data if there was an outage between sending the response and flushing the data to disk.
For this reason, newly ingested data is also written to a shard's [`Translog`], whose main purpose is to persist uncommitted operations (e.g., document insertions or deletions), so they can be replayed by just reading them sequentially from the translog during [recovery](#recovery) in the event of ephemeral failures such as a crash or power loss.
The translog can persist operations quicker than a Lucene commit, because it just stores raw operations / documents without the analysis and indexing that Lucene does.
The translog is always persisted and fsync'ed on disk before acknowledging writes back to the user.
This can be seen in [`InternalEngine`] which calls the `add()` method of the translog to append operations, e.g., its `index()` method at some point adds a document insertion operation to the translog.
The translog ultimately truncates operations once they have been flushed to disk by a Lucene commit; indeed, in some sense the point of a "flush" is to clear out the translog.

Main usages of the translog are:

* During recovery, an index shard can be recovered up to at least the last acknowledged operation by replaying the translog onto the last flushed commit of the shard.
* Facilitate real-time (m)GETs of documents without refreshing.

#### Translog Truncation

[Flush API]:https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-flush.html
[`INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING`]:https://github.com/elastic/elasticsearch/blob/dd1db5031ee7fdac284753c0c3b096b0e981d71a/server/src/main/java/org/elasticsearch/index/IndexSettings.java#L352
[`INDEX_TRANSLOG_FLUSH_THRESHOLD_AGE_SETTING`]:https://github.com/elastic/elasticsearch/blob/dd1db5031ee7fdac284753c0c3b096b0e981d71a/server/src/main/java/org/elasticsearch/index/IndexSettings.java#L370

Translog files are automatically truncated when they are no longer needed, specifically after all their operations have been persisted by Lucene commits on disk.
Lucene commits are initiated by flushes (e.g., with the index [Flush API]).

Flushes may also be automatically initiated by Elasticsearch, e.g., if the translog exceeds a configurable size [`INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING`] or age [`INDEX_TRANSLOG_FLUSH_THRESHOLD_AGE_SETTING`], which ultimately truncates the translog as well.

#### Acknowledging writes

[`index()` or `delete()`]:https://github.com/elastic/elasticsearch/blob/591fa87e43a509d3eadfdbbb296cdf08453ea91a/server/src/main/java/org/elasticsearch/index/engine/Engine.java#L546-L564
[`TransportWriteAction`]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/replication/TransportWriteAction.java
[`indexShard.syncAfterWrite()`]:https://github.com/elastic/elasticsearch/blob/387eef070c25ed57e4139158e7e7e0ed097c8c98/server/src/main/java/org/elasticsearch/action/support/replication/TransportWriteAction.java#L548
[`Location`]:https://github.com/elastic/elasticsearch/blob/693f3bfe30271d77a6b3147e4519b4915cbb395d/server/src/main/java/org/elasticsearch/index/translog/Translog.java#L977
[`AsyncIOProcessor`]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/common/util/concurrent/AsyncIOProcessor.java

A bulk request will repeateadly call ultimately the Engine methods such as [`index()` or `delete()`] which adds operations to the Translog.
Finally, the AfterWrite action of the [`TransportWriteAction`] will call [`indexShard.syncAfterWrite()`] which will put the last written translog [`Location`] of the bulk request into a [`AsyncIOProcessor`] that is responsible for gradually fsync'ing the Translog and notifying any waiters.
Ultimately the bulk request is notified that the translog has fsync'ed past the requested location, and can continue to acknowledge the bulk request.
This process involes multiple writes to the translog before the next fsync(), and this is done so that we amortize the cost of the translog's fsync() operations across all writes.

#### Translog internals

[`Checkpoint`]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/index/translog/Checkpoint.java
[`Location`]:https://github.com/elastic/elasticsearch/blob/693f3bfe30271d77a6b3147e4519b4915cbb395d/server/src/main/java/org/elasticsearch/index/translog/Translog.java#L977
[`Operation`]:https://github.com/elastic/elasticsearch/blob/693f3bfe30271d77a6b3147e4519b4915cbb395d/server/src/main/java/org/elasticsearch/index/translog/Translog.java#L1087
[`Snapshot`]:https://github.com/elastic/elasticsearch/blob/693f3bfe30271d77a6b3147e4519b4915cbb395d/server/src/main/java/org/elasticsearch/index/translog/Translog.java#L711
[`sync()`]:https://github.com/elastic/elasticsearch/blob/693f3bfe30271d77a6b3147e4519b4915cbb395d/server/src/main/java/org/elasticsearch/index/translog/Translog.java#L813
[`rollGeneration()`]:https://github.com/elastic/elasticsearch/blob/693f3bfe30271d77a6b3147e4519b4915cbb395d/server/src/main/java/org/elasticsearch/index/translog/Translog.java#L1656
[`createEmptyTranslog()`]:https://github.com/elastic/elasticsearch/blob/693f3bfe30271d77a6b3147e4519b4915cbb395d/server/src/main/java/org/elasticsearch/index/translog/Translog.java#L1929
[`TranslogHeader`]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/index/translog/TranslogHeader.java
[`TranslogReader`]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/index/translog/TranslogReader.java
[`TranslogSnapshot`]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/index/translog/TranslogSnapshot.java
[`MultiSnapshot`]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/index/translog/MultiSnapshot.java
[`TranslogWriter`]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/index/translog/TranslogWriter.java

Each translog is a sequence of files, each identified by a translog generation ID, each containing a sequence of operations, with the last file open for writes.
The last file has a part which has been fsync'ed to disk, and a part which has been written but not necessarily fsync'ed yet to disk.
Each operation is identified by a sequence number (`seqno`), which is monotonically increased by the engine's ingestion functionality.
Typically the entries in the translog are in increasing order of their sequence number, but not necessarily.
A [`Checkpoint`] file is also maintained, which is written on each fsync operation of the translog, and is necessary because it records important metadata and statistics about the translog, such as the current translog generation ID, its last fsync'ed operation and location (i.e., we should read only up to this location during recovery), the minimum translog generation ID, and the minimum and maximum sequence number of operations the sequence of translog generations include, all of which are used to identify the translog operations needed to be replayed upon recovery.
When the translog rolls over, e.g., upon the translog file exceeding a configurable size, a new file in the sequence is created for writes, and the last one becomes read-only.
A new commit flushed to the disk will also induce a translog rollover, since the operations in the translog so far will become eligible for truncation.

A few more words on terminology and classes used around the translog Java package.
A [`Location`] of an operation is defined by the translog generation file it is contained in, the offset of the operation in that file, and the number of bytes that encode that operation.
An [`Operation`] can be a document indexed, a document deletion, or a no-op operation.
A [`Snapshot`] iterator can be created to iterate over a range of requested operation sequence numbers read from the translog files.
The [`sync()`] method is the one that fsync's the current translog generation file to disk, and updates the checkpoint file with the last fsync'ed operation and location.
The [`rollGeneration()`] method is the one that rolls the translog, creating a new translog generation, e.g., called during an index flush.
The [`createEmptyTranslog()`] method creates a new translog, e.g., for a new empty index shard.
Each translog file starts with a [`TranslogHeader`] that is followed by translog operations.

Some internal classes used for reading and writing from the translog are the following.
A [`TranslogReader`] can be used to read operation bytes from a translog file.
A [`TranslogSnapshot`] can be used to iterate operations from a translog reader.
A [`MultiSnapshot`] can be used to iterate operations over multiple [`TranslogSnapshot`]s.
A [`TranslogWriter`] can be used to write operations to the translog.

#### Real-time GETs from the translog

[Get API]:https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html
[`LiveVersionMap`]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/index/engine/LiveVersionMap.java

The [Get API] (and by extension, the multi-get API) supports a real-time mode, which can query documents by ID, even recently ingested documents that have not yet been refreshed and not searchable.
This capability is facilitated by another data structure, the [`LiveVersionMap`], which maps recently ingested documents by their ID to the translog location that encodes their indexing operation.
That way, we can return the document by reading the translog operation.

The tracking in the version map is not enabled by default.
The first real-time GET induces a refresh of the index shard, and a search to get the document, but also enables the tracking in the version map for newly ingested documents.
Thus, next real-time GETs are serviced by going first through the version map, to query the translog, and if not found there, then search (refreshed data) without requiring to refresh the index shard.

On a refresh, the code safely swaps the old map with a new empty map.
That is because after a refresh, any documents in the old map are now searchable in Lucene, and thus we do not need them in the version map anymore.

### Index Version

### Lucene

(copy a sketch of the files Lucene can have here and explain)

(Explain about SearchIndexInput -- IndexWriter, IndexReader -- and the shared blob cache)

(Lucene uses Directory, ES extends/overrides the Directory class to implement different forms of file storage.
Lucene contains a map of where all the data is located in files and offsites, and fetches it from various files.
ES doesn't just treat Lucene as a storage engine at the bottom (the end) of the stack. Rather ES has other information that
works in parallel with the storage engine.)

#### Segment Merges

# Recovery

(All shards go through a 'recovery' process. Describe high level. createShard goes through this code.)

(How is the translog involved in recovery?)

### Create a Shard

### Local Recovery

### Peer Recovery

### Snapshot Recovery

### Recovery Across Server Restart

(partial shard recoveries survive server restart? `reestablishRecovery`? How does that work.)

### How a Recovery Method is Chosen

# Data Tiers

(Frozen, warm, hot, etc.)

# Allocation

### Indexes and Shards

Each index consists of a fixed number of primary shards. The number of primary shards cannot be changed for the lifetime of the index. Each
primary shard can have zero-to-many replicas used for data redundancy. The number of replicas per shard can be changed dynamically.

The allocation assignment status of each shard copy is tracked by its [ShardRoutingState][]. The `RoutingTable` and `RoutingNodes` objects
are responsible for tracking the data nodes to which each shard in the cluster is allocated: see the [routing package javadoc][] for more
details about these structures.

[routing package javadoc]: https://github.com/elastic/elasticsearch/blob/v9.0.0-beta1/server/src/main/java/org/elasticsearch/cluster/routing/package-info.java
[ShardRoutingState]: https://github.com/elastic/elasticsearch/blob/4c9c82418ed98613edcd91e4d8f818eeec73ce92/server/src/main/java/org/elasticsearch/cluster/routing/ShardRoutingState.java#L12-L46

### Core Components

The `DesiredBalanceShardsAllocator` is what runs shard allocation decisions. It leverages the `DesiredBalanceComputer` to produce
`DesiredBalance` instances for the cluster based on the latest cluster changes (add/remove nodes, create/remove indices, load, etc.). Then
the `DesiredBalanceReconciler` is invoked to choose the next steps to take to move the cluster from the current shard allocation to the
latest computed `DesiredBalance` shard allocation. The `DesiredBalanceReconciler` will apply changes to a copy of the `RoutingNodes`, which
is then published in a cluster state update that will reach the data nodes to start the individual shard recovery/deletion/move work.

The `DesiredBalanceReconciler` is throttled by cluster settings, like the max number of concurrent shard moves and recoveries per cluster
and node: this is why the `DesiredBalanceReconciler` will make, and publish via cluster state updates, incremental changes to the cluster
shard allocation. The `DesiredBalanceShardsAllocator` is the endpoint for reroute requests, which may trigger immediate requests to the
`DesiredBalanceReconciler`, but asynchronous requests to the `DesiredBalanceComputer` via the `ContinuousComputation` component. Cluster
state changes that affect shard balancing (for example index deletion) all call some reroute method interface that reaches the
`DesiredBalanceShardsAllocator` to run reconciliation and queue a request for the `DesiredBalancerComputer`, leading to desired balance
computation and reconciliation actions. Asynchronous completion of a new `DesiredBalance` will also invoke a reconciliation action, as will
cluster state updates completing shard moves/recoveries (unthrottling the next shard move/recovery).

The `ContinuousComputation` saves the latest desired balance computation request, which holds the cluster information at the time of that
request, and a thread that runs the `DesiredBalanceComputer`. The `ContinuousComputation` thread takes the latest request, with the
associated cluster information, feeds it into the `DesiredBalanceComputer` and publishes a `DesiredBalance` back to the
`DesiredBalanceShardsAllocator` to use for reconciliation actions. Sometimes the `ContinuousComputation` thread's desired balance
computation will be signalled to exit early and publish the initial `DesiredBalance` improvements it has made, when newer rebalancing
requests (due to cluster state changes) have arrived, or in order to begin recovery of unassigned shards as quickly as possible.

### Rebalancing Process

There are different priorities in shard allocation, reflected in which moves the `DesiredBalancerReconciler` selects to do first given that
it can only move, recover, or remove a limited number of shards at once. The first priority is assigning unassigned shards, primaries being
more important than replicas. The second is to move shards that violate any rule (such as node resource limits) as defined by an
`AllocationDecider`. The `AllocationDeciders` holds a group of `AllocationDecider` implementations that place hard constraints on shard
allocation. There is a decider, `DiskThresholdDecider`, that manages disk memory usage thresholds, such that further shards may not be
allowed assignment to a node, or shards may be required to move off because they grew to exceed the disk space; or another,
`FilterAllocationDecider`, that excludes a configurable list of indices from certain nodes; or `MaxRetryAllocationDecider` that will not
attempt to recover a shard on a certain node after so many failed retries. The third priority is to rebalance shards to even out the
relative weight of shards on each node: the intention is to avoid, or ease, future hot-spotting on data nodes due to too many shards being
placed on the same data node. Node shard weight is based on a sum of factors: disk memory usage, projected shard write load, total number
of shards, and an incentive to distribute shards within the same index across different nodes. See the `WeightFunction` and
`NodeAllocationStatsAndWeightsCalculator` classes for more details on the weight calculations that support the `DesiredBalanceComputer`
decisions.

### Inter-Node Communicaton

The elected master node creates a shard allocation plan with the `DesiredBalanceShardsAllocator` and then selects incremental shard
movements towards the target allocation plan with the `DesiredBalanceReconciler`. The results of the `DesiredBalanceReconciler` is an
updated `RoutingTable`. The `RoutingTable` is part of the cluster state, so the master node updates the cluster state with the new
(incremental) desired shard allocation information. The updated cluster state is then published to the data nodes. Each data node will
observe any change in shard allocation related to itself and take action to achieve the new shard allocation by: initiating creation of a
new empty shard; starting recovery (copying) of an existing shard from another data node; or removing a shard. When the data node finishes
a shard change, a request is sent to the master node to update the shard as having finished recovery/removal in the cluster state. The
cluster state is used by allocation as a fancy work queue: the master node conveys new work to the data nodes, which pick up the work and
report back when done.

- See `DesiredBalanceShardsAllocator#submitReconcileTask` for the master node's cluster state update post-reconciliation.
- See `IndicesClusterStateService#doApplyClusterState` for the data node hook to observe shard changes in the cluster state.
- See `ShardStateAction#sendShardAction` for the data node request to the master node on completion of a shard state change.

# Autoscaling

The Autoscaling API in ES (Elasticsearch) uses cluster and node level statistics to provide a recommendation
for a cluster size to support the current cluster data and active workloads. ES Autoscaling is paired
with an ES Cloud service that periodically polls the ES elected master node for suggested cluster
changes. The cloud service will add more resources to the cluster based on Elasticsearch's recommendation.
Elasticsearch by itself cannot automatically scale.

Autoscaling recommendations are tailored for the user [based on user defined policies][], composed of data
roles (hot, frozen, etc.) and [deciders][]. There's a public [webinar on autoscaling][], as well as the
public [Autoscaling APIs] docs.

Autoscaling's current implementation is based primary on storage requirements, as well as memory capacity
for ML and frozen tier. It does not yet support scaling related to search load. Paired with ES Cloud,
autoscaling only scales upward, not downward, except for ML nodes that do get scaled up _and_ down.

[based on user defined policies]: https://www.elastic.co/guide/en/elasticsearch/reference/current/xpack-autoscaling.html
[deciders]: https://www.elastic.co/guide/en/elasticsearch/reference/current/autoscaling-deciders.html
[webinar on autoscaling]: https://www.elastic.co/webinars/autoscaling-from-zero-to-production-seamlessly
[Autoscaling APIs]: https://www.elastic.co/guide/en/elasticsearch/reference/current/autoscaling-apis.html

### Plugin REST and TransportAction entrypoints

Autoscaling is a [plugin][]. All the REST APIs can be found in [autoscaling/rest/][].
`GetAutoscalingCapacityAction` is the capacity calculation operation REST endpoint, as opposed to the
other rest commands that get/set/delete the policies guiding the capacity calculation. The Transport
Actions can be found in [autoscaling/action/], where [TransportGetAutoscalingCapacityAction][] is the
entrypoint on the master node for calculating the optimal cluster resources based on the autoscaling
policies.

[plugin]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/Autoscaling.java#L72
[autoscaling/rest/]: https://github.com/elastic/elasticsearch/tree/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/rest
[autoscaling/action/]: https://github.com/elastic/elasticsearch/tree/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/action
[TransportGetAutoscalingCapacityAction]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/action/TransportGetAutoscalingCapacityAction.java#L82-L98

### How cluster capacity is determined

[AutoscalingMetadata][] implements [Metadata.ClusterCustom][] in order to persist autoscaling policies. Each
Decider is an implementation of [AutoscalingDeciderService][]. The [AutoscalingCalculateCapacityService][]
is responsible for running the calculation.

[TransportGetAutoscalingCapacityAction.computeCapacity] is the entry point to [AutoscalingCalculateCapacityService.calculate],
which creates a [AutoscalingDeciderResults][] for [each autoscaling policy][]. [AutoscalingDeciderResults.toXContent][] then
determines the [maximum required capacity][] to return to the caller. [AutoscalingCapacity][] is the base unit of a cluster
resources recommendation.

The `TransportGetAutoscalingCapacityAction` response is cached to prevent concurrent callers
overloading the system: the operation is expensive. `TransportGetAutoscalingCapacityAction` contains
a [CapacityResponseCache][]. `TransportGetAutoscalingCapacityAction.masterOperation`
calls [through the CapacityResponseCache][], into the `AutoscalingCalculateCapacityService`, to handle
concurrent callers.

[AutoscalingMetadata]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/AutoscalingMetadata.java#L38
[Metadata.ClusterCustom]: https://github.com/elastic/elasticsearch/blob/f461731a30a6fe55d7d7b343d38426ddca1ac873/server/src/main/java/org/elasticsearch/cluster/metadata/Metadata.java#L147
[AutoscalingDeciderService]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/capacity/AutoscalingDeciderService.java#L16-L19
[AutoscalingCalculateCapacityService]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/capacity/AutoscalingCalculateCapacityService.java#L43

[TransportGetAutoscalingCapacityAction.computeCapacity]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/action/TransportGetAutoscalingCapacityAction.java#L102-L108
[AutoscalingCalculateCapacityService.calculate]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/capacity/AutoscalingCalculateCapacityService.java#L108-L139
[AutoscalingDeciderResults]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/capacity/AutoscalingDeciderResults.java#L34-L38
[each autoscaling policy]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/capacity/AutoscalingCalculateCapacityService.java#L124-L131
[AutoscalingDeciderResults.toXContent]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/capacity/AutoscalingDeciderResults.java#L78
[maximum required capacity]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/capacity/AutoscalingDeciderResults.java#L105-L116
[AutoscalingCapacity]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/capacity/AutoscalingCapacity.java#L27-L35

[CapacityResponseCache]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/action/TransportGetAutoscalingCapacityAction.java#L44-L47
[through the CapacityResponseCache]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/action/TransportGetAutoscalingCapacityAction.java#L97

### Where the data comes from

The Deciders each pull data from different sources as needed to inform their decisions. The
[DiskThresholdMonitor][] is one such data source. The Monitor runs on the master node and maintains
lists of nodes that exceed various disk size thresholds. [DiskThresholdSettings][] contains the
threshold settings with which the `DiskThresholdMonitor` runs.

[DiskThresholdMonitor]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/routing/allocation/DiskThresholdMonitor.java#L53-L58
[DiskThresholdSettings]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/routing/allocation/DiskThresholdSettings.java#L24-L27

### Deciders

The `ReactiveStorageDeciderService` tracks information that demonstrates storage limitations are causing
problems in the cluster. It uses [an algorithm defined here][]. Some examples are
- information from the `DiskThresholdMonitor` to find out whether nodes are exceeding their storage capacity
- number of unassigned shards that failed allocation because of insufficient storage
- the max shard size and minimum node size, and whether these can be satisfied with the existing infrastructure

[an algorithm defined here]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/storage/ReactiveStorageDeciderService.java#L158-L176

The `ProactiveStorageDeciderService` maintains a forecast window that [defaults to 30 minutes][]. It only
runs on data streams (ILM, rollover, etc.), not regular indexes. It looks at past [index changes][] that
took place within the forecast window to [predict][] resources that will be needed shortly.

[defaults to 30 minutes]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/storage/ProactiveStorageDeciderService.java#L32
[index changes]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/storage/ProactiveStorageDeciderService.java#L79-L83
[predict]: https://github.com/elastic/elasticsearch/blob/v8.13.2/x-pack/plugin/autoscaling/src/main/java/org/elasticsearch/xpack/autoscaling/storage/ProactiveStorageDeciderService.java#L85-L95

There are several more Decider Services, implementing the `AutoscalingDeciderService` interface.

# Snapshot / Restore

(We've got some good package level documentation that should be linked here in the intro)

(copy a sketch of the file system here, with explanation -- good reference)

### Snapshot Repository

### Creation of a Snapshot

(Include an overview of the coordination between data and master nodes, which writes what and when)

(Concurrency control: generation numbers, pending generation number, etc.)

(partial snapshots)

### Deletion of a Snapshot

### Restoring a Snapshot

### Detecting Multiple Writers to a Single Repository

# Task Management / Tracking

[TransportRequest]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/transport/TransportRequest.java
[TaskManager]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/tasks/TaskManager.java
[TaskManager#register]:https://github.com/elastic/elasticsearch/blob/6d161e3d63bedc28088246cff58ce8ffe269e112/server/src/main/java/org/elasticsearch/tasks/TaskManager.java#L125
[TaskManager#unregister]:https://github.com/elastic/elasticsearch/blob/d59df8af3e591a248a25b849612e448972068f10/server/src/main/java/org/elasticsearch/tasks/TaskManager.java#L317
[TaskId]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/tasks/TaskId.java
[Task]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/tasks/Task.java
[TaskAwareRequest]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/tasks/TaskAwareRequest.java
[TaskAwareRequest#createTask]:https://github.com/elastic/elasticsearch/blob/6d161e3d63bedc28088246cff58ce8ffe269e112/server/src/main/java/org/elasticsearch/tasks/TaskAwareRequest.java#L50
[CancellableTask]:https://github.com/elastic/elasticsearch/blob/d59df8af3e591a248a25b849612e448972068f10/server/src/main/java/org/elasticsearch/tasks/CancellableTask.java#L20
[TransportService]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/transport/TransportService.java
[Task management API]:https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html
[cat task management API]:https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-tasks.html
[TransportAction]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/TransportAction.java
[NodeClient#executeLocally]:https://github.com/elastic/elasticsearch/blob/5e8fd548b959039b6b77ad53715415b429568bc0/server/src/main/java/org/elasticsearch/client/internal/node/NodeClient.java#L100
[TaskManager#registerAndExecute]:https://github.com/elastic/elasticsearch/blob/5e8fd548b959039b6b77ad53715415b429568bc0/server/src/main/java/org/elasticsearch/tasks/TaskManager.java#L174
[RequestHandlerRegistry#processMessageReceived]:https://github.com/elastic/elasticsearch/blob/5e8fd548b959039b6b77ad53715415b429568bc0/server/src/main/java/org/elasticsearch/transport/RequestHandlerRegistry.java#L65

The tasks infrastructure is used to track currently executing operations in the Elasticsearch cluster. The [Task management API] provides an interface for querying, cancelling, and monitoring the status of tasks.

Each individual task is local to a node, but can be related to other tasks, on the same node or other nodes, via a parent-child relationship.

> [!NOTE]
> The Task management API is experimental/beta, its status and outstanding issues can be tracked [here](https://github.com/elastic/elasticsearch/issues/51628).

### Task tracking and registration

Tasks are tracked in-memory on each node in the node's [TaskManager], new tasks are registered via one of the [TaskManager#register] methods.
Registration of a task creates a [Task] instance with a unique-for-the-node numeric identifier, populates it with some metadata and stores it in the [TaskManager].

The [register][TaskManager#register] methods will return the registered [Task] instance, which can be used to interact with the task. The [Task] class is often sub-classed to include task-specific data and operations. Specific [Task] subclasses are created by overriding the [createTask][TaskAwareRequest#createTask] method on the [TaskAwareRequest] passed to the [TaskManager#register] methods.

When a task is completed, it must be unregistered via [TaskManager#unregister].

#### A note about task IDs
The IDs given to a task are numeric, supplied by a counter that starts at zero and increments over the life of the node process. So while they are unique in the individual node process, they would collide with IDs allocated after the node restarts, or IDs allocated on other nodes.

To better identify a task in the cluster scope, a tuple of persistent node ID and task ID is used. This is represented in code using the [TaskId] class and serialized as the string `{node-ID}:{local-task-ID}` (e.g. `oTUltX4IQMOUUVeiohTt8A:124`). While [TaskId] is safe to use to uniquely identify tasks _currently_ running in a cluster, it should be used with caution as it can collide with tasks that have run in the cluster in the past (i.e. tasks that ran prior to a cluster node restart).

### What Tasks Are Tracked

The purpose of tasks is to provide management and visibility of the cluster workload. There is some overhead involved in tracking a task, so they are best suited to tracking non-trivial and/or long-running operations. For smaller, more trivial operations, visibility is probably better implemented using telemetry APIs.

Some examples of operations that are tracked using tasks include:
- Execution of [TransportAction]s
  - [NodeClient#executeLocally] invokes [TaskManager#registerAndExecute]
  - [RequestHandlerRegistry#processMessageReceived] registers tasks for actions that are spawned to handle [TransportRequest]s
- Publication of cluster state updates

### Tracking a Task Across Threads and Nodes

#### ThreadContext

[ThreadContext]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/common/util/concurrent/ThreadContext.java
[ThreadPool]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/threadpool/ThreadPool.java
[ExecutorService]:https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ExecutorService.html

All [ThreadPool] threads have an associated [ThreadContext]. The [ThreadContext] contains a map of headers which carry information relevant to the operation currently being executed. For example, a thread spawned to handle a REST request will include the HTTP headers received in that request.

When threads submit work to an [ExecutorService] from the [ThreadPool], those spawned threads will inherit the [ThreadContext] of the thread that submitted them. When [TransportRequest]s are dispatched, the headers from the sending [ThreadContext] are included and then loaded into the [ThreadContext] of the thread handling the request. In these ways, [ThreadContext] is preserved across threads involved in an operation, both locally and on remote nodes.

#### Headers

[Task#HEADERS_TO_COPY]:https://github.com/elastic/elasticsearch/blob/5e8fd548b959039b6b77ad53715415b429568bc0/server/src/main/java/org/elasticsearch/tasks/Task.java#L62
[ActionPlugin#getTaskHeaders]:https://github.com/elastic/elasticsearch/blob/5e8fd548b959039b6b77ad53715415b429568bc0/server/src/main/java/org/elasticsearch/plugins/ActionPlugin.java#L99
[X-Opaque-Id API DOC]:https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html#_identifying_running_tasks

When a task is registered by a thread, a subset (defined by [Task#HEADERS_TO_COPY] and any [ActionPlugin][ActionPlugin#getTaskHeaders]s loaded on the node) of the headers from the [ThreadContext] are copied into the [Task]'s set of headers.

One such header is `X-Opaque-Id`. This is a string that [can be submitted on REST requests][X-Opaque-Id API DOC], and it will be associated with all tasks created on all nodes in the course of handling that request.

#### Parent/child relationships

[ParentTaskAssigningClient]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/client/internal/ParentTaskAssigningClient.java
[TaskAwareRequest#setParentTask]:https://github.com/elastic/elasticsearch/blob/5e8fd548b959039b6b77ad53715415b429568bc0/server/src/main/java/org/elasticsearch/tasks/TaskAwareRequest.java#L20
[TransportService#sendChildRequest]:https://github.com/elastic/elasticsearch/blob/c47162afca78f7351e30accc4857fd4bb38552b7/server/src/main/java/org/elasticsearch/transport/TransportService.java#L932

Another way to track the operations of a task is by following the parent/child relationships. When registering a task it can be optionally associated with a parent task. Generally if an executing task initiates sub-tasks, the ID of the executing task will be set as the parent of any spawned tasks (see [ParentTaskAssigningClient], [TransportService#sendChildRequest] and [TaskAwareRequest#setParentTask] for how this is implemented for [TransportAction]s).

### Kill / Cancel A Task

[TaskManager#cancelTaskAndDescendants]:https://github.com/elastic/elasticsearch/blob/5e8fd548b959039b6b77ad53715415b429568bc0/server/src/main/java/org/elasticsearch/tasks/TaskManager.java#L811
[BanParentRequestHandler]:https://github.com/elastic/elasticsearch/blob/5e8fd548b959039b6b77ad53715415b429568bc0/server/src/main/java/org/elasticsearch/tasks/TaskCancellationService.java#L356
[UnregisterChildTransportResponseHandler]:https://github.com/elastic/elasticsearch/blob/5e8fd548b959039b6b77ad53715415b429568bc0/server/src/main/java/org/elasticsearch/transport/TransportService.java#L1763
[Cancel Task REST API]:https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html#task-cancellation
[RestCancellableNodeClient]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/rest/action/RestCancellableNodeClient.java
[TaskCancelledException]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/tasks/TaskCancelledException.java

Some long-running tasks are implemented to be cancel-able. Cancellation of a task and its descendants can be done via the [Cancel Task REST API] or programmatically using [TaskManager#cancelTaskAndDescendants]. Perhaps the most common use of cancellation you will see is cancellation of [TransportAction]s dispatched from the REST layer when the client disconnects, to facilitate this we use the [RestCancellableNodeClient].

In order to support cancellation, the [Task] instance associated with the task must extend [CancellableTask]. It is the job of any workload tracked by a [CancellableTask] to periodically check whether it has been cancelled and, if so, finish early. We generally wait for the result of a cancelled task, so tasks can decide how they complete upon being cancelled, typically it's exceptionally with [TaskCancelledException].

When a [Task] extends [CancellableTask] the [TaskManager] keeps track of it and any child tasks that it spawns. When the task is cancelled, requests are sent to any nodes that have had child tasks submitted to them to ban the starting of any further children of that task, and any cancellable child tasks already running are themselves cancelled (see [BanParentRequestHandler]).

When a cancellable task dispatches child requests through the [TransportService], it registers a proxy response handler that will instruct the remote node to cancel that child and any lingering descendants in the event that it completes exceptionally (see [UnregisterChildTransportResponseHandler]). A typical use-case for this is when no response is received within the time-out, the sending node will cancel the remote action and complete with a timeout exception.

### Publishing Task Results

[TaskResult]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/tasks/TaskResult.java
[TaskResultsService]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/tasks/TaskResultsService.java
[CAT]:https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html
[ActionRequest]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/ActionRequest.java
[ActionRequest#getShouldStoreResult]:https://github.com/elastic/elasticsearch/blob/b633fe1ccb67f7dbf460cdc087eb60ae212a472a/server/src/main/java/org/elasticsearch/action/ActionRequest.java#L32
[TaskResultStoringActionListener]:https://github.com/elastic/elasticsearch/blob/b633fe1ccb67f7dbf460cdc087eb60ae212a472a/server/src/main/java/org/elasticsearch/action/support/TransportAction.java#L149

A list of tasks currently running in a cluster can be requested via the [Task management API], or the [cat task management API]. The former returns each task represented using [TaskResult], the latter returning a more compact [CAT] representation.

Some [ActionRequest]s allow the results of the actions they spawn to be stored upon completion for later retrieval. If [ActionRequest#getShouldStoreResult] returns true, a [TaskResultStoringActionListener] will be inserted into the chain of response listeners. [TaskResultStoringActionListener] serializes the [TaskResult] of the [TransportAction] and persists it in the `.tasks` index using the [TaskResultsService].

The [Task management API] also exposes an endpoint where a task ID can be specified, this form of the API will return currently running tasks, or completed tasks whose results were persisted. Note that although we use [TaskResult] to return task information from all the JSON APIs, the `error` or `response` fields will only ever be populated for stored tasks that are already completed.

### Persistent Tasks

[PersistentTaskPlugin]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/plugins/PersistentTaskPlugin.java
[PersistentTasksExecutor]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/persistent/PersistentTasksExecutor.java
[PersistentTasksExecutor.Scope.Cluster]:https://github.com/elastic/elasticsearch/blob/f461731a30a6fe55d7d7b343d38426ddca1ac873/server/src/main/java/org/elasticsearch/persistent/PersistentTasksExecutor.java#L52
[PersistentTasksExecutor.Scope.Project]:https://github.com/elastic/elasticsearch/blob/f461731a30a6fe55d7d7b343d38426ddca1ac873/server/src/main/java/org/elasticsearch/persistent/PersistentTasksExecutor.java#L48
[PersistentTasksExecutorRegistry]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/persistent/PersistentTasksExecutorRegistry.java
[PersistentTasksNodeService]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/persistent/PersistentTasksNodeService.java
[PersistentTasksClusterService]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/persistent/PersistentTasksClusterService.java
[AllocatedPersistentTask]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/persistent/AllocatedPersistentTask.java
[ShardFollowTasksExecutor]:https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/ccr/src/main/java/org/elasticsearch/xpack/ccr/action/ShardFollowTasksExecutor.java
[HealthNodeTaskExecutor]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/health/node/selection/HealthNodeTaskExecutor.java
[SystemIndexMigrationExecutor]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/upgrades/SystemIndexMigrationExecutor.java
[PersistentTasksCustomMetadata]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/persistent/PersistentTasksCustomMetadata.java
[ClusterPersistentTasksCustomMetadata]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/persistent/ClusterPersistentTasksCustomMetadata.java
[PersistentTasksCustomMetadata.PersistentTask]:https://github.com/elastic/elasticsearch/blob/d466ad1c3c4cedc7d5f6ab5794abe7bfd72aef4e/server/src/main/java/org/elasticsearch/persistent/PersistentTasksCustomMetadata.java#L305

Up until now we have discussed only ephemeral tasks. If we want a task to survive node failures, it needs to be registered as a persistent task at the cluster level.

Plugins can register persistent tasks definitions by implementing [PersistentTaskPlugin] and returning one or more [PersistentTasksExecutor] instances. These are collated into a [PersistentTasksExecutorRegistry] which is provided to [PersistentTasksNodeService] active on each node in the cluster, and a [PersistentTasksClusterService] active on the master. A [PersistentTasksExecutor] can declare either [project][PersistentTasksExecutor.Scope.Project] or [cluster][PersistentTasksExecutor.Scope.Cluster] scope, but not both. A project scope task is not able to access data on a different project.

The [PersistentTasksClusterService] runs on the master to manage the set of running persistent tasks. It periodically checks that all persistent tasks are assigned to live nodes and handles the creation, completion, removal and updates-to-the-state of persistent task instances in the cluster state (see [PersistentTasksCustomMetadata] and [ClusterPersistentTasksCustomMetadata]).

The [PersistentTasksNodeService] monitors the cluster state to:
 - Start any tasks allocated to it (tracked in the local [TaskManager] by an [AllocatedPersistentTask])
 - Cancel any running tasks that have been removed ([AllocatedPersistentTask] extends [CancellableTask])

If a node leaves the cluster while it has a persistent task allocated to it, the master will re-allocate that task to a surviving node. To do this, it creates a new [PersistentTasksCustomMetadata.PersistentTask] entry with a higher `#allocationId`. The allocation ID is included any time the [PersistentTasksNodeService] communicates with the [PersistentTasksClusterService] about the task, it allows the [PersistentTasksClusterService] to ignore persistent task messages originating from stale allocations.

Some examples of the use of persistent tasks include:
 - [ShardFollowTasksExecutor]: Defined by [cross-cluster replication](#cross-cluster-replication-ccr) to poll a remote cluster for updates
 - [HealthNodeTaskExecutor]: Used to schedule work related to monitoring cluster health. This is currently the only example of a cluster scope persistent task.
 - [SystemIndexMigrationExecutor]: Manages the migration of system indices after an upgrade

### Integration with APM

[Traceable]:https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/telemetry/tracing/Traceable.java
[APM Spans]:https://www.elastic.co/guide/en/observability/current/apm-data-model-spans.html

Tasks are integrated with the ElasticSearch APM infrastructure. They implement the [Traceable] interface, and [spans][APM Spans] are published to represent the execution of each task.

# Cross Cluster Replication (CCR)

(Brief explanation of the use case for CCR)

(Explain how this works at a high level, and details of any significant components / ideas.)

### Cross Cluster Search

# Indexing / CRUD

(Explain that the Distributed team is responsible for the write path, while the Search team owns the read path.)

(Generating document IDs. Same across shard replicas, \_id field)

(Sequence number: different than ID)

### Reindex

### Locking

(what limits write concurrency, and how do we minimize)

### Soft Deletes

### Refresh

(explain visibility of writes, and reference the Lucene section for more details (whatever makes more sense explained there))

# Server Startup

# Server Shutdown

### Closing a Shard

(this can also happen during shard reallocation, right? This might be a standalone topic, or need another section about it in allocation?...)
