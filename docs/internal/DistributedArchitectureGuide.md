# Distributed Area Internals

The Distributed Area contains Indexing and Coordination.

Indexing covers the index path, stretching from the user REST command through shard routing down to the translog and storage
engine. Reindexing is effectively reading from a source index and writing to a destination index. Shard recovery and cross
cluster replication also fall under indexing.

Coordination encompasses the distributed components relating to cluster coordination, networking, shard allocation decisions
(i.e. the balancer algorithms), cluster autoscaling stats, the discovery plugin system, task management, and snapshot/restore.

A guide to the general Elasticsearch components can be found [here](https://github.com/elastic/elasticsearch/blob/main/docs/internal/GeneralArchitectureGuide.md).

# Networking

### ThreadPool

(We have many thread pools, what and why)

### ActionListener

Callbacks are used extensively throughout Elasticsearch because they enable us to write asynchronous and nonblocking code, i.e. code which
doesn't necessarily compute a result straight away but also doesn't block the calling thread waiting for the result to become available.
They support several useful control flows:

- They can be completed immediately on the calling thread.
- They can be completed concurrently on a different thread.
- They can be stored in a data structure and completed later on when the system reaches a particular state.
- Most commonly, they can be passed on to other methods that themselves require a callback.
- They can be wrapped in another callback which modifies the behaviour of the original callback, perhaps adding some extra code to run
  before or after completion, before passing them on.

`ActionListener` is a general-purpose callback interface that is used extensively across the Elasticsearch codebase. `ActionListener` is
used pretty much everywhere that needs to perform some asynchronous and nonblocking computation. The uniformity makes it easier to compose
parts of the system together without needing to build adapters to convert back and forth between different kinds of callback. It also makes
it easier to develop the skills needed to read and understand all the asynchronous code, although this definitely takes practice and is
certainly not easy in an absolute sense. Finally, it has allowed us to build a rich library for working with `ActionListener` instances
themselves, creating new instances out of existing ones and completing them in interesting ways. See for instance:

- all the static methods on [ActionListener](https://github.com/elastic/elasticsearch/blob/v8.12.2/server/src/main/java/org/elasticsearch/action/ActionListener.java) itself
- [`ThreadedActionListener`](https://github.com/elastic/elasticsearch/blob/v8.12.2/server/src/main/java/org/elasticsearch/action/support/ThreadedActionListener.java) for forking work elsewhere
- [`RefCountingListener`](https://github.com/elastic/elasticsearch/blob/v8.12.2/server/src/main/java/org/elasticsearch/action/support/RefCountingListener.java) for running work in parallel
- [`SubscribableListener`](https://github.com/elastic/elasticsearch/blob/v8.12.2/server/src/main/java/org/elasticsearch/action/support/SubscribableListener.java) for constructing flexible workflows

Callback-based asynchronous code can easily call regular synchronous code, but synchronous code cannot run callback-based asynchronous code
without blocking the calling thread until the callback is called back. This blocking is at best undesirable (threads are too expensive to
waste with unnecessary blocking) and at worst outright broken (the blocking can lead to deadlock). Unfortunately this means that most of our
code ends up having to be written with callbacks, simply because it's ultimately calling into some other code that takes a callback. The
entry points for all Elasticsearch APIs are callback-based (e.g. REST APIs all start at
[`org.elasticsearch.rest.BaseRestHandler#prepareRequest`](https://github.com/elastic/elasticsearch/blob/v8.12.2/server/src/main/java/org/elasticsearch/rest/BaseRestHandler.java#L158-L171),
and transport APIs all start at
[`org.elasticsearch.action.support.TransportAction#doExecute`](https://github.com/elastic/elasticsearch/blob/v8.12.2/server/src/main/java/org/elasticsearch/action/support/TransportAction.java#L65))
and the whole system fundamentally works in terms of an event loop (a `io.netty.channel.EventLoop`) which processes network events via
callbacks.

`ActionListener` is not an _ad-hoc_ invention. Formally speaking, it is our implementation of the general concept of a continuation in the
sense of [_continuation-passing style_](https://en.wikipedia.org/wiki/Continuation-passing_style) (CPS): an extra argument to a function
which defines how to continue the computation when the result is available. This is in contrast to _direct style_ which is the more usual
style of calling methods that return values directly back to the caller so they can continue executing as normal. There's essentially two
ways that computation can continue in Java (it can return a value or it can throw an exception) which is why `ActionListener` has both an
`onResponse()` and an `onFailure()` method.

CPS is strictly more expressive than direct style: direct code can be mechanically translated into continuation-passing style, but CPS also
enables all sorts of other useful control structures such as forking work onto separate threads, possibly to be executed in parallel,
perhaps even across multiple nodes, or possibly collecting a list of continuations all waiting for the same condition to be satisfied before
proceeding (e.g.
[`SubscribableListener`](https://github.com/elastic/elasticsearch/blob/v8.12.2/server/src/main/java/org/elasticsearch/action/support/SubscribableListener.java)
amongst many others). Some languages have first-class support for continuations (e.g. the `async` and `await` primitives in C#) allowing the
programmer to write code in direct style away from those exotic control structures, but Java does not. That's why we have to manipulate all
the callbacks ourselves.

Strictly speaking, CPS requires that a computation _only_ continues by calling the continuation. In Elasticsearch, this means that
asynchronous methods must have `void` return type and may not throw any exceptions. This is mostly the case in our code as written today,
and is a good guiding principle, but we don't enforce void exceptionless methods and there are some deviations from this rule. In
particular, it's not uncommon to permit some methods to throw an exception, using things like
[`ActionListener#run`](https://github.com/elastic/elasticsearch/blob/v8.12.2/server/src/main/java/org/elasticsearch/action/ActionListener.java#L381-L390)
(or an equivalent `try ... catch ...` block) further up the stack to handle it. Some methods also take (and may complete) an
`ActionListener` parameter, but still return a value separately for other local synchronous work.

This pattern is often used in the transport action layer with the use of the
[ChannelActionListener](https://github.com/elastic/elasticsearch/blob/v8.12.2/server/src/main/java/org/elasticsearch/action/support/ChannelActionListener.java)
class, which wraps a `TransportChannel` produced by the transport layer. `TransportChannel` implementations can hold a reference to a Netty
channel with which to pass the response back to the network caller. Netty has a many-to-one association of network callers to channels, so a
call taking a long time generally won't hog resources: it's cheap. A transport action can take hours to respond and that's alright, barring
caller timeouts.

(TODO: add useful starter references and explanations for a range of Listener classes. Reference the Netty section.)

### REST Layer

(including how REST and Transport layers are bound together through the ActionModule)

### Transport Layer

### Chunk Encoding

#### XContent

### Performance

### Netty

(long running actions should be forked off of the Netty thread. Keep short operations to avoid forking costs)

### Work Queues

# Cluster Coordination

(Sketch of important classes? Might inform more sections to add for details.)

(A NodeB can coordinate a search across several other nodes, when NodeB itself does not have the data, and then return a result to the caller. Explain this coordinating role)

### Node Roles

### Master Nodes

### Master Elections

(Quorum, terms, any eligibility limitations)

### Cluster Formation / Membership

(Explain joining, and how it happens every time a new master is elected)

#### Discovery

### Master Transport Actions

### Cluster State

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

(Explain checkpointing and generations, when happens on Lucene flush / fsync)

(Concurrency control for flushing)

(VersionMap)

#### Translog Truncation

#### Direct Translog Read

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

(AllocationService runs on the master node)

(Discuss different deciders that limit allocation. Sketch / list the different deciders that we have.)

### APIs for Balancing Operations

(Significant internal APIs for balancing a cluster)

### Heuristics for Allocation

### Cluster Reroute Command

(How does this command behave with the desired auto balancer.)

# Autoscaling

The Autoscaling API in ES (Elasticsearch) uses cluster and node level statistics to provide a recommendation
for a cluster size to support the current cluster data and active workloads. ES Autoscaling is paired
with an ES Cloud service that periodically polls the ES elected master node for suggested cluster
changes. The cloud service will add more resources to the cluster based on Elasticsearch's recommendation.
Elasticsearch by itself cannot automatically scale.

Autoscaling recommendations are tailored for the user [based on user defined policies][], composed of data
roles (hot, frozen, etc) and [deciders][]. There's a public [webinar on autoscaling][], as well as the
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

[AutoscalingMetadata][] implements [Metadata.Custom][] in order to persist autoscaling policies. Each
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
[Metadata.Custom]: https://github.com/elastic/elasticsearch/blob/v8.13.2/server/src/main/java/org/elasticsearch/cluster/metadata/Metadata.java#L141-L145
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
runs on data streams (ILM, rollover, etc), not regular indexes. It looks at past [index changes][] that
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

(How we identify operations/tasks in the system and report upon them. How we group operations via parent task ID.)

### What Tasks Are Tracked

### Tracking A Task Across Threads

### Tracking A Task Across Nodes

### Kill / Cancel A Task

### Persistent Tasks

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
