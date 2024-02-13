# Distributed Area Team Internals

(Summary, brief discussion of our features)

# Networking

### ThreadPool

(We have many thread pools, what and why)

### ActionListener

Callbacks are used extensively throughout Elasticsearch because they enable us to write asynchronous and nonblocking code, i.e. code which
doesn't necessarily compute a result straight away but also doesn't block the calling thread waiting for the result to become available.
Callbacks can be passed into methods for the method to perhaps complete straight away, or instead perhaps to store in a data structure and
complete at some later time, or (most commonly) pass along down into some other method which takes a callback argument, possibly after
modifying its behaviour slightly by wrapping it in another callback.

`ActionListener` is a general-purpose callback that is used extensively across the Elasticsearch codebase. It's an important feature of the
Elasticsearch codebase that `ActionListener` is used pretty much everywhere that needs to perform some asynchronous and nonblocking
computation: the uniformity makes it easier to compose parts of the system together without needing to build adapters to convert back and
forth between different kinds of callback. It also makes it easier to develop the skills needed to read and understand all the asynchronous
code, although it definitely takes practice and is certainly not easy in an absolute sense. Finally, it has allowed us to build a rich
library for working with `ActionListener` instances themselves, creating new instances out of existing ones and completing them in
interesting ways. See for instance:

- all the static methods on [ActionListener](https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/ActionListener.java) itself
- [`ThreadedActionListener`](https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/ThreadedActionListener.java) for forking work elsewhere
- [`RefCountingListener`](https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/RefCountingListener.java) for running work in parallel
- [`SubscribableListener`](https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/SubscribableListener.java) for constructing flexible workflows

Callback-based asynchronous code can easily call regular synchronous code, but synchronous code cannot run callback-based asynchronous code
without blocking the calling thread until the continuation is executed. This blocking is at best undesirable (threads are too expensive to
waste with unnecessary blocking) and at worst outright broken (the blocking can lead to deadlock). Unfortunately this means that most of our
code ends up having to be written with callbacks, simply because it's ultimately calling into some other code that takes a callback. The
entry points for all Elasticsearch APIs are callback-based, and ultimately work in terms of an event loop which processes network events via
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
proceeding. Some languages have first-class support for continuations (e.g. the `async` and `await` primitives in C#) allowing the
programmer to write code in direct style away from those exotic control structures, but Java does not. That's why we have to manipulate all
the callbacks ourselves.

This pattern is often used in the transport action layer with the use of the
[ChannelActionListener](https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/support/ChannelActionListener.java)
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

(Reactive and proactive autoscaling. Explain that we surface recommendations, how control plane uses it.)

(Sketch / list the different deciders that we have, and then also how we use information from each to make a recommendation.)

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
