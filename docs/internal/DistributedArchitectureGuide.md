# Distributed Area Internals

The Distributed Area contains indexing and coordination systems.

The index path stretches from the user REST command through shard routing down to each individual shard's translog and storage
engine. Reindexing is effectively reading from a source index and writing to a destination index (perhaps on different nodes).
The coordination side includes cluster coordination, shard allocation, cluster autoscaling stats, task management, and cross
cluster replication. Less obvious coordination systems include networking, the discovery plugin system, the snapshot/restore
logic, and shard recovery.

A guide to the general Elasticsearch components can be found [here](https://github.com/elastic/elasticsearch/blob/main/docs/internal/GeneralArchitectureGuide.md).

# Networking

Every elasticsearch node maintains various networking clients and servers,
protocols, and synchronous/asynchronous handling. Our public docs cover user
facing settings and some internal aspects - [Network Settings](https://www.elastic.co/docs/reference/elasticsearch/configuration-reference/networking-settings).

## HTTP Server

The HTTP Server is a single entry point for all external clients (excluding
cross-cluster communication). Management, ingestion, search, and all other
external operations pass through the HTTP server.

Elasticsearch works over HTTP 1.1 and supports features such as TLS, chunked
transfer encoding, content compression, and pipelining. While attempting to
be HTTP spec compliant, Elasticsearch is not a webserver. ES Supports `GET`
requests with a payload (though some old proxies may drop content) and
`POST` for clients unable to send `GET-with-body`. Requests cannot be cached
by middle boxes.

There is no connection limit, but a limit on payload size exists. The default
maximum payload is 100MB after compression. It's a very large number and almost
never a good target that the client should approach. See
`HttpTransportSettings` class.

Security features, including basic security: authentication(authc),
authorization(authz), Transport Layer Security (TLS) are available in the free
tier and achieved with separate x-pack modules.

The HTTP server provides two options for content processing: full aggregation
and incremental processing. Aggregated content is a preferable choice for small
messages that do not fit for incremental parsing (e.g., JSON). Aggregation has
drawbacks: it requires more memory, which is reserved until all bytes are
received. Concurrent incomplete requests can lead to unbounded memory growth and
potential OOMs. Large delimited content, such as bulk indexing, which is
processed in byte chunks, provides better control over memory usage but is more
complicated for application code.

Incremental bulk indexing includes a back-pressure feature. See `org.
elasticsearch.index.IndexingPressure`. When memory pressure grows high
(`LOW_WATERMARK`), reading bytes from TCP sockets is paused for some
connections, allowing only a few to proceed until the pressure is resolved.
When memory grows too high (`HIGH_WATERMARK`) bulk items are rejected with 429.
This mechanism protects against unbounded memory usage and `OutOfMemory`
errors (OOMs).

ES supports multiple `Content-Type`s for the payload. These are
implementations of `MediaType` interface. A common implementation is called
`XContentType`, including CBOR, JSON, SMILE, YAML, and their versioned types.
X-pack extensions includes PLAIN_TEXT, CSV, etc. Classes that implement
`ToXContent` and friends can be serialized and sent over HTTP.

HTTP routing is based on a combination of Method and URI. For example,
`RestCreateIndexAction` handler uses `("PUT", "/{index}")`, where curly braces
indicate path variables. `RestBulkAction` specifies a list of routes

```java
@Override
  public List<Route> routes() {
    return List.of(
      new Route(POST, "/_bulk"),
      new Route(PUT, "/_bulk"),
      new Route(POST, "/{index}/_bulk"),
      new Route(PUT, "/{index}/_bulk")
    );
  }
```

Every REST handler must be declared in the `ActionModule` class in the
`initRestHandlers` method. Plugins implementing `ActionPlugin` can extend the
list of handlers via the `getRestHandlers` override. Every REST handler
should extend `BaseRestHandler`.

The REST handler’s job is to parse and validate the HTTP request and construct a
typed version of the request, often a Transport request. When security is
enabled, the HTTP layer handles authentication (based on headers), and the
Transport layer handles authorization.

Request handling flow from Java classes view goes as:

```
(if security enabled) Security.getHttpServerTransportWithHeadersValidator
-> `Netty4HttpServerTransport`
-> `AbstractHttpServerTransport`
-> `RestController`
-> `BaseRestHandler`
-> `Rest{Some}Action`
```

`Netty4HttpServerTransport` is a single implementation of
`AbstractHttpServerTransport` from the `transport-netty4`
module. The `x-pack/security` module injects TLS and headers validator.

## Transport

Transport is the term for node-to-node communication, utilizing a TCP-based
custom binary protocol. Every node acts as both a client and a server.
Node-to-node communication almost never uses HTTP transport (except for
reindex-from-remote).

`Netty4Transport` is the sole implementation of TCP transport, initializing
both the Transport client and server. The `x-pack/security` plugin provides
a secure version: `SecurityNetty4Transport` (with TLS and authentication).

A `Connection` between nodes is a pool of `Channel`s, where each channel is a
non-blocking TCP connection (Java NIO terminology). Once a cluster is
discovered, a `Connection` (pool of `Channel`s) is opened to every other node,
and every other node opens a `Connection` back. This results in two
`Connection`s between any two nodes `(A→B and B→A)`. A node sends requests only
on the `Connection` it opens (acting as a client). The default pool is around 13
`Channel`s, divided into sub-pools for different purposes (e.g., ping,
node-state, bulks). The pool structure is defined in the `ConnectionProfile`
class.

ES never behaves incorrectly (e.g. loses data) in the face of network outages
but it may become unavailable unless the network is stable. Network stability
between nodes is assumed, though connectivity issues remain a constant
challenge.

Request timeouts are discouraged, as Transport requests are guaranteed to
eventually receive a response, even without a timeout. `SO_KEEPALIVE` helps
detect and tear down dead connections. When a connection closes with an error,
the entire pool is closed, outstanding requests fail, and the pool is
reconnected.

There are no retries on the Transport layer itself. The application layer
decides when and how to retry (e.g., via `RetryableAction` or
`TransportMasterNodeAction`). In the future Transport framework might support
retries #95100.

Transport can multiplex requests and responses in a single `Channel`, but
cannot multiplex parts of messages. Each transport message must be fully
dispatched before the next can be sent. Proper application-layer sizing/chunking
of messages is recommended to ensure fairness of delivery across multiple
senders. A Transport message cannot be larger than 30% of heap (
`org.elasticsearch.transport.TcpTransport#THIRTY_PER_HEAP_SIZE`) or 2GB (due to
`org.elasticsearch.transport.Header#networkMessageSize` being an `int`).

The `TransportMessage` family tree includes various types (node-to-node,
broadcast, master node acknowledged) to ensure correct dispatch and response
handling. For example when a message must be accepted on all nodes.

## Other networking stacks

Snapshotting to remote repositories involves different networking clients
and SDKs. For example AWS SDK comes with Apache or Netty HTTP client, Azure
with Netty-based Project-Reactor, GCP uses default Java HTTP client.
Underlying clients may be reused between repositories, with varying levels of
control over networking settings.

There are other features such as SAML/JWT metadata reloading, Watcher HTTP
action, reindex and ML related features such as inference that also use HTTP
clients.

## Sync/Async IO and threading

ES handles a mix of I/O operations (disk, HTTP server,
Transport client/server, repositories), resulting in a combination of
synchronous and asynchronous styles. Asynchronous IO utilizes a small set of
threads by running small tasks, minimizing context switch. Synchronous IO
uses many threads and relies on an OS scheduler. ES typically runs with 100+
threads, where Async and Sync threads compete for resources.

## Netty

Netty is a networking framework/toolkit used extensively for HTTP and Transport
networks, providing foundational building blocks for networking applications.

### Event-Loop (Transport-Thread)

Netty is an Async IO framework, it runs with a few threads. An event-loop is
a thread that processes events for one or many `Channels` (TCP connections).
Every `Channel` has exactly one, unchanging event-loop, eliminating the need to
synchronize events within that `Channel`. A single, CPU-bound `Transport
ThreadPool` (e.g.,4 threads for 4 cores) serves all HTTP and Transport
servers and clients, handling potentially hundreds or thousands of connections.

Event-loop threads serve many connections each, it's critical to not block
threads for a long time. Fork any blocking operation or heavy computation to
another thread pool. Forking, however, comes with overhead. Do not fork
simple requests that can be served from memory and do not require heavy
computations (milliseconds).

Transport threads are monitored by `ThreadWatchdog`. A warning log appears if a
single task runs longer than 5 seconds. Slowness can be caused by blocking, GC
pauses, or CPU starvation from other thread pools.

### ByteBuf - byte buffers and reference counting

Netty's controlled memory allocation provides a performance edge by managing and
reusing byte buffer pools (e.g., pools of 1MiB byte chunks sliced into 16KiB
pages). Some pages might not be in use while taking up heap space and show up in
the heap dump.

Netty reads socket bytes into direct buffers, and ES copies them into pooled
byte-buffers (`CopyBytesSocketChannel`). The application is responsible for
retaining (increasing ref-count) and releasing (decreasing ref-count) for
pooled buffers.

Reference counting introduces two primary problems:

1. Use after release (free): Accessing a buffer after it has been explicitly
   released.
2. Never release (leak): Failing to release a buffer, leading to memory leaks.

The compiler does not help detect these issues. They require careful testing
using Netty's LeakDetector with a Paranoid level. It's enabled by default in
all tests.

### Async methods return futures

Every asynchronous operation in Netty returns a future. It is easy to forget
to check the result, as a following call always succeeds:

```java
ctx.write(message)
```

Check the result of an async operation:

```java
ctx.write(message).addListener(f -> { if (f.isSuccess() ...)});
```

### ThreadPool

(We have many thread pools, what and why)

### ActionListener

See the [Javadocs for `ActionListener`](https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/action/ActionListener.java)

(TODO: add useful starter references and explanations for a range of Listener classes. Reference the Netty section.)

### Chunk Encoding

#### XContent

### Performance

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

(Majority consensus to apply, what happens if a master-eligible node falls behind / is incommunicado.)

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
This process involves multiple writes to the translog before the next fsync(), and this is done so that we amortize the cost of the translog's fsync() operations across all writes.

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

When a shard is created on a node, it starts out empty. *Recovery* is the process of loading the shard's data from
some data source into the newly created `IndexShard` in order to make it available for index or search requests.
When the shard allocation process has chosen a node for a shard, it records its choice by writing an updated [ShardRouting][]
record into the cluster state's [IndexRoutingTable][]. The [ShardRouting][] entry includes [RecoverySource][] metadata that
describes where the shard's data can be found based on the shard's previous allocation. For example, a shard for a newly created
index will have its `recoverySource` set to `EMPTY_STORE` to indicate that recovery should bring up the shard without loading
any existing data, while a `recoverySource` of `EXISTING_STORE` would tell recovery to load the shard from files already
present on disk, likely because the node was restarted and had hosted the shard until it shut down.

The `IndicesClusterStateService` on each node listens for updates to the `IndexRoutingTable` and when it finds that a
shard in state `INITIALIZING` has been assigned to its node, it creates a fresh `IndexShard` for the assigned shard and kicks off a recovery process
for that node, using the [RecoverySource][] in the [ShardRouting][] entry to determine the parameters of the recovery process.
The full list of recovery types is defined in [RecoverySource.Type][]. The various modes are discussed below, roughly in
order of complexity. Some modes build on others; for example, snapshot recovery sets up a local data store by copying
files from a snapshot source and then uses `EXISTING_STORE` recovery. Similarly, if there is any local data, then
peer recovery starts by using  `EXISTING_STORE` recovery to bring the local shard as close to up to date as it can, and then
finishes synchronizing the shard through RPCs (Remote Procedure Calls) to an active source shard.

At the end of the recovery process, recovery finalization marks the shard as `STARTED` in cluster state, which makes it
available to handle index and search requests.

### Create a New Shard

The simplest form of recovery is `EMPTY_STORE`, which is just what it sounds like. This recovery type causes the recovery
process to invoke [StoreRecovery] for the recovery. `StoreRecovery` will create an empty shard directory on disk (deleting
any existing files that may be present) and bootstrap an empty translog and then tell the Engine to use that directory.
That's pretty much the whole process.

### Restore from an Existing Directory

Only slightly more complex is `EXISTING_STORE` recovery, which is used when the node is expected to have an up-to-date
copy of the shard's data already present on disk. Once again `StoreRecovery` manages the recovery process, but in this
case it expects to find Lucene files and a translog already present. It validates the Lucene files, potentially dropping
any incomplete commits that may not have been fsynced to disk before shutdown, tells the engine to open the directory
as its backing store, and then replays the transaction log to replay any operations that may have been acknowledged to
the client but not written into a durable Lucene commit. At that point the shard is ready to serve requests.

### Snapshot Recovery

In snapshot recovery, the data source is a snapshot of the index shard stored on a remote repository. Snapshot recovery,
also managed in `StoreRecovery` but invoked through the `recoverFromRepository` method, downloads and unpacks the snapshot
from the remote repository into the local shard directory and then invokes the same logic as `EXISTING_STORE` to bring
the shard on line, with some small differences. The key one is that the snapshot is taken from a Lucene commit, and
so it does not need to store the shard translog when the snapshot is taken, or restore it during recovery. Instead
it creates a new empty translog before bringing the shard on line.

### Local Shards Recovery
Local shards recovery is a type of recovery that reuses existing data from other shard(s) allocated on the current node (hence local shards). It is used exclusively to implement index [Shrink/Split/Clone APIs](#shrinksplitclone-index-apis).

This recovery type uses `HardlinkCopyDirectoryWrapper` to hard link or copy data from the source shard(s) directory. Copy is used if the runtime environment does not support hard links (e.g., on Windows). Source shard(s) directories are added using the `IndexWriter#addIndexes` API. Once an `IndexWriter` is correctly set up with source shard(s), the necessary data modifications are performed (like deleting excess documents during split) and a new commit is created for the recovering shard. After that recovery proceeds using standard store recovery logic utilizing the commit that was just created.

### Peer Recovery

Whereas the other recovery modes are used to bring up a *primary* shard, peer recovery is used to add replicas of an
already active primary shard. Peer recovery is also used for primary shard relocation, but relocation goes through
the standard peer recovery process to bring a replica in sync, before handing off the primary role to the recovered
replica during finalization.

Unlike `StoreRecovery`, peer recovery is managed through a separate service on the node recovering the shard, the
[PeerRecoveryTargetService][]. When the IndexShard sees that its recovery source is of type `PEER`, it hands over the
recovery process to `PeerRecoveryTargetService` by invoking its `startRecovery` method. This service begins by creating
an in-memory record of the recovery process to track its progress, and then runs `EXISTING_STORE` recovery in case the
recovering replica held a copy of the shard before that has gone out of sync (e.g., because the node holding the
replica restarted). Because the shard is a replica, it only recovers up to the latest known global checkpoint for the shard
and discards any operations in the local store that are ahead of that point (see [Translog][#Translog] for details).
Once the local shard has been brought close to current, the service then sends a request to a corresponding service on the source node, `PeerRecoverySourceService`,
to complete synchronization.

Synchronization begins by discovering any differences between the source and target shards and transmitting any missing files
to the target shard. The source for the files can be the source shard itself, but if a snapshot of the shard is available
that has some subset of the files to be transmitted, then recovery will fetch them from the snapshot in order to reduce
load on the source shard.

The next step is to transfer any operations from the source translog. Since the source shard is active,
it may be receiving index operations while recovery is in process. So, to ensure that the target shard doesn't miss any
new operations, the source shard adds the target to the shard's replication group (see the [replication][#Replication] docs)
*before* completing the operation transfer phase. Because of this ordering, any operations accepted on the shard between the
time it reads and sends the latest operation in the translog and the time the replica completes recovery are sent through the
request replication process and will not be lost. Once the target has been added to the recovery group, the source reads the
latest sequence number from its transaction log knowing that any updates past that will be handled by recovery, and replays
the translog to the target up to that point.

At this point the target is ready to be started as an in sync replica. However, peer recovery is also used to perform
primary relocation. If the target shard is being recovered in order to take over as primary, then the finalization
stage will call `IndexShard.relocate` to complete the handoff of primary responsibilities. This method blocks operations
on the source shard and sends an RPC to the target shard with the [ReplicationTracker.PrimaryContext][] needed
to promote the target to primary. Once the target acknowledges the handoff, the source shard moves itself into
replica mode.

[ShardRouting]:https://github.com/elastic/elasticsearch/blob/1d4a20ae194ce71fd5819786ba6dfb154ceb123f/server/src/main/java/org/elasticsearch/cluster/routing/ShardRouting.java
[IndexRoutingTable]:https://github.com/elastic/elasticsearch/blob/473c4da497681c889728c05cebb27030ae97fc13/server/src/main/java/org/elasticsearch/cluster/routing/IndexRoutingTable.java
[RecoverySource]:https://github.com/elastic/elasticsearch/blob/5346213ade708c63021824ad70cc3fa89f1ea307/server/src/main/java/org/elasticsearch/cluster/routing/RecoverySource.java
[RecoverySource.Type]:https://github.com/elastic/elasticsearch/blob/5346213ade708c63021824ad70cc3fa89f1ea307/server/src/main/java/org/elasticsearch/cluster/routing/RecoverySource.java#L79
[IndicesClusterStateService]:https://github.com/elastic/elasticsearch/blob/5346213ade708c63021824ad70cc3fa89f1ea307/server/src/main/java/org/elasticsearch/indices/cluster/IndicesClusterStateService.java
[StoreRecovery]:https://github.com/elastic/elasticsearch/blob/d70878f488dfa2e2ba4d02e335c15be7cd4d5af2/server/src/main/java/org/elasticsearch/index/shard/StoreRecovery.java
[PeerRecoveryTargetService]:https://github.com/elastic/elasticsearch/blob/5346213ade708c63021824ad70cc3fa89f1ea307/server/src/main/java/org/elasticsearch/indices/recovery/PeerRecoveryTargetService.java
[ReplicationTracker.PrimaryContext]:https://github.com/elastic/elasticsearch/blob/1352df3f0b5157ca1d730428ea5aba2a7644e79b/server/src/main/java/org/elasticsearch/index/seqno/ReplicationTracker.java#L1573

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

### Inter-Node Communication

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

Snapshot copies index data files from node local storage to a remote repository. These files can later be restored
from the repository back to local storage to re-create the index. In addition to indices, it can also backup and
restore the cluster state [Metadata][] so that settings, templates, pipelines and other configurations can be preserved.

Snapshots are deduplicated in that it does not copy a data file if it has already been copied in a previous snapshot.
Instead, it adds a reference to the existing file in the metadata stored in the repository, effectively a ref-tracking
system for the data files. This also means we can freely delete any snapshot without worrying about affecting other
snapshots.

This [snapshots Java package documentation][] provides a good explanation on how snapshot operations work.

Restoring a snapshot is a process which largely relies on [index recoveries](#Recovery). The restore service initializes
the process by preparing shards of restore indices as unassigned with snapshot as their recovery source ([SnapshotRecoverySource][])
in the cluster state. These shards go through the regular allocation process to be allocated. They then recover on the target nodes
by copying data files from the snapshot repository to local storage.

Both snapshot and restore are coordinated by the master node, while index data transfer is done by the data nodes.
The communications from the master node to data nodes are always cluster state updates. Data nodes send transport
requests to the master node to update the status. These requests, at the end, also triggers cluster state updates
which can be further reacted upon by the data nodes until the processes are complete.

[Metadata]: https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/cluster/metadata/Metadata.java
[snapshots Java package documentation]: https://github.com/elastic/elasticsearch/blob/8ccbdd553b096465a282297332d2389328ed665a/server/src/main/java/org/elasticsearch/snapshots/package-info.java#L11
[SnapshotRecoverySource]: https://github.com/elastic/elasticsearch/blob/31db11b3b067baf97e305bfefefea9e11cb85371/server/src/main/java/org/elasticsearch/cluster/routing/RecoverySource.java#L207

### Snapshot Repository

A [Repository][] must be created before any snapshot operation can take place. There are different types of
repositories. The most common ones are file system based (FS) and cloud storage based (S3, GCS, Azure) which
all extend the [BlobStoreRepository][] class. A repository must be registered as writeable with a single
cluster while registered as readable with multiple clusters. NOTE registering a repository as writeable
with multiple clusters can lead to data corruption. We try our best to detect such situation, but it's not
completely foolproof.

The content structure of a repository is similar to the local index storage structure, with `indices` folder
holding indices data separate by their UUID and shard ID. Here is a simple example of a repository structure:

```
my-repository/
├── index-2       <-- The root blob file of the repository, also called repository generation file
├── index.latest
├── indices
│   └── 8K9JNuqjTnygrjKY8qmsiA   <-- UUID of the snapshotted index. Not the same index UUID in the cluster
│       ├── 0                    <-- shard 0
│       │   ├── __I0e0reaMQzuXv8fY1GYD2w           <-- data file
│       │   ├── __XqE3EVhOREWBnCHASLALtw           <-- another data file
│       │   ├── index-pPXvvdFWSmajXZcfrIwggA       <-- shard level generation file
│       │   └── snap-3kXOuTRmTFm6VMcEqPkKNQ.dat    <-- shard level snapshot info
│       └── meta-tdTzfI8BkmhGlJ2SvOok.dat          <-- index metadata
├── meta-3kXOuTRmTFm6VMcEqPkKNQ.dat    <-- cluster metadata
└── snap-3kXOuTRmTFm6VMcEqPkKNQ.dat    <-- snapshot information
```
See also the [blobstore Java package documentation][] for more details on the repository structure.

The most important file in the repository is the `index-N` file, where `N` is a numeric generation number starting
from `9`. Its corresponding Java class is [RepositoryData][].
This file holds the global state of the repository, including all valid snapshots and their corresponding
indices, shards and index metadata. Every mutable operation on the repository, such as creating or deleting a snapshot,
results in a new `index-N` file being created with an incremented generation number. The `index.latest` file stores
the latest repository generation and is effectively a pointer to the latest `index-N` file.

The repository is not rescuable if the repository generation file is corrupted. This is the reason that we are
very careful when updating this file by leveraging cluster consensus to ensure only the latest master node update
it to a generation that is accepted by the rest of the cluster members. The updating process also attempts to
detect concurrent writes to avoid multiple clusters writing to the same repository. This is done by comparing
the expected repository generation to the actual generation files in the repository. If this file is corrupted,
as reported occasionally on SDHs, it is almost certain that some other external process has modified it.

If other files in the repository are corrupted, we can usually delete the broken snapshots and retain the good ones.
The broken snapshots usually lead to exception being thrown when accessed by APIs like the [GetSnapshot API][].
Since v8.16.0, there is also [VerifyRepositoryIntegrity API][] that can be used to actively scan the repository
and identify any corrupted snapshots.

The state of a repository must always transition through fully valid states in that there are no dangling references
to non-existent blobs. This is an important property that guarantees the repository integrity as long as there is
no external interference. We add new blobs before they become reachable from the root, then update the root blob,
and only if the root-blob update succeeds do we delete any now-unreachable blobs. See also
[Creation of a Snapshot](#creation-of-a-snapshot) and [Deletion of a Snapshot](#deletion-of-a-snapshot) for more details.

It is worth note that repository's compatibility guarantee is more permissive than Elasticsearch's general version
compatibility policy. A repository may contain snapshots from a version as old as 5.0.0 and, if it does, the repository
layout must remain compatible (for reads and writes) with the oldest version in the repository so that these snapshots
remain restorable in the corresponding versions. With older snapshots deleted, the repository will start using
new format when possible (see also the static `IndexVersion` constants in `SnapshotsService`).

[Repository]: https://github.com/elastic/elasticsearch/blob/2d4687af9bf21321573eb64eade0b0365213a303/server/src/main/java/org/elasticsearch/repositories/Repository.java#L53
[BlobStoreRepository]: https://github.com/elastic/elasticsearch/blob/2d4687af9bf21321573eb64eade0b0365213a303/server/src/main/java/org/elasticsearch/repositories/blobstore/BlobStoreRepository.java#L200
[blobstore Java package documentation]: https://github.com/elastic/elasticsearch/blob/24fad8fac774983bb231da34321108abef102745/server/src/main/java/org/elasticsearch/repositories/blobstore/package-info.java#L11
[RepositoryData]: https://github.com/elastic/elasticsearch/blob/31db11b3b067baf97e305bfefefea9e11cb85371/server/src/main/java/org/elasticsearch/repositories/RepositoryData.java#L58
[GetSnapshot API]: https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-get
[VerifyRepositoryIntegrity API]: https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-repository-verify-integrity

#### Repository Management

A snapshot repository has two components: (1) a `RepositoryMetadata` containing the configuration stored
as a `Metadata.ProjectCustom` in the cluster state; (2) the actual repository object created on the master
node and each data node. A series of APIs are available to create, update, get and delete repositories.
Each of these APIs is backed by a `TransportMasterNodeAction` which publishes a cluster state for the
`RepositoryMetadata` change so that relevant nodes can update their local repository objects accordingly.
The API call will only return after the repository object has been updated on all relevant nodes.
For creating a repository, it also performs extra verification steps which (1) attempts to create a temporary
repository directly on the master node as well as write and read a test file before proceeding with the
cluster state update and (2) performs another write and read test after repository objects are created on all
relevant nodes. This verification steps are enabled by default and can be disabled per request.

The core service class is [RepositoriesService][] which all APIs eventually delegate to. It also implements
`ClusterStateApplier` which performs the actual repository object creation, update and deletion.

Besides the APIs, reserved repositories are managed via file based settings. These repositories are managed
by Elasticsearch service providers, such as the Elastic Cloud. File based settings is effectively a way
to publish cluster state based on file contents. Hence, they also go through the same code path as the APIs
under the hood. Reserved repositories cannot be modified via APIs.

While new repository can be created at any time, deleting a repository has some restrictions. In general,
a repository cannot be deleted if it is in use by either ongoing snapshots or restores or hosting mounted
searchable snapshots. Since v9.4.0, the default snapshot repository cannot be deleted either. A default
repository is meant to be the repository used by ILM and SLM when no repository is explicitly specified.
Updating a repository usually involves closing the existing repository first and creating a new one.
Therefore, they often subject to the same restrictions as deletion.

Cloud storage backed (S3, GCS, Azure) repositories requires network access to the storage services. Hence,
they have concept of clients which manages the network requests. At least the `default` client must be
configured for a cluster which is used by a repository if no client is explicitly specified. Multiple
clients can be added, via `elasticsecrh.yml` to the same cluster and used by different repositories to
spread snapshots to different locations if so desired.

We allow different implementations of the same cloud storage type to be used as long as they are compatible
in both APIs and performance characteristics. For example, many storage service claims S3 compatibility.
But they may fall short under load or even just return outright incorrect responses in some corner cases.
The [RepositoryAnalyze API][] can be used to proactively test the
compatibility which we suggest on SDHs from time to time.

[RepositoriesService]: https://github.com/elastic/elasticsearch/blob/f55a90c941f5ca80fff4978be7b15e97614ce55f/server/src/main/java/org/elasticsearch/repositories/RepositoriesService.java#L98
[PutRepository API]: https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-create-repository
[RepositoryAnalyze API]: https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-snapshot-repository-analyze

### Creation of a Snapshot

Creating a snapshot is the most involved snapshot operation because it requires work to be done on
both master and data nodes. In contrast, cloning snapshot, deleting snapshot and cleaning repository
all happen entirely on the master node.

The following Java classes are mostly responsible for the snapshot creation process:
1. [SnapshotsService][] - runs on the master node and coordinates the overall snapshot process.
2. [SnapshotsServiceUtils][] - utility class separated from `SnapshotsService` to reduce file length.
3. [SnapshotShardsService][] - runs on data node and manages the actual snapshot of individual shards.
4. [BlobStoreRepository][] - used by both master and data nodes to read and write snapshot data and metadata.

As discussed earlier, a snapshot operation always starts with a cluster state triggered by a transport request.
Such is the case for snapshot creation. When `SnapshotsService` receives the request, it computes all indices
and their shards required for the snapshot and creates an object representing this snapshot and stores it in the
cluster state. The overarching object representing snapshot creation in the cluster state is[SnapshotsInProgress][]
which is essentially a map keyed by repository name with values being a list of ongoing snapshots(`SnapshotsInProgress.Entry`)
for that repository. It's a list because the order is important: Snapshots within each repository operate as a queue,
such that each shard's snapshots run in order. Different snapshots may complete in different orders if they target different shards.
Each ongoing snapshot further tracks its overall state (`SnapshotsInProgress#State`)
as well states (`SnapshotsInProgress#ShardState`) of each shard required for the snapshot.

When the snapshot creation entry (`SnapshotsInProgress.Entry`) is first added to the cluster state, its shard states
can be in different `ShardState` depending on the shard's status and snapshot activities:
* If the shard is started and has no other active snapshot activity, its state is set to `INIT` indicating
  it is ready to be snapshotted by the data node hosting it. Shards in this state cannot relocate due to
  `SnapshotInProgressAllocationDecider`.
* If the shard is started but is still running another snapshot, its state is set to `QUEUED` indicating it
  will be snapshotted later when the ongoing snapshots and any other snapshots queued before it are finished.
* If the shard is relocating or initializing, its state is set to `WAITING` which will be changed to a new state
  once the shard finishes relocation or initialization.
* If the index no longer exists or the shard is unassigned, the shard state is set to `MISSING`. This state
  is final. A snapshot creation fails on shard with this state if it is issued with `partial=false`. Note
  snapshot creation is always issued with `partial=true` in Elastic Cloud so that snapshot does not fail
  entirely for temporary shard unavailability.
* If the node hosting the shard is being shutdown, the shard state is set to `PAUSED_FOR_NODE_REMOVAL`.
  This state will be updated when the node finishes the shutdown process or the shard state changes.

When a data node (`SnapshotShardsService`) receives the updated cluster state with a new snapshot entry,
it takes the shards with the `INIT` state and hosted on itself to create a shard snapshot task for each
of them. The shard state is computed for all shards involved in the snapshot at once when the snapshot entry
is created. A large snapshot can easily have thousands of shards with `INIT` state indicating ready to be snapshotted.
To avoid overwhelming the data nodes, a dedicated snapshot thread pool as well as `ThrottledTaskRunner` are
used to keep concurrent running shard snapshots under control. Priority is given for snapshots which started
earlier. We also order by shard to limit the number of incomplete shard snapshots (see also [ShardSnapshotTaskRunner][]
and `ShardSnapshotTaskRunner#COMPARATOR`).

The lifecycle of each shard snapshot is also tracked in-memory on the data node with `IndexShardSnapshotStatus`.
The status is indicated by `IndexShardSnapshotStatus#Stage` which is updated at various points during the process.
When a shard snapshot task runs, it first acquires an index commit of the shard so that the files to be copied remain
available throughout the shard snapshot process without being deleted by ongoing indexing activities. It then
writes a new shard level generation file (`index-<UUID>.data`, Java class [BlobStoreIndexShardSnapshots][]).
This is basically a shard level catalog file pointing to all the valid shard snapshots. Each snapshot creates a new one.
The UUID is used to avoid name collision.
Previous shard generation files are not deleted because they may still be needed if the current shard snapshot fails.
Following that, shard level data files are copied to the repository with `BlobStoreRepository#doSnapshotShard`,
`BlobStoreRepository#snapshotFile` etc.
After all data files are uploaded, it writes a shard level snapshot file (`snap-<UUID>.dat`, Java class
[BlobStoreIndexShardSnapshot][]) indicating what data files
should be included in this shard snapshot. Note the data file's physical name is replaced with double underscore (`__`)
followed by an UUID to avoid name collision. The actual name is mapped and stored in the shard level snapshot file.

It is worth note that the shard level generation file (`index-<UUID>.data`) can be reconstructed from all shard
level snapshot files (`snap-<UUID>.dat`) so that it is technically redundant. However, listing and reading
all shard snapshot files can be rather inefficient since there could be hundreds or thousands of these files.
Hence, having the shard level generation file helps with performance for deletion operations which
needs to read only a single file to decide what can be deleted at the shard level.

Once the shard snapshot is completed successfully, the data node releases the previously acquired index commit and
sends a transport request(`UpdateIndexShardSnapshotStatusRequest`) with the new shard generation (`ShardGeneration`)
to the master node to update its shard status (`SnapshotsInProgress#ShardState`) in the cluster state. If there is any
`QUEUED` shard snapshot for the same shard, the master node (`SnapshotsService`) updates the next one's
status to `INIT` so that it can run. The master responds to the data node only after the cluster state is published.

When all shards in a snapshot are completed, the master node performs a finalization step
(`SnapshotsService#SnapshotFinalization` and `BlobStoreRepository#finalizeSnapshot`) which does the following:
1. Create a `SnapshotInfo` object representing the completed snapshot for serialization.
2. Collect and write the latest `Metadata` and `IndexMetadata` relevant to this snapshot.
3. Write the snapshot metadata file `snap-<UUID>.dat` (Java class [SnapshotInfo][]).
4. Create a new root blob (repository generation file, `index-N`) with incremented generation number
   and updated content including the new snapshot and publish a cluster state to accept this new
   generation as the current/safe (`BlobStoreRepository#writeIndexGen`).

Step 4 is the most critical one. The root blob is intentionally written at the very last so that
any prior failure only leaves some redundant files in the repository which will be cleaned up in due time.
A snapshot is not completed until the root blob is successfully updated. To ensure consistency,
updating the root blob is a 3-steps process leveraging cluster consensus:
1. Picks a new pending repository generation number which is greater than the current pending generation
   and publishes a cluster state update for it. Both repository generation number and pending generation
   numbers are part of [RepositoryMetadata][].
2. If previous step is successful, writes the new root blob with the pending generation number.
3. Publishes another cluster state to set the current/safe repository generation to the new pending generation.

When master fails over during snapshot creation, the above steps ensures that only the new master can
successfully update the root blob to avoid data corruption.

Multiple snapshots can run concurrently in the same repository. But the process is sequential at shard level,
i.e. only one shard snapshot for the same shard can be in the `INIT` state at any time.
Snapshot deletions and creations are mutually exclusive. See also [Deletion of a Snapshot](#Deletion-of-a-Snapshot).

[SnapshotsService]: https://github.com/elastic/elasticsearch/blob/5c3270085a72ec6b97d2cd34e2a18e664ebd28ba/server/src/main/java/org/elasticsearch/snapshots/SnapshotsService.java#L133
[SnapshotsServiceUtils]: https://github.com/elastic/elasticsearch/blob/5c3270085a72ec6b97d2cd34e2a18e664ebd28ba/server/src/main/java/org/elasticsearch/snapshots/SnapshotsServiceUtils.java#L83
[SnapshotShardsService]: https://github.com/elastic/elasticsearch/blob/5c3270085a72ec6b97d2cd34e2a18e664ebd28ba/server/src/main/java/org/elasticsearch/snapshots/SnapshotShardsService.java#L81
[BlobStoreRepository]: https://github.com/elastic/elasticsearch/blob/2d4687af9bf21321573eb64eade0b0365213a303/server/src/main/java/org/elasticsearch/repositories/blobstore/BlobStoreRepository.java#L200
[SnapshotsInProgress]: https://github.com/elastic/elasticsearch/blob/5c3270085a72ec6b97d2cd34e2a18e664ebd28ba/server/src/main/java/org/elasticsearch/cluster/SnapshotsInProgress.java#L78
[ShardSnapshotTaskRunner]: https://github.com/elastic/elasticsearch/blob/01ace3927df065f1caf090653404f29688e8103a/server/src/main/java/org/elasticsearch/repositories/blobstore/ShardSnapshotTaskRunner.java#L36
[BlobStoreIndexShardSnapshots]: https://github.com/elastic/elasticsearch/blob/495c7c2f4ec5817001aa767f6d45a9f1c8c31082/server/src/main/java/org/elasticsearch/index/snapshots/blobstore/BlobStoreIndexShardSnapshots.java#L40
[BlobStoreIndexShardSnapshot]: https://github.com/elastic/elasticsearch/blob/495c7c2f4ec5817001aa767f6d45a9f1c8c31082/server/src/main/java/org/elasticsearch/index/snapshots/blobstore/BlobStoreIndexShardSnapshot.java#L38
[SnapshotInfo]: https://github.com/elastic/elasticsearch/blob/495c7c2f4ec5817001aa767f6d45a9f1c8c31082/server/src/main/java/org/elasticsearch/snapshots/SnapshotInfo.java#L50
[RepositoryMetadata]: https://github.com/elastic/elasticsearch/blob/495c7c2f4ec5817001aa767f6d45a9f1c8c31082/server/src/main/java/org/elasticsearch/cluster/metadata/RepositoryMetadata.java#L24

#### Shard snapshot pausing

When a node is shutting down, it must vacate all shards via relocation. Since shards being snapshotted
(shard snapshot status `INIT`) cannot relocate, we need a way to transit these shards out of the
`INIT` state to avoid stall the shutdown process. This is where the shard snapshot pausing mechanism
comes into play.

When a node shutdown is initiated, `SnapshotsService` reacts to the new shutdown metadata by updating
`SnapshotsInProgress#nodesIdsForRemoval` which tracks the node IDs for the shutting down node. When
this change is published and observed by the shutting down data node (`SnapshotShardsService`), it pauses
its shard snapshots by first setting the shard snapshot status `PAUSING`, which is checked regularly
by the file uploading process (`BlobStoreRepository#snapshotFile`) and leads to a `PausedSnapshotException`
to be thrown to abort the shard snapshot. The data node then notifies the master node about the status
change with the same status update request (`UpdateIndexShardSnapshotStatusRequest`) in the happy path.
The master node updates the shard state to `PAUSED_FOR_NODE_REMOVAL` upon receiving the notification.
From this point on, the shard can relocate as normal. When the shard is started on the target node,
`SnapshotsService` will observe the shard state changes and transition the shard snapshot status back to
`INIT`.

If a node is already being shutdown when a new snapshot creation request arrives, the relevant
shard snapshot will be created with `PAUSED_FOR_NODE_REMOVAL` as its initial state. This assumes there
is no ongoing shard snapshot that is already `PAUSED_FOR_NODE_REMOVAL`, in which case the new shard
snapshot will start out as `QUEUED`.

When the node shutdown completes and its associated shutdown metadata is removed from the cluster state,
`SnapshotsService` will also remove the node ID from `SnapshotsInProgress#nodesIdsForRemoval`.

### Deletion of a Snapshot

Both completed snapshots and ongoing snapshots can be deleted. Unless the snapshot being deleted has not
started yet, e.g. all its shards are in `QUEUED` state, which means it can be deleted right away from the
cluster state without touch the repository content, deletion must run exclusively in a repository.

If the deletion requires any file removal in the repository, `SnapshotsService` creates/updates the
`SnapshotDeletionsInProgress` in the cluster state to track the new deletion. If the snapshot is
currently running, it also updates any incomplete shard snapshots to `ShardState.ABORTED` for the
data node (`SnapshotShardsService`) to react once the cluster state is published. The data node goes through
a similar process to the shard snapshot pausing but with a different exception (`AbortedSnapshotException`)
to interrupt the shard snapshot process and sends a request back to the master node to update the
corresponding status (`ShardState.FAILED`) tracked in the cluster state. Once all shard snapshots
stop, deletion will proceed to remove relevant files from the repository as well as create a new
root blob (`index-N`) with the same mechanism described in the snapshot creation section.
File deletions (`BlobStoreRepository#SnapshotsDeletion`) happen entirely on the master node.

### Clone of a Snapshot

TODO: Clone is not used in Elastic Cloud Serverless.

### Cleaning a Repository
Repository clean up is cluster wide exclusive and must run by itself. It does not actually clean up anything
more than a regular snapshot deletion. It was useful in the early days when we had some long-since-fixed
leaks that needed cleaning up in ECH. It is not used in Elastic Cloud Serverless.

### Restoring a Snapshot

[RestoreService][] is responsible for handling the initial restore request and prepare the unassigned shards
with snapshot as their recovery source in the cluster state. Once the shard is allocated on a data node, the
recovery process kicks in and eventually calls into `IndexShard#restoreFromSnapshot` which delegates to
`BlobStoreRepository#restoreShard` to copy data files from the repository to local storage.

[RestoreService]: https://github.com/elastic/elasticsearch/blob/1b7e99ee7a4ec92d20846be639fa4c3d15f20abc/server/src/main/java/org/elasticsearch/snapshots/RestoreService.java#L149

### Detecting Multiple Writers to a Single Repository

This is a best effort attempt to prevent repository corruption due to concurrent writes from multiple clusters.
When writing the root blob (`index-N`), we cross compare the cached and expected repository generation
(see `BlobStoreRepository#latestKnownRepoGen` and `BlobStoreRepository#latestKnownRepositoryData`)
to the generation physically found in the repository. The writing also fails if the blob already exists
since this indicates that some other cluster attempted to write to the repository. We do that in all repositories
that support such a check, which includes FS/GCS/Azure since forever and S3 since 9.2 (but not HDFS).
A `RepositoryException` is thrown on any mismatch or conflicts. The repository is marked as corrupted
by setting its generation number to `RepositoryData.CORRUPTED_REPO_GEN` to block further write operations.

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

# Shrink/Split/Clone index APIs

These APIs are used to create a new index that contains a copy of data from a provided index and differs in number of shards and/or index settings. They can only be executed against source indices that are marked read-only.

The shrink API (`/{index}/_shrink/{target}`) creates an index that has fewer shards than the original index.

The split API (`/{index}/_split/{target}`) creates an index that has more shards than the original index.

The clone API (`/{index}/_clone/{target}`) creates an index that has the same number of shards as the original index but may have different index settings.

The main implementation logic is centralized in `TransportResizeAction` however it only creates a new index in the cluster state using special `recoverFrom` and `resizeType` parameters. The entire workflow involves multiple components.

The high level structure is the following:
1. `TransportResizeAction` creates index metadata for the new index that contains information about the resize performed. It also creates a routing table for the new index which assigns a `LocalShardsRecoverySource.INSTANCE` recovery source to primary shards based on the resize information in the index metadata .
2. Allocation logic allocates shards of the index so that they are on the same node as the corresponding shards of the source index. See `ResizeAllocationDecider`. Note that this doesn't work for shrink case since during shrink there are multiple source shards that are "merged" together. These source shards may be on different nodes already. Shard movement in this case needs to be performed manually.
3. Primary shards perform [local shards recovery](#local-shards-recovery) using index metadata to know what type of resize operation is performed.

# Health API

The Health API (`GET /_health_report`) provides a structured health report for the cluster. It is indicator-based: each health indicator evaluates a specific aspect of cluster health (for example shard allocation or disk) and returns a status (`GREEN`, `YELLOW`, `RED`, or `UNKNOWN`) plus structured details, impacts, and diagnoses. The top-level status is derived from the worst indicator status.

## Health node and data flow

A health node is selected by the master node via a persistent task. The health node maintains a cache of per-node health information. Each node periodically publishes its local health info to the health node cache. That cached health info is then used as input to health indicators when evaluating the overall cluster health. If the health node fails or leaves the cluster, the master node selects a new health node.

```
Publish: LocalHealthMonitor (local node) -> UpdateHealthInfoCacheAction -> HealthInfoCache (health node)
Retrieve: Health API (coordination node) -> FetchHealthInfoCacheAction -> HealthInfoCache (health node)
```

Relevant classes:
- [HealthNodeTaskExecutor][HealthNodeTaskExecutor]: the persistent task executor that selects the health node.
- [HealthInfoCache][HealthInfoCache]: contains the cached per-node health info on the health node.
- [LocalHealthMonitor][LocalHealthMonitor]: monitors local health and publishes to the health node.
- [HealthTracker][HealthTracker]: invoked by LocalHealthMonitor and tracks local health info on each node.

## Health indicators

There are two kinds of health indicators: preflight and regular. Preflight indicators run first; if any preflight indicator is not GREEN, regular indicators return UNKNOWN to avoid misleading results on an unstable cluster. Currently, the only preflight indicator is [StableMasterHealthIndicatorService][StableMasterHealthIndicatorService], which checks whether the cluster has a stable master node.

Relevant classes:
- [HealthIndicatorService][HealthIndicatorService]: the interface for health indicators.
- [HealthService][HealthService]: orchestrates indicator evaluation.

## Health periodic logger

Cluster health status can be logged periodically by [HealthPeriodicLogger][HealthPeriodicLogger], which runs on the health node and calls [HealthService][HealthService] at a configured interval. It enables alerting on the health status.

## Health metadata

Some health configuration is stored in the cluster state [HealthMetadata][HealthMetadata]. It stores user-configurable health settings, such as disk watermark thresholds. The current master node publishes its settings to the cluster state so the same settings are used across nodes in the cluster.

Relevant classes:
[HealthMetadataService][HealthMetadataService]: runs on master node, publishes health metadata to the cluster state.

[HealthService]: https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/health/HealthService.java
[HealthIndicatorService]: https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/health/HealthIndicatorService.java
[HealthInfoCache]: https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/health/node/HealthInfoCache.java
[LocalHealthMonitor]: https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/health/node/LocalHealthMonitor.java
[HealthPeriodicLogger]: https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/health/HealthPeriodicLogger.java
[HealthTracker]: https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/health/node/tracker/HealthTracker.java
[StableMasterHealthIndicatorService]: https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/cluster/coordination/StableMasterHealthIndicatorService.java
[HealthMetadata]: https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/health/metadata/HealthMetadata.java
[HealthMetadataService]: https://github.com/elastic/elasticsearch/blob/main/server/src/main/java/org/elasticsearch/health/metadata/HealthMetadataService.java

# Watcher

[Watcher]: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/Watcher.java
[WatcherLifeCycleService]: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/WatcherLifeCycleService.java
[ExecutionService]: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/execution/ExecutionService.java
[WatcherService]: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/WatcherService.java
[TickerScheduleTriggerEngine]: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/trigger/schedule/engine/TickerScheduleTriggerEngine.java
[EmailAction]: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/actions/email/EmailAction.java
[EmailService]: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/notification/email/EmailService.java
[WebhookAction]: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/actions/webhook/WebhookAction.java
[WebhookService]: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/notification/WebhookService.java
[Actions Package]: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/actions
[ReportingAttachment]: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/watcher/src/main/java/org/elasticsearch/xpack/watcher/notification/email/attachment/ReportingAttachment.java

Watcher lets you set a schedule to run a query, and if a condition is met it executes an action.
As an example, the following performs a search every 10 minutes. If the number of hits found is greater than 0 then it logs an error message.
```console
PUT _watcher/watch/log_error_watch
{
  "trigger" : { "schedule" : { "interval" : "10m" }},
  "input" : {
    "search" : {
      "request" : {
        "indices" : [ "logs" ],
        "body" : {
          "query" : {
            "match" : { "message": "error" }
          }
        }
      }
    }
  },
  "condition" : {
    "compare" : { "ctx.payload.hits.total" : { "gt" : 0 }}
  },
  "actions" : {
    "log_error" : {
      "logging" : {
        "text" : "Found {{ctx.payload.hits.total}} errors in the logs"
      }
    }
  }
}

```

## How Watcher Works

- We have an API to define a “watch”, which includes the schedule, the query, the condition, and the action
- Watch definitions are kept in the `.watches` index
- Information about currently running watches is in the `.triggered_watches` index
- History is written to the `.watcher_history` index
- Watcher ([WatcherLifeCycleService]) runs on all nodes, but only executes watches on a node that has a copy of the `.watches` shard that the particular watch is in (see [WatcherService])
  -  Uses a hash to choose the node if there is more than one shard
- Example common use cases:
  - Periodically send data to a 3rd party system
  - Email users with alerts if certain conditions appear in log files
  - Periodically generate a report using Kibana, and email that report as an attachment. This is supported by declaring a [ReportingAttachment] [ReportingAttachment] to the [EmailAction] [EmailAction] in the watch definition.

## Relevant classes:

- [Watcher] [Watcher] – the plugin class
- [WatcherLifeCycleService] [WatcherLifeCycleService]  – created by the Watch plugin on each node
- [WatcherService] [WatcherService] – decides which watches this node ought to run
- [ExecutionService] [ExecutionService] – executes watches
- [TickerScheduleTriggerEngine] [TickerScheduleTriggerEngine] – handles the periodic (non-cron) schedules that we see the most
- [EmailAction] [EmailAction] / [EmailService] [EmailService] – emails to third-party email server
- [WebhookAction] [WebhookAction] / [WebhookService] [WebhookService] – sends requests to external endpoints
- [Various other actions] [Actions Package] (for example posting to Slack, Jira, etc.)

## Debugging

- The most useful debugging information is in the Elasticsearch logs and the `.watcher_history` index
- It is often useful to get the contents of the `.watches` index
- Frequent sources of problems:
  - There is no guarantee that an interval schedule watch will run at exactly the requested interval after the last run
  - In older versions (before 8.17), the counter for the interval schedule restarts if the shard moves. For example, if the interval is once every 12 hours, and the shard moves 10 hours into that interval, it will be at least 12 more hours until it runs.
  - Calls to remote systems ([EmailAction] and [WebhookAction]) are a frequent source of failures. Watcher sends the request but doesn't know what happens after that. If you see that the call was successful in `.watcher_history`, the best way to continue the investigation is in the logs of the remote system.
  - Even if watcher fails during a call to a remote system, the error is likely to be outside of watcher (e.g. network problems). Check the error message in `.watcher_history`.

