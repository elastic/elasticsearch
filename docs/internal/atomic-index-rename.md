# Atomic index rename: proof of concept, tradeoffs and open decisions

Status: proof of concept. This document records what the prototype does, every
shortcut taken to get it working end to end, and the decisions that need to be
made before any of it is production quality. Tracking issue:
elastic/elasticsearch-team#1743 (public ask: elastic/elasticsearch#37880).

The prototype builds on Niels Bauman's and Mary Gouseti's 2025 spikes. Their
hand-off notes are in the tracking issue.

## What the prototype does

`POST /{index}/_rename/{target}` (transport action `indices:admin/rename`,
`TransportRenameIndexAction`) renames an index in a single cluster state update.
The index keeps its UUID, shards, settings, mappings, aliases, blocks, ILM state
and data stream membership; only the name under which it is addressed changes.
No shard is closed, recovered or relocated, and indexing and search continue
across the rename. Renames are batched through a `MasterServiceTaskQueue`.

The index UUID is treated as the identity of an index throughout. Everything
already keyed on the UUID survives a rename untouched: node data paths
(`NodeEnvironment`), persisted cluster state (`PersistedClusterStateService`),
the stateless object store (`ObjectStoreService`), and the data node's
`IndicesService#indices` map. Everything keyed on the name has to be rewritten
in the cluster state update (`ProjectMetadata#indices`, `RoutingTable`,
`ClusterBlocks`, `DataStream` backing index lists) or tolerated (snapshot
repository `IndexId`, see below).

### Master side

`TransportRenameIndexAction` validates the request, then submits a task to a
`MasterServiceTaskQueue` (batched, acknowledged by every node) that:

1. builds a new `IndexRoutingTable` for `new Index(target, uuid)` by rewriting
   the `ShardId` of every `ShardRouting` (`ShardRouting#updateIndex`), and
   removes the old one;
2. moves the index blocks from the old name to the new name;
3. puts the `IndexMetadata` under the new name and removes the old entry;
4. if the index is a backing or failure index of a data stream, replaces it in
   the data stream.

Nothing in the cluster state marks the rename. The request is acknowledged once
every node has applied the state.

### Data node side

`ClusterChangedEvent#indicesRenamed()` reports every index whose previous name
is gone from a project while an index with the same UUID is still present
there, using a lazily built by-UUID map on `ProjectMetadata`
(`ProjectMetadata#indexByUuid`). `indicesDeleted()` excludes those indices.
`IndicesClusterStateService#renameIndices` runs first in the applier and calls
`IndicesService#renameIndex` for each, which updates the `IndexService`
metadata and calls `IndexShard#renameTo` on every shard. That swaps the shard's
`ShardId` and its `ShardRouting` in place.

Detection by UUID holds however many cluster states a node missed: a node that
was partitioned during a rename and rejoins later still finds the UUID under
the new name. A marker in the cluster state (the first version of this
prototype used a custom metadata tag removed by a second update) does not have
that property, and a node that skipped the tagged state would have deleted the
index's shards.

To make that possible, `Index#equals`/`hashCode` compare only the UUID (the
name is used only when the UUID is the `_na_` placeholder), `ShardId` therefore
compares by UUID and shard number, `AbstractIndexShardComponent#shardId` is
`volatile`, and `IndexSettings#getIndex()` returns the index of the current
`IndexMetadata` rather than a final field captured at construction.

### Test coverage trick

`ESIntegTestCase#createIndex` creates a temporary index and renames it to the
requested name, unless the name matches a composable template, starts with `.`
or `<`, or the caller opts out. Every integration test that creates an index
through that helper therefore exercises the rename. The suite-wide result of
running `:server:internalClusterTest` with this enabled is recorded under
"Failure inventory" below.

### Trying it

```
PUT  /my-index
POST /my-index/_rename/my-index-renamed
GET  /my-index-renamed/_search
```

The index must not be a system index, must not be in a running snapshot or
restore, and must not be the write index of a data stream. The cluster must
consist only of nodes that publish the `index.rename` feature.

## Tradeoffs taken to get the prototype working

Each item names the shortcut, why it was taken, and what a production
implementation would need instead.

### T1. `Index` identity is the UUID alone

`Index#equals` and `hashCode` ignore the name whenever the UUID is known. This
is what lets data node structures keyed on `Index` or `ShardId` (routing nodes,
shard maps, search contexts, replication groups, pending deletes) keep working
after a rename without being rebuilt.

Cost: an `Index` object's `getName()` may be stale. Any code that reads the
name from a cached `Index` (log lines, exception messages, `ShardId#toString`)
can print the old name for a while. Tests that construct two `Index` objects
with the same UUID and different names and expect them to differ now break;
Mary's elastic/elasticsearch#132443 cleans up the tests that relied on `_na_`
collisions and should land first.

Alternative considered: keep name-and-UUID equality and swap every `Index`
instance on rename. That means finding every map keyed on `Index`/`ShardId`
across server, modules and x-pack and is why previous attempts stalled.

### T2. Rename detection costs a by-UUID map per cluster state

`ProjectMetadata#indexByUuid` is backed by an `ImmutableOpenMap` built lazily on
first use and cached, the same pattern as `indicesLookup`. It is not serialized.
Keys are the UUID strings the index metadata already holds, so at 15,000
indices the map adds roughly 256 KB per cluster state and a sub-millisecond
build on first lookup. Every applier that compares states by index name now has
to consult `indicesRenamed()`; today only `IndicesClusterStateService` does.
Other candidates are listed under D2.

### T3. Renames have their own task queue

Renames go through a dedicated `MasterServiceTaskQueue`, so several renames
batch into one cluster state update, but they do not batch with alias changes
or `remove_index`. Making a rename atomic with those needs the rename to be an
action of the aliases executor (see D1).

### T4. Data node state is mutated in place

`AbstractIndexShardComponent#shardId` became `volatile` and `IndexShard#renameTo`
replaces `shardId` and `shardRouting` under the shard's monitor. Every read of
`shardId()` on the hot path is now a volatile read. On x86 and arm64 that is a
plain load, so the cost is negligible, but the semantic change is that
`shardId()` can return different values across two calls. Nothing in the
prototype re-reads it in a way that matters, but this has not been audited
beyond "the integration tests pass". `IndexSettings#getIndex()` likewise became
a read through the volatile `indexMetadata` field.

Alternative: leave the data node's `ShardId` immutable with the original name
and rely on UUID equality for every lookup, treating the name on data nodes as
advisory. That avoids the volatile field but makes stale names permanent until
the shard is reallocated. The failure inventory below is the input for choosing
between the two.

### T5. Data stream membership reuses `replaceBackingIndex`

Renaming a backing index updates the data stream through
`DataStream#replaceBackingIndex`/`replaceFailureStoreIndex`. Those methods bump
the data stream generation, which a rename should not do, and they refuse to
replace the write index, so the prototype refuses to rename a write index.
Production code needs a dedicated `DataStream#renameIndex` that keeps the
generation and allows the write index.

### T6. Snapshot repositories key indices by name

`RepositoryData` resolves `IndexId` by index name. The first snapshot after a
rename therefore sees a "new" index and uploads all of its segments again
instead of only the delta. Restoring older snapshots is unaffected (they restore
under the name they were taken with, or a `rename_pattern`). The prototype
refuses to rename an index while it is being snapshotted or restored, mirroring
`MetadataDeleteIndexService`. Fixing the re-upload requires `RepositoryData` to
match on index UUID as well as name, which is a repository format change and out
of scope for the prototype.

### T7. Two indices with one UUID would be silently merged

`indexByUuid` keeps one entry per UUID. Two live indices sharing a UUID is
impossible today (restore, clone, shrink and split all mint new UUIDs) and the
builder asserts it, but the production change should turn that into a
validation in `ProjectMetadata.Builder`.

### T8. Validation is done twice and is not exhaustive

The request is validated against the state the master received it with and
again inside the cluster state task. The checks are: feature gate, source
exists, source is not a system index, destination passes
`MetadataCreateIndexService#validateIndexName` (valid name, no existing index,
alias, ESQL view or dataset), no snapshot or restore in flight for the source,
and source is not a data stream write index. Not checked: CCR (a follower whose
leader is renamed stops following, and a renamed follower keeps its leader
settings), searchable snapshot mounts (settings move with the metadata, so
these should keep working but are untested), shrink/split/clone with the source
as resize source, and the enrich, ML, transform and watcher references that
store index names in their own configuration (these are the user's problem, as
they are today with reindex).

### T9. The REST API is a dedicated endpoint, not an `_aliases` action

`POST /{index}/_rename/{target}` is the simplest surface for the prototype. The
ILM and Upgrade Assistant use cases need the rename to be atomic with deleting
the source index and swapping aliases, which only the `_aliases` executor can
offer today. See D1.

### T10. Security authorizes both names, nothing more

`RenameIndexAction.Request#indices()` reports both the source and the
destination, so the caller needs `manage` (any privilege matching
`indices:admin/*`) on both names. That is the same bar as creating the
destination and deleting the source. Document-level and field-level security
rules are attached to index name patterns in roles, so a rename can move an
index into or out of a user's DLS/FLS scope exactly as creating it under that
name would. Niels's last CI run failed on security tests when the rename trick
was extended to `assertAcked`; those failures have not been re-examined.

### T11. The `createIndex` rename trick is always on

In the prototype every `ESIntegTestCase#createIndex` call renames. That is what
produces the failure inventory, but it cannot be merged as is: it slows every
test by two extra cluster state updates and a health check, and it makes
unrelated test failures look rename-related. Production should make it a
randomized or opt-in behavior, like other `ESIntegTestCase` randomizations.

## Decisions that need to be made

### D1. Where the API lives

Options: (a) dedicated `_rename` endpoint only; (b) a `rename` action inside
`POST _aliases` only; (c) both, with the endpoint delegating to the aliases
executor. The ILM shrink and searchable snapshot steps and Upgrade Assistant
need rename, `remove_index` and alias moves to be one atomic cluster state
update, which argues for (b) or (c). (c) is the recommendation: discoverability
for the simple case, atomicity for the automated ones.

### D2. Which other appliers need `indicesRenamed()`

`IndicesClusterStateService` is the only applier the prototype teaches about
renames. Candidates to audit: `IndicesStore` (deletes shard folders for indices
not in the routing table; keyed by `ShardId`, so UUID equality covers it, but
unverified), `SearchService` (frees contexts on index removal), and the
ILM/SLM/CCR/enrich cluster state listeners that key work by index name.

### D3. Data node mutation versus advisory names

See T4. Decide after reviewing the failure inventory.

### D4. Preconditions for a first release

Proposed: allow open and closed plain indices and non-write data stream
backing/failure indices; refuse system indices, indices with a snapshot,
restore or resize in flight, and CCR followers. Do not require the index to be
read-only: the zero-downtime reindex use case depends on renaming a live index,
and nothing in the write path keys on the name. The prototype does respect
index metadata blocks (`read_only`, `metadata`), exactly as delete and close
do, because a rename is a metadata write; a `write` block alone does not stop a
rename, which is what the ILM shrink and searchable snapshot flows need.

### D5. Mixed-version clusters

A node without the `index.rename` feature treats a renamed index as deleted
(`ClusterChangedEvent#indicesDeletedFromClusterState` compares by name) and
deletes its shard data. The prototype refuses renames until every node publishes
`index.rename`. A node that joins a cluster after a rename is safe, because it
starts from a tombstone-based diff rather than a name diff. Whether this gate
should also block serverless rollouts in some way needs a check with the
control plane owners; the stateless object store is keyed by UUID, so the
object store itself is unaffected.

### D6. Snapshot `IndexId` by UUID

See T6. Decide whether the full re-upload after a rename is acceptable for a
first release (it is a cost, not a correctness problem) and schedule the
repository change separately.

## Failure inventory

`:server:internalClusterTest` on main at f992e630 with the rename trick enabled
in every `createIndex` call (2026-11-02, macOS, 15.5 minutes):

| Result | Count |
|---|---|
| Tests run | 3979 |
| Failed | 6 |
| Skipped | 14 |

The six failures, and what each one says about the rename:

- `ShardStateActionIT` (two tests plus the class-level cascade). The test runs a
  HIGH-priority looping master task that blocks every NORMAL-or-lower task. The
  trick's `ensureYellowAndNoInitializingShards` waits for the master queue to
  drain, which can never happen, so `createIndex` times out. Test-framework
  artifact; the test now opts out of the rename.
- `SkippedClusterStateIT#testInitializingPrimaryTermBump`. The test intercepts
  commit-state requests and watches the primary of `INDEX_NAME` from the moment
  it is created. With the trick the shard initializes under the temporary name
  and is already STARTED when renamed, so the interceptor never sees
  INITIALIZING. Test-framework artifact; the test now opts out.
- `RejectionActionIT#testRemoteReductionRejectionWithPendingCoordinatorReduction`.
  A test plugin hooks `onIndexModule` only when `indexModule.getIndex().getName()`
  is `rejection-remote`; with the trick the index is created as
  `temp_rejection-remote` and never hooked. The test now opts out. This one is
  worth remembering beyond the test: anything that captures the index name at
  `IndexModule` creation time (plugins, `IndexSettings` consumers, index-scoped
  loggers) keeps the creation-time name after a rename. See T1/T4.
- `WriteLoadConstraintDeciderIT#testHighNodeWriteLoadPreventsNewShardAllocation`.
  A temporary cluster state listener with a 10 second timeout is registered
  before `createIndex` of a 10 to 20 shard index; with the trick's extra cluster
  state updates and health check the listener can time out before the renamed
  index appears. It passes in isolation on the prototype, so this is load
  sensitivity of the test under the full suite, made worse by the slower
  `createIndex`.

All four classes pass on plain main, and all four pass on the prototype when
run on their own after the three opt-outs above.

No failure in the run points at a rename bug in the write path, search path,
recovery, allocation, snapshots, blocks, mappings, settings or aliases: the
remaining 3973 tests created their index through a rename and passed.

Tests that already opted out of the rename in Niels's commit, for similar
reasons: `ClusterHealthIT`, `ZenDiscoveryIT`, `UpdateShardAllocationSettingsIT`,
`IndexRecoveryIT`, `NodeShutdownShardsIT`, `WriteActionsTests`.

Not yet run with the trick: `modules/*` and `x-pack/*` internal cluster tests,
REST tests, and the unit test suites that construct `Index` objects by hand.
Niels's last CI run of the trick extended to `assertAcked` failed on security
tests; that extension is not part of this prototype.

## Suggested sequence of production PRs

Each step is independently reviewable and leaves `main` working without the
rename being reachable until the last one.

1. Test cleanup replacing `_na_` index UUIDs with unique ones (revive
   elastic/elasticsearch#132443).
2. `Index#equals`/`hashCode` by UUID (T1), with the `ShardId` follow-through.
3. By-UUID index lookup in `ProjectMetadata`, `ClusterChangedEvent#indicesRenamed()`,
   and the `index.rename` node feature (D2, D5). Add a test that an ungated
   node refuses a state containing a rename rather than deleting data.
4. Data node rename handling, choosing between in-place mutation and advisory
   names (D3).
5. Randomized rename-on-create in `ESIntegTestCase` (T11), extended to the
   module and x-pack internal cluster test suites.
6. The master-side rename in the aliases executor plus the REST endpoint and
   security tests (D1, T10), with the data stream handling done properly (T5).
7. Follow-ups: `RepositoryData` matching by UUID (D6); ILM and Upgrade
   Assistant adopting the rename.
