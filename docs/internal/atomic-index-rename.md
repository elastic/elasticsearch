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

The index must not be a system index and must not be in a running snapshot or
restore. The cluster must
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

A second, less obvious cost: cluster state diffs decide whether a component
changed with `equals`. Any `Diffable` component that holds `Index` objects and
compares them with `Index#equals` now looks unchanged after a rename, so the
diff carries nothing and every other node keeps the old name. The prototype hit
this with `DataStream`: renaming a backing index without bumping the generation
produced a data stream equal to its predecessor, the data nodes and the other
master-eligible nodes kept the old backing index name, and wildcard resolution
on those nodes threw a `NullPointerException` for the missing index.
`DataStream.DataStreamIndices#equals` and `hashCode` now compare index names as
well as UUIDs, with a diff round-trip test. Components audited for the same
problem: `IndexMetadata` and the routing table are diffed as maps keyed by
index name, so a rename is a remove plus an add and is safe; `IndexGraveyard`
is append-only; `SnapshotsInProgress` and `RestoreInProgress` keep the
start-time name by design (see T6). Every future cluster state component that
stores an `Index` needs the same care, which argues for an `equalsIncludingName`
style helper on `Index` and a test that renames an index and round-trips the
whole cluster state through a diff.

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

### T4. Data node state is mutated in place, and only partly

`AbstractIndexShardComponent#shardId` became `volatile` and `IndexShard#renameTo`
replaces `shardId` and `shardRouting` together under the shard's `mutex`, the
same lock `updateShardState` takes for relocations and promotions, so a rename
never interleaves with a routing update. `IndexSettings#getIndex()` likewise
became a read through the volatile `indexMetadata` field.

Decision (D3): keep the mutation. The alternative, leaving the data node's
`ShardId` immutable and treating the name as advisory, is not the cheaper option
it looks like. Because `Index` and `ShardId` compare by UUID (T1), a stale name
is harmless wherever the id is a key, but a data node also uses the name as a
string in two places that matter: `ProjectMetadata#index(Index)` resolves by
name first and returns `null` for a stale `Index` (eleven call sites in server
feed it a shard's own id, including `IndicesClusterStateService#updateShard` on
every cluster state), and `DocWriteResponse` writes `_index` from the data
node's shard id, so every index, delete and bulk item after a rename would
report the old name to the client. Mutation fixes both at the source; advisory
names would need a UUID fallback at each lookup and would still leave the write
responses wrong. The suite-wide run (see the failure inventory) found nothing
that trips on a changed shard id.

What "different values across two calls" can cost: almost nothing, for the same
reason. Code that reads `shardId()` twice and compares, hashes or looks up still
gets equal results, since only the name differs. The volatile read itself is a
plain load on x86 and arm64.

The mutation is partial, by design. Only `IndexShard` and its `ShardGetService`
are renamed. `Store`, `Translog`, `ReplicationTracker`,
`ShardIndexWarmerService` and `ShardBitsetFilterCache` each copied the shard id
at construction and keep the old name. They use it in log and exception
messages and as a key, and keys compare by UUID, so the stale name there is
cosmetic. Loggers are the same: every shard-scoped logger is prefixed with the
creation-time shard id and is never re-created, so log lines for a renamed
shard keep the old name until the shard is reallocated. The production change
should say so in the rename API documentation rather than try to chase every
copy.

### T5. Data streams (resolved)

Renaming a backing or failure index updates the data stream through a new
`DataStream#renameIndex`, which replaces the index in place, allows either
write index, and keeps the generation. The generation counts rollovers and
names the next backing index, so a rename must not move it; the integration
test renames the write index and then rolls over to check that the next
backing index name is still derived from the unchanged generation. The
existing `replaceBackingIndex`/`replaceFailureStoreIndex` remain for ILM's
shrink and downsample flows, which really do swap in an index with different
data and a new UUID.

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
alias, ESQL view or dataset), and no snapshot or restore in flight for the
source. Not checked, and not yet tested: shrink/split/clone with the source as
resize source, and searchable snapshot mounts (settings move with the metadata,
so these should keep working). The enrich, ML, transform and watcher references
that store index names in their own configuration stay the user's problem, as
they are today with reindex.

Cross-cluster replication is handled rather than refused, see T14.

### T14. Cross-cluster replication follows by UUID across a rename

CCR already identifies indices by UUID where it matters: the follower stores
`leader_index_uuid`, every shard-changes fetch is checked against the leader's
history UUID, and auto-follow records followed leaders by UUID. Names were only
used as addresses, and that is where a rename broke things. Today a leader that
disappears fails the shard follow task fatally (`IndexNotFoundException` is not
retried), while a leader or follower that is merely closed is retried. A rename
keeps the identity, so it belongs with the retried cases, not with delete.

What was name-based and is now resolved through the UUID:

- The follower's leader metadata requests (mapping, settings and alias sync)
  filter the remote cluster state by leader name. `ShardFollowTasksExecutor`
  now runs each through `runWithLeaderIndex`: on an `IndexNotFoundException`
  it asks the leader cluster for the current name by UUID
  (`CcrRequests#resolveIndexByUuid`, an indices-stats listing without stats,
  only on that miss path), remembers it, and retries once. A leader that is
  really gone still fails the task as before.
- `ShardChangesAction` resolved the leader shard by the name in the request;
  it now resolves through `RoutingTable#index(Index)`, which falls back to the
  UUID (T13).
- The follower side fetched its own shard stats by the task's follower name;
  it now takes the current name from the metadata found by UUID.
- Pause, unfollow and follow-stats matched tasks to the requested follower by
  name; they now match by UUID after resolving the requested name.

Not changed: the follower's `leader_index_name` custom metadata keeps the name
the leader had when following started, and `_ccr/info` shows that. The
production change should show the current name instead.

`CcrRenameIT` covers a leader rename mid-replication (writes, a mapping change,
a settings change and an alias change afterwards all reach the follower), a
follower rename (replication continues, and stats, pause, resume and unfollow
work under the new name) and auto-follow with a renamed leader (no second
follower is created, the existing one keeps replicating).

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

### T12. Stateless: writes to a renamed index hung, fixed by T13

In a stateless cluster (one index node, one search node) the first bulk after a
rename never completed. `StatelessRenameIndexIT` reproduced it with debug
logging: `TranslogReplicator` holds the shard's creation-time `Index`, resolves
it through `Metadata#lookupProject(Index)` and `ProjectMetadata#index(Index)`,
both of which looked up by name first and found nothing after the rename, so it
logged "index not found while checking if shard is red", reported the primary
term as unknown, and skipped the translog sync notification that acknowledges
the write. The same pattern exists in roughly twenty-five other stateless call
sites (`StatelessCommitService`, `HollowShardsService`, recovery and reshard
services) and in eleven server ones. Fixing them one by one is the
whack-a-mole that T4 warned about; T13 fixes the lookup instead.

### T13. Lookups by `Index` fall back to the UUID

`ProjectMetadata#index(Index)`, `hasIndex(Index)` and `getIndexSafe(Index)`,
and `RoutingTable#index(Index)` and `hasIndex(Index)`, resolve by name first
and, when the name is missing or belongs to an index with a different UUID,
fall back to the by-UUID map (`ProjectMetadata#indexByUuid`, and a matching
lazily built map on `RoutingTable`). `Metadata#lookupProject(Index)` and
`projectFor(Index)` get this for free: the single-project lookup delegates to
`ProjectMetadata#hasIndex(Index)`, and the multi-project lookup was already
keyed by UUID. `ClusterState#routingTable(..).shardRoutingTable(ShardId)` goes
through `index(Index)` too.

This is the decision that makes stale `Index` copies acceptable (T4): a
component that captured an `Index` before a rename keeps finding its metadata
and routing under the new name, and the only thing it gets wrong is the name
it prints. The common path is unchanged, since the fallback only runs after a
name miss. The one semantic change is that a stale `Index` for an index that
was deleted and recreated under the same name still resolves to nothing, as
before, while a stale `Index` whose name was later reused by a different index
now resolves to the renamed index rather than to nothing. `StaleIndexLookupTests`
covers both.

With the fallback in place `StatelessRenameIndexIT` passes: a rename in a
stateless cluster followed by a write with a dynamic mapping update, a plain
write, a refresh and a search through the search tier.

One subtlety the fallback introduces: code that detects a rename or a deletion
by asking "is this `Index` still in the metadata" must now look up by name
explicitly, because `index(Index)` answers yes for a renamed index.
`ClusterChangedEvent#indicesRenamed()` does so; the first version of the
fallback silently stopped every rename from being reported to the appliers,
which surfaced as "shard has not applied block" failures in `RenameIndexIT`.

Two fixes ride along: `RecoveryState` is renamed with its shard so that
`GET _recovery` groups the index under its current name, and
`index.provided_name` is updated to the new name, since tooling (including the
stateless test base class) reads it back as the index name.

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

Decided in favour of mutation; see T4 for the reasoning and the caveats
(partial mutation, stale logger prefixes).

### D4. Preconditions for a first release

Proposed: allow open and closed plain indices and any data stream backing or
failure index, write indices included; refuse system indices, indices with a snapshot,
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

### Modules, plugins and x-pack

Every `internalClusterTest` task under `modules/`, `plugins/` and `x-pack/`
(76 projects) was run with the trick enabled, against the commit before the
UUID-diff and batching changes (ea3d4b01), on the same machine, 1.5 hours.

| Area | Tests | Failed | Skipped |
|---|---|---|---|
| modules (29 projects) | 723 | 0 | 2 |
| plugins | 43 | 0 | 10 |
| x-pack except stateless | 27741 | 5 | 685 |
| x-pack stateless | 758 | 29 | 717 |

The x-pack failures outside stateless:

- `SnapshotBasedIndexRecoveryIT#testRecoveryConcurrentlyWithIndexing` and
  `NodeShutdownShardsIT#testReallocationForReplicaDuringNodeReplaceAfterShutdownOfReplica`.
  Both read `GET _recovery` for the index and find nothing under its name. A
  real stale-name bug, not a test artifact: `RecoveryState` copies the
  `ShardId` when a recovery starts and `TransportRecoveryAction` groups the
  response by that shard id's index name, so after a rename the recovery API
  lists the index under its old name. Fixed by renaming the shard's
  `RecoveryState` in `IndexShard#renameTo` (its shard id is now volatile).
- `CsvIT` `lookup-join.multipleBatchesWithAggregate3` timed out waiting for a
  task. Not re-examined; likely load, since the stateless suites were timing out
  in parallel.
- `SuppressWaitForActiveShardsActionFilterIT` (stateless companion plugin) hung
  for the same reason as the stateless suites below.

Stateless is the one area where the trick found a hard problem rather than
artifacts: 15 suites hit the 20 minute suite timeout, every one blocked in the
first write to a renamed index. See T12 for the cause and T13 for the fix. The
other stateless failures are `findIndexShard` lookups in the test base class
that resolve the index through `index.provided_name`, which still held the
temporary creation name; the prototype now updates that setting on rename.
With the fix (T13) the two stateless suites were rerun: 691 tests, 7 failed,
6 skipped, 25 minutes. None hangs on a write any more. What is left:

- `StatelessPreFlushRelocationIT` (two tests) and
  `StatelessAbortRunningMergesOnRelocationIT`: relocations of a renamed shard
  time out. The search-shard-information request sent during a stateless
  relocation carries a shard id that can still hold the old name, and
  `TransportFetchSearchShardInformationAction` looked the index up by that
  name string, so it hit a `NullPointerException`; it now resolves through the
  `Index` (T13). Not yet rerun after that change.
- `ObsoleteSegmentCacheEvictionIT` asserts the index name as reported by the
  search engine, which keeps its creation-time shard id (T4: the engine is one
  of the components that keep the old name). Test-framework artifact of the
  rename trick, but a reminder that `Engine#config().getShardId()` is stale
  after a rename.
- `StatelessRealTimeGetIT#testLiveVersionMapMemoryUsageTriggersFlush`: a
  live version map memory measurement came back as zero. Not examined.

Not yet run with the trick: REST tests, and the unit test suites that
construct `Index` objects by hand. Niels's last CI run of the trick extended to
`assertAcked` failed on security tests; that extension is not part of this
prototype.

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
   security tests (D1, T10).
7. Follow-ups: `RepositoryData` matching by UUID (D6); ILM and Upgrade
   Assistant adopting the rename.
