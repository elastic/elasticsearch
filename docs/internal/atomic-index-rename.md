# Atomic index rename: proof of concept, tradeoffs and open decisions

Status: proof of concept. This document records what the prototype does, every
shortcut taken to get it working end to end, and the decisions that need to be
made before any of it is production quality. Tracking issue:
elastic/elasticsearch-team#1743 (public ask: elastic/elasticsearch#37880).

The prototype builds on Niels Bauman's and Mary Gouseti's 2025 spikes. Their
hand-off notes are in the tracking issue.

## What the prototype does

A rename is an action of the aliases API: `POST _aliases` with a
`rename_index` action (`{"rename_index": {"index": "a", "new_index": "b"}}`),
and `POST /{index}/_rename/{target}` as a shorthand for a request with that one
action. Both run as `indices:admin/aliases` through
`TransportIndicesAliasesAction` and the aliases executor
(`MetadataIndexAliasesService`), so a rename is applied in the same cluster
state update as the `remove_index` and alias actions of the same request. The
index keeps its UUID, shards, settings, mappings, aliases, blocks, ILM state
and data stream membership; only the name under which it is addressed changes.
No shard is closed, recovered or relocated, and indexing and search continue
across the rename.

The index UUID is treated as the identity of an index throughout. Everything
already keyed on the UUID survives a rename untouched: node data paths
(`NodeEnvironment`), persisted cluster state (`PersistedClusterStateService`),
the stateless object store (`ObjectStoreService`), and the data node's
`IndicesService#indices` map. Everything keyed on the name has to be rewritten
in the cluster state update (`ProjectMetadata#indices`, `RoutingTable`,
`ClusterBlocks`, `DataStream` backing index lists) or tolerated (snapshot
repository `IndexId`, see below).

### Master side

`TransportIndicesAliasesAction` validates a `rename_index` action (feature
gate, the current name is a regular index rather than an alias or a data
stream, neither name is a system index, the new name is free) and turns it into
an `AliasAction.RenameIndex`. The aliases executor applies the actions of a
request in three phases, whatever their order in the request: `remove_index`
first (as before), then alias changes, then renames
(`MetadataRenameIndexService#renameIndex`, mirroring
`MetadataDeleteIndexService#deleteIndices`). Every name in a request therefore
refers to the state before the request; an alias action cannot address the new
name, but a rename can take over the name of an index the same request removed,
and two renames in one request chain. Each rename:

1. builds a new `IndexRoutingTable` for `new Index(target, uuid)` by rewriting
   the `ShardId` of every `ShardRouting` (`ShardRouting#updateIndex`), and
   removes the old one;
2. moves the index blocks from the old name to the new name;
3. puts the `IndexMetadata` under the new name and removes the old entry;
4. if the index is a backing or failure index of a data stream, replaces it in
   the data stream.

Nothing in the cluster state marks the rename. The request is acknowledged once
every node has applied the state, like any other aliases request.

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
or `<`, the creation settings carry an API block that forbids metadata writes
(such an index cannot be renamed, by design), or the caller opts out. Every integration test that creates an index
through that helper therefore exercises the rename. The suite-wide result of
running `:server:internalClusterTest` with this enabled is recorded under
"Failure inventory" below.

### Trying it

```
PUT  /my-index
POST /my-index/_rename/my-index-renamed
GET  /my-index-renamed/_search
```

or, atomically with other alias actions, in the shape the ILM shrink swap
needs (delete the original, move its alias, give the shrunk index the
original's name):

```
POST _aliases
{
  "actions": [
    { "remove_index": { "index": "my-index" } },
    { "add":          { "index": "my-index-shrunk", "alias": "my-alias" } },
    { "rename_index": { "index": "my-index-shrunk", "new_index": "my-index" } }
  ]
}
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

### T3. Renames share the aliases task queue (resolved)

The first version of the prototype gave renames a dedicated
`MasterServiceTaskQueue`, so several renames batched into one cluster state
update but never with alias changes or `remove_index`. Renames are now an
action of the aliases executor (D1, option c), so they batch with everything
else that queue carries and are atomic with the other actions of their request.
The cost is that a rename adds a routing table rebuild to the `index-aliases`
queue, which is busy in some clusters because of Kibana alias churn; renames
are rare, so this is noise.

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

The mutation now reaches every shard component. `IndexShard#renameTo` renames
itself, its `ShardGetService`, `Store`, `ReplicationTracker`,
`ShardIndexWarmerService` and `ShardBitsetFilterCache` (all through
`AbstractIndexShardComponent#renameTo`, which also re-creates the component's
logger with the new prefix), and the current engine, which renames itself, its
translog and its merge scheduler. Before switching prefixes the shard logs one
`INFO` line under the old prefix naming the new shard id, so that someone
reading the log by the old name finds the pointer. A logger's prefix is a
Log4j marker that cannot be changed in place, which is why the loggers became
volatile fields that are replaced rather than updated.

What still keeps the creation-time name: `EngineConfig#getShardId` (the
config is immutable; an engine created after the rename gets a config with
the new id, one created concurrently with it may keep the old id until it is
replaced), objects that copied the shard id or a logger at construction
(`LiveVersionMap`, merge policies, the `ElasticsearchDirectoryReader` wrapper
of readers opened before the rename), and thread names of merges that were
already running. All cosmetic: the UUID is what identifies the shard.

Inventory of what keeps the creation-time name on a data node, by what a user
sees:

| Holder | Where the name shows | User impact |
|---|---|---|
| Shard-scoped loggers (`AbstractIndexShardComponent#logger` and the seven components built on it; the engine, translog and merge scheduler loggers) | Every log line about the shard: `[old][0]` | Was the highest: support reads logs by index name. Fixed: the loggers are re-created on rename, after an `INFO` pointer line under the old prefix (`IndexShardTests#testRenameToReCreatesLoggers`). |
| `EngineConfig#shardId` (immutable config), readers opened before the rename, merges already running | The id in an engine config read back by tooling; hot threads output of in-flight merges | Low and bounded: new merges and readers use the engine's renamed shard id. |
| `Store`, `Translog`, `ReplicationTracker`, `ShardIndexWarmerService`, `ShardBitsetFilterCache` | Their own log and exception messages | Fixed: renamed with the shard, see above. |
| A point in time or scroll opened before the rename | `_index` of hits from that context | Low and short-lived: only contexts that outlive the rename, and only their own hits. New searches report the new name because the coordinator builds the shard target from the current routing. |
| `ClusterInfo` shard-size keys on the master | Allocation decisions for up to one refresh interval | Fixed (D2): a rename triggers a refresh. |

Everything else a user sees comes from state that is updated: write responses
(`_index` from the renamed `IndexShard`), shard-level stats APIs and `_cat`
output (from `IndexShard#shardId` and the routing table), slow logs (from the
shard id at log time), recovery output (`RecoveryState#updateIndex`), and
request and query cache statistics (keyed by `ShardId`, equal by UUID).

The logger fix turned out smaller than estimated: the base class's
`renameTo` re-creates the logger for all seven components at once, the engine
gained a `renameTo` that the internal engine carries to its translog and
merge scheduler, and nothing branches on the prefix. `EngineConfig` was left
immutable.

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

### T6. Snapshot repositories key indices by name (incremental snapshots fixed)

`RepositoryData` resolves `IndexId` by index name, so the first version of the
prototype saw a "new" index after a rename and uploaded all of its segments
again instead of only the delta. The prototype now matches a renamed index to
the id it was snapshotted under before, by its UUID, so the next snapshot is
incremental again and old and new snapshots share the blobs (D6 has the
mechanism and the places that had to change). Restoring older snapshots is
unaffected (they restore under the name they were taken with, or a
`rename_pattern`). The prototype still refuses to rename an index while it is
being snapshotted or restored, mirroring `MetadataDeleteIndexService`; lifting
that needs the format change described in D6.

### T7. Two indices with one UUID would be silently merged

`indexByUuid` keeps one entry per UUID. Two live indices sharing a UUID is
impossible today (restore, clone, shrink and split all mint new UUIDs) and the
builder asserts it, but the production change should turn that into a
validation in `ProjectMetadata.Builder`.

### T8. Validation is split between the transport action and the executor

The checks that do not depend on the state the rename is eventually applied to
are made once, in `TransportIndicesAliasesAction`, before the request is
queued: feature gate, the current name is a regular index (not an alias, data
stream or system index), the new name is well formed and not a system index
name. The checks that do depend on it are made by
`MetadataRenameIndexService#validateRename` inside the executor, against the
state produced by the actions before the rename, which is what lets a rename
take over the name of an index the same request removes (a check made against
the state the request arrived with would refuse that): the current name
exists, the new name passes
`MetadataCreateIndexService#validateIndexName` (valid name, no existing index,
alias, ESQL view or dataset), and no snapshot or restore in flight for the
index. The request validators that plugins register for aliases requests
(CCR's, which protects a follower's replicated aliases) are not consulted for a
rename, which leaves the aliases alone. Resize sources and searchable snapshot
mounts are not checked (their settings move with the metadata) and are covered
by the tests below. The enrich, ML, transform and watcher references that store
index names in their own configuration stay the user's problem, as they are
today with reindex.

Cross-cluster replication is handled rather than refused, see T14. Tests now
cover: bulk requests in flight across a rename, which succeed because every
lookup on the write path resolves the shard by UUID, while requests sent under
the old name afterwards fail with index not found
(`RenameIndexIT#testBulkIndexingDuringRename`); rename refused during a
snapshot and during a restore, snapshot and
restore of a renamed index (`RenameIndexSnapshotIT`); clone, shrink and split
from a renamed source and a rename of the source while a clone target recovers
(`RenameIndexResizeIT`); rename of a system index and into a system index name,
both refused (`RenameSystemIndexIT`, which found that the destination was not
checked against system index patterns, now fixed); a mounted searchable
snapshot index renamed and recovered again after a full restart
(`SearchableSnapshotsRenameIT`); ILM carrying on under the new name
(`IndexLifecycleInitialisationTests#testRenameIndexWhileWaitingOnStep`, see
T15); and the mixed-cluster gate (`IndexRenameRollingUpgradeIT`, compiled but
not run locally, since it needs the previous version's distribution).

### T15. ILM steps queued before a rename saw the old name (fixed)

`ExecuteStepsUpdateTask` carries the `Index` the step was queued with and hands
it to the step. The task itself finds the index metadata through `index(Index)`,
which falls back to the UUID (T13), but several wait steps resolve the index by
name string from that `Index` (`WaitForRolloverReadyStep`,
`WaitForActiveShardsStep`, `WaitForIndexColorStep`,
`UpdateRolloverLifecycleDateStep`, `ReplaceDataStreamBackingIndexStep`, the
shrink, downsample and searchable snapshot action steps). A step task queued
just before a rename and executed just after it therefore fails once with a
`NullPointerException` and moved the index to the ERROR step. Fixed:
`ExecuteStepsUpdateTask` now hands the steps `indexMetadata.getIndex()`, the
index as it is in the state being updated, rather than the captured `Index`.
Found by the ILM rename test, whose test-only wait step happened to look the
index up by a fixed name and failed the same way. `IndexLifecycleRunner` still
keys some bookkeeping by index name (the busy-index set); a rename clears it
one cycle late at worst.

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
- Retention lease renewals. The follower keeps a retention lease on the leader
  shard alive with a background request, and the server-side
  `RetentionLeaseActions` resolved the shard by the name in the request, so
  after a leader rename every renewal failed with `IndexNotFoundException` and
  the lease would have expired after the retention period (12 hours by
  default), at which point the leader could discard history the follower still
  needs. It now resolves through the shard id's `Index`. This one is not
  CCR-specific: any remote caller of the retention lease actions with a stale
  name hit it.
- The follow-stats status reported the names captured when following started;
  it now reports the leader name learned after a rename and the follower's
  current name resolved by UUID.

Not changed, by design: a leader rename while the follower is still
bootstrapping (restoring from the leader through the CCR repository) fails
that bootstrap, because the repository addresses the leader by the name in the
snapshot index id; the follow request has to be retried. The test waits for the
bootstrap to finish before renaming.

Not changed: the follower's `leader_index_name` custom metadata keeps the name
the leader had when following started, and `_ccr/info` shows that. The
production change should show the current name instead.

`CcrRenameIT` covers a leader rename mid-replication (writes, a mapping change,
a settings change and an alias change afterwards all reach the follower), a
follower rename (replication continues, and stats, pause, resume and unfollow
work under the new name) and auto-follow with a renamed leader (no second
follower is created, the existing one keeps replicating).

### T9. The REST surface is an `_aliases` action plus a shorthand endpoint (resolved)

`rename_index` is an action of `POST _aliases`, next to `add`, `remove` and
`remove_index`, and `POST /{index}/_rename/{target}` builds a one-action
aliases request. There is no dedicated transport action: both surfaces run as
`indices:admin/aliases`. Two details of the aliases API shape the action:

- `TransportIndicesAliasesAction` resolves every action's names against the
  state it received the request with, before the task is queued, so the new
  name cannot be addressed by another action of the same request. The executor
  runs renames last so that this ordering is also the one the user sees.
- The action type travels as a byte on the wire. A new transport version
  (`rename_index_alias_action`) lets a node refuse to send a rename to a node
  that would not understand it, with a clear message rather than a
  deserialization failure. The `index.rename` node feature still gates the
  request on the master.

### T10. Security authorizes both names, nothing more

A `rename_index` action reports both the current and the new name through
`indices()`, so the caller needs `manage` (any privilege matching
`indices:admin/*`) on both names. That is the same bar as creating the
destination and deleting the source. Because the rename runs as
`indices:admin/aliases`, a role cannot grant alias management without granting
rename; nothing is lost today, since both already require `manage`, but it is
a consequence of D1 worth stating. Security's special handling of
`IndicesAliasesRequest` resolves each action's names and hands the pair back
through `indices(String...)`; the action stores it as current and new name
again. Document-level and field-level security
rules are attached to index name patterns in roles, so a rename can move an
index into or out of a user's DLS/FLS scope exactly as creating it under that
name would. Niels's last CI run failed on security tests when the rename trick
was extended to `assertAcked`; those failures have not been re-examined.

Data stream backing indices follow the usual rule: a privilege granted on the
data stream's name covers its backing indices, so `manage` on the data stream
authorizes the source name of a backing index rename. The destination name does
not exist yet and belongs to no data stream, so it must be matched by a role
pattern directly; `manage` on the data stream alone is refused. After the
rename the index is still the data stream's backing index, so access through
the data stream name continues under the new name.

`RenameIndexSecurityTests` (x-pack security internal cluster tests) covers the
model through the aliases action: `manage` on both names succeeds; `manage` on the source alone, or on
neither, is refused and leaves the index untouched; a backing index is renamed
when the source is authorized through the data stream and the destination by
pattern, and refused with the data stream privilege alone.

One rule in the security plugin itself had to change for the UUID-only `Index`
equality: `RBACEngine#shardIdAuthorized` resolved the shard's index through
`ProjectMetadata#index(Index)`, which after T13 falls back to a by-UUID lookup.
Authorization must only ever match the name the user was authorized for, so
that check now looks the index up by name and additionally requires the UUID to
match. Any future authorization code must use the strict by-name lookup, never
`index(Index)`.

### T11. The `createIndex` rename trick is always on

In the prototype every `ESIntegTestCase#createIndex` call renames unless the
test class overrides `renameIndexOnCreate()` or the call opts out. That is what
produces the failure inventory, but it cannot be merged as is: it slows every
test by two extra cluster state updates and a health check, and it makes
unrelated test failures look rename-related. Production should make it a
randomized or opt-in behavior, like other `ESIntegTestCase` randomizations,
keeping the class-level hook for tests that observe creation-time names.

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

One hard rule comes with the fallback: authorization must never use it. The
fulfilling-cluster check for shard-level actions (`RBACEngine#shardIdAuthorized`)
grants access for the *name* in a shard id and then proved that the shard id
belongs to that index with `hasIndex(Index)`. With the fallback, a forged shard
id pairing an authorized name with another index's UUID resolved to that other
index and passed, which `RemoteClusterSecurityFcActionAuthorizationIT` caught
in CI. The check now resolves by name and compares the UUID strictly. The
production change should give the strict form its own method
(`ProjectMetadata#indexStrict(Index)` or similar) and audit every caller that
uses an `Index` as proof of identity rather than as a key; the security module
had only this one.

Two fixes ride along: `RecoveryState` is renamed with its shard so that
`GET _recovery` groups the index under its current name, and
`index.provided_name` is updated to the new name, since tooling (including the
stateless test base class) reads it back as the index name.

## Decisions that need to be made

### D1. Where the API lives (decided: c)

Options: (a) dedicated `_rename` endpoint only; (b) a `rename` action inside
`POST _aliases` only; (c) both, with the endpoint delegating to the aliases
executor. The ILM shrink and searchable snapshot steps and Upgrade Assistant
need rename, `remove_index` and alias moves to be one atomic cluster state
update, which argues for (b) or (c). (c) was chosen and is what the prototype
now does: discoverability for the simple case, atomicity for the automated
ones. It went one step further than (c) as written, dropping the dedicated
transport action so that there is one privilege path and one audit action name
(`indices:admin/aliases`) for both surfaces. The engineering consequences are
in T3, T8, T9 and T10; none argued against the choice.

### D2. Which other appliers need `indicesRenamed()` (audited)

`IndicesClusterStateService` is the only applier that needs to know about
renames. Every other `ClusterStateApplier` and `ClusterStateListener` in
server, the modules, x-pack and stateless was triaged, and the ones that keep
per-index or per-shard state were read. Two properties make them safe without
changes: `Index` and `ShardId` equality is by UUID (T1), so a map keyed by
either still finds its entry under the new name, and `indicesDeleted()`
excludes renamed indices, so nothing that reacts to deletions reacts to a
rename. The lenient `index(Index)` / `hasIndex(Index)` lookups (T13) cover the
rest.

Verified safe, with the mechanism that makes them so:

- `IndicesStore`: `folderNotFoundCache` is pruned with `hasIndex(Index)`, the
  shard-active check sends the index UUID and the receiving node resolves the
  shard through the UUID-keyed `IndicesService` map.
- `SearchService`: no cluster state listener; contexts are freed only on
  index removal, which a rename does not trigger, and request-side lookups go
  through `indexServiceSafe(Index)`. Scrolls and points in time opened before
  a rename keep working; hits from such a point in time may report the old
  name, since the shard targets in its id carry it.
- `SnapshotShardsService` (aborts shard snapshots of deleted indices, by
  `ShardId`), `BlobStoreCacheMaintenanceService` (searchable snapshot cache
  cleanup on deletion, by snapshot and index id), `StatelessMemoryMetricsService`
  (by `ShardId`; the re-registration under the new name is a no-op because
  the keys are equal), `TimestampFieldMapperService` (by `Index`, pruned with
  `index(Index)`), `RemoveRefreshClusterBlockService` and
  `ShardsMappingSizeCollector` (by `Index`), `ThrottlingRecoveryService` (by
  allocation id and `ShardId`), `PeerRecoverySourceService` and
  `RecoveryDirectCancellationService` (react to nodes and routing changes
  only), `DelayedAllocationService` and `DiskThresholdMonitor` (recompute
  from the routing table each time), `DanglingIndicesState` (excludes on-disk
  indices by UUID, so a renamed index is never dangling).
- ILM: `IndexLifecycleService` removes deleted indices from the policy
  registry and otherwise iterates the current indices; the runner's
  `busyIndices` dedup set is keyed by `Index`; the cluster state update tasks
  look their index up with `index(Index)` and re-put the metadata under its
  current name (T15 covers the one task that had to be told the current
  name). Data stream lifecycle's error store is keyed by `Index`.
- CCR: `ShardFollowTaskCleaner` decides whether a follower still exists with
  `index(Index)`, so renaming a follower does not get its task cleaned up
  (`CcrRenameIT#testRenameFollowerWhileFollowing`); `AutoFollowCoordinator`
  tracks followed leaders by UUID, so a renamed leader is not followed twice
  (`CcrRenameIT#testAutoFollowDoesNotFollowRenamedLeaderAgain`); the shard
  follow tasks themselves are T14.
- Stateless: `ObjectStoreService` garbage collection compares object store
  index UUIDs with the cluster state (T12); the commit, cleanup, relocation
  and shutdown services key by `ShardId`; the maps keyed by `String` in them
  are blob file names, not index names.
- SLM, transform, ML, watcher, security, monitoring, enrich and the
  remaining listeners keep no per-index state, or manage indices they own by
  name (see below).

One gap found and fixed: `InternalClusterInfoService` keys shard sizes by a
string built from `ShardId#toString`, which includes the index name, and it
refreshed only on the `cluster.info.update.interval` timer (30 seconds by
default) or when a data node joined. After a rename, allocation deciders and
the desired balance saw no size for the renamed index's shards until the next
refresh: a less informed decision, not a wrong one, but for up to 30 seconds.
The service now also refreshes when the state it applies carries a rename
(`ClusterInfoServiceIT#testShardSizesRefreshedAfterRename`). Keying the map by
UUID would remove the need, but the string key is visible in the allocation
explain output, so the refresh is the smaller change.

Two classes of consumer remain the user's problem, as they are with a delete
or a reindex: features that record index names in their own state and act on
them later (ILM's shrink and searchable snapshot actions store the name of the
index they are building; downsample persistent tasks, ML datafeeds, transform
and enrich configurations and watcher actions name their indices), and
features that own hidden, non-system indices by naming convention (`.ml-*`,
`.transform-*`, `.enrich-*`, profiling). Renaming such an index while the
feature is using it breaks the feature the way deleting it would. D4 should
decide whether some of these are refused like system indices.

The rules for a new listener, so that it needs no rename handling: key
per-index state by `Index` or `ShardId`, never by name; react to
`indicesDeleted()`, never to "this name is gone"; look indices up with
`index(Index)`, except in authorization code (T13).

### D3. Data node mutation versus advisory names

Decided in favour of mutation; see T4 for the reasoning and the caveats
(partial mutation, stale logger prefixes).

### D4. Preconditions for a first release

What the prototype does: allow open and closed plain indices, any data stream
backing or failure index (write indices included), CCR leaders and followers
(T14) and mounted searchable snapshot indices; refuse system indices and
system index names, and indices with a snapshot or restore in flight (T6).
A resize in flight is not refused and is tested (`RenameIndexResizeIT`). Do
not require the index to be read-only: the zero-downtime reindex use case
depends on renaming a live index, and nothing in the write path keys on the
name. The prototype does respect index metadata blocks (`read_only`,
`metadata`), exactly as delete and close do, because a rename is a metadata
write; a `write` block alone does not stop a rename, which is what the ILM
shrink and searchable snapshot flows need.

Still to decide, found by the D2 audit: whether to refuse indices that a
feature owns by naming convention and tracks by name (`.ml-*`, `.transform-*`,
`.enrich-*`, profiling, the serverless metering records), and whether to
refuse a rename while a feature is building the index under a name it stored
in its own state (ILM's shrink and searchable snapshot actions, downsample
tasks). Renaming those breaks the feature the way deleting them would; a delete
is allowed today, so refusing a rename would be stricter than the status quo.
The cheapest consistent rule is to refuse nothing beyond system indices and
document it; the safest is to refuse hidden indices by default with an
override, the way the delete of a system index needs the system-index header.

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

What the cost is, precisely. A repository identifies an index by an `IndexId`
(`name`, `id`): the `id` is a random string minted by `resolveNewIndices` the
first time a *name* is snapshotted, and every blob of the index lives under
`indices/<id>/<shard>/`. Incremental snapshots work by reading the shard's
`index-N` listing under that path and uploading only the segment files it does
not already have. After a rename the new name is unknown to the repository, so
it mints a new `id`, finds an empty shard path, and uploads every segment file
again. The old `id` keeps its blobs until the last snapshot referencing it is
deleted; until then the index is stored twice. Restores are unaffected.

Sketch of matching by UUID without a format change. The repository already
records, per snapshot and `IndexId`, an index metadata identifier that starts
with the cluster index UUID (`IndexMetaDataGenerations#buildUniqueIdentifier`),
so when an index with UUID U arrives under a new name the master can find the
`IndexId` last used for U and reuse its `id` under the new name:
`new IndexId(newName, sameId)`. The shard paths then match and the upload is
incremental again. Three places assume one name per `id` and need care:

- `ShardGenerations` and `RepositoryData#indexSnapshots` are keyed by the
  whole `IndexId` (name and `id`), so the new `IndexId` starts with no shard
  generations and the data node would again see an empty listing. The
  generations of the old `IndexId` have to be carried over when the snapshot
  entry is built in `SnapshotsService`.
- Snapshot deletion rewrites each affected shard's `index-N` per `IndexId`.
  Two `IndexId`s sharing an `id` must be processed once, by `id`, or the two
  rewrites of the same shard path race. Stale-container cleanup is already by
  `id` (`survivingIndexIds`), so it is safe as is.
- An older node reading a repository where two names share an `id` would run
  the per-`IndexId` deletion twice. The repository's minimum-version marker
  should be raised the first time an `id` is shared, so older nodes refuse to
  write to it.

The first form is now in the prototype:

- `RepositoryData#resolveNewIndices` takes the index UUID of each name; for a
  name new to the repository it looks for the `IndexId` last used for that
  UUID (`indexIdForIndexUuid`, through the metadata identifiers) and reuses
  its id under the new name. The on-disk format is unchanged: the `indices`
  object simply has two names carrying the same `id`.
- `ShardGenerations` resolves a lookup that misses by exact `IndexId` through
  any other `IndexId` with the same id, and a `put` updates every name with
  that id, so the names never disagree about the shard listing. Both names
  write identical generation lists to disk.
- `SnapshotsServiceUtils#shards` decides whether an index is new to the
  repository by id, not by name, so the renamed index starts from the existing
  shard generations rather than from `NEW_SHARD_GEN`.
- Snapshot deletion (`BlobStoreRepository`) processes each id once, with the
  union of the snapshots that reference it under any name, so deleting every
  old-name snapshot rewrites the shared shard listings to keep the new-name
  snapshot's files instead of dropping the whole shard path. The old name
  disappears from `RepositoryData` when its last snapshot goes; the container
  survives through the new name.
- `RepositoryData` parsing: the per-snapshot index metadata lookup is stored
  by id, and the parser resolved each id to an `IndexId` through a map keyed
  by id, so the second name won and the first name's snapshots lost their
  metadata identifiers. The parser now picks the name whose snapshot list
  contains that snapshot. (This is also the proof that the on-disk format can
  carry two names with one id without change: a snapshot references exactly
  one of them.)
- `BlobStoreTestUtil#assertConsistency` expects one container per distinct
  id, and expects the metadata blobs of every name sharing it.

`RenameIndexSnapshotIT#testSnapshotAfterRenameIsIncremental` shows the second
snapshot uploading no segment file, the old-name snapshot deleted with the
new-name one still restorable, and a third snapshot incremental again; the
base class checks repository consistency after every test.

Not done: the repository minimum-version marker. `RepositoryData` writes a
`min_version` so that older nodes refuse to read a format they do not
understand, but the code that writes it carries a note that no numeric
`IndexVersion` can be written helpfully (versions before 8.11 parse it as a
dotted version string), so adding a new marker needs its own design. Until
then an older node reading a repository where two names share an id would
process the shared shard path once per name on deletion, with the race
described above. For a production change this must be solved first, either
with a new marker or by making the sharing invisible to old readers.

The cleaner long-term form remains a format change that makes `id` the key,
stores the current name and the index UUID as attributes, and resolves names
per snapshot from `SnapshotInfo`; that is also what would allow lifting the
T6 refusal of renames during a snapshot, since `SnapshotsInProgress` and
`RepositoryShardId` would stop carrying names.

## Failure inventory

This section is a log of what the rename trick (T11) found, in the order it
was found, not a current status. Every failure below is resolved, by a fix or
by an opt-out, and the subsections after the baseline record what later
sweeps and CI runs added. The current state is: all suites listed here pass
on the PR head, and the open items are the two serverless-side test changes
in the CI subsection.

### Baseline sweep

`:server:internalClusterTest` on main at f992e630 with the rename trick enabled
in every `createIndex` call (2026-11-02, macOS, 15.5 minutes), before the
aliases rework and the later merges with main:

| Result | Count |
|---|---|
| Tests run | 3979 |
| Failed | 6 |
| Skipped | 14 |

The six failures of that sweep, and what each one said about the rename:

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
run on their own after the three opt-outs above. A later merge with main
brought in `UpdateSettingsIT#testMultipleSettingsUpdateWithMetadataWriteBlock`,
which creates a metadata-blocked index; the trick now skips such creations
(see the CI subsection).

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
  task. All 319 lookup-join cases pass when run on their own, so this was load
  from the stateless suites timing out in parallel.
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
  `StatelessAbortRunningMergesOnRelocationIT`: still fail after the
  `TransportFetchSearchShardInformationAction` lookup was moved to the `Index`
  (T13, a real fix, kept). The remaining cause is on the test side: both tests
  block a commit or a merge and then wait for an engine-level event (a
  pre-flush observer, a merge recorder) filtered by index name, and the engine
  reports the creation-time name, so the event is never matched and the
  relocation the test is holding open never finishes. The same shape as
  `ObsoleteSegmentCacheEvictionIT` and `StatelessRealTimeGetIT` below, and a
  concrete consequence of T4 worth deciding on: anything that keys engine or
  commit events by index name (plugin hooks, operational tooling) sees the old
  name for a renamed shard until it is reallocated.
- `ObsoleteSegmentCacheEvictionIT` asserts the index name as reported by the
  search engine, which keeps its creation-time shard id (T4: the engine is one
  of the components that keep the old name). Test-framework artifact of the
  rename trick, but a reminder that `Engine#config().getShardId()` is stale
  after a rename.
- `StatelessRealTimeGetIT#testLiveVersionMapMemoryUsageTriggersFlush` counts
  commit notifications whose compound commit's shard id matches the index name;
  the commit carries the creation-time name, so the count stays zero. Same
  cause as above.

All three suites pass on plain main, so none of this is pre-existing flakiness.
These classes, and `WriteLoadConstraintDeciderIT`, now opt out of the rename
through `ESIntegTestCase#renameIndexOnCreate()`, a class-level hook that
replaces per-call opt-outs; each override says why.

### Unit test suites

The unit test tasks CI runs for server, the test framework, the modules and
x-pack plugins touched by the branch, plus ml, security, sql and monitoring
(about 62,000 tests) were run against the branch. What they found, beyond
tests of the new code:

- Two fixtures gave two indices the same UUID (`TranslogReplicatorTests`
  registers `name1` and `name2` under one UUID; `TransportUnfollowActionTests`
  used an empty UUID on the task and the placeholder on the index). With UUID
  identity those are the same index. The fixtures now use distinct UUIDs. This
  is the category elastic/elasticsearch#132443 cleans up at scale.
- The by-UUID fallback must never apply to the `_na_` placeholder: a point in
  time referencing a non-existent index with the placeholder UUID resolved to
  an unrelated test index. `ProjectMetadata` and `RoutingTable` now skip the
  fallback for it, and the routing table's by-UUID view omits such indices.
- `ShardChangesAction` returned a `NullPointerException` instead of
  `ShardNotFoundException` for a missing shard after the lookup change; fixed.
- The new `indicesByUuid` field had to be registered with the heap accounting
  test as a derived view.
- `PutTransformActionRequestTests#testCloudCredentialRoundTripPreservesValue`
  failed once on a locale-dependent string and passes on repeat; unrelated.

### CI

The PR's CI builds (each push starts one and cancels the previous) found, on
top of the local sweep: the security authorization regression above; the
rolling-upgrade gate test assuming the old version lacks the feature, which is
false for an upgrade within the same minor where the "old" nodes are built from
this branch (the test now consults `oldClusterHasFeature`); and the logger
usage check on a log line inherited from the spike. All fixed. Niels's last CI
run of the trick extended to `assertAcked` failed on security tests; that
extension is not part of this prototype.

After the move to the aliases action, CI found three more things. The
flakiness job, which reruns touched test classes many times, hit the three
`AliasActionsTests` methods that draw a random action type and assumed the
old three (fixed). `UpdateSettingsIT`, merged from main after the local sweep,
creates an index with `index.blocks.read_only` in its creation settings; the
trick then tried to rename a metadata-blocked temporary index, which is
refused by design, and the read-only temporary index broke the cluster wipe
(the trick now skips creations whose settings block metadata writes). And the
serverless checks, which run the core YAML tests against a real stateless
cluster, failed the rename YAML tests with "no shard available": the setup
asked for `number_of_replicas: 0`, which in serverless means no search copies
at all, so any search would have failed, rename or not (the setting is
removed; verified locally against the serverless build, where all five rename
YAML tests pass, and a new `StatelessRenameIndexIT#testSearchImmediatelyAfterRename`
checks that a search works right after the acknowledgement without waiting
for green).

Two serverless failures need companion changes in the serverless repository
and are left open here: `ReplicaRankingContextTests` and
`ReplicasUpdaterServiceTests` build several `Index` fixtures with the same
UUID, which collapse under T1 (the same class of fixture fix as step 1 of the
production sequence); and `RawStorageMeteringIT` and `UsageReportIT`, which
run with the T11 trick and assert on records keyed by the index name, so the
temporary name shows up in their expectations (either opt the classes out
with `renameIndexOnCreate()` or decide how metering should treat a rename).

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
6. The `rename_index` aliases action, `MetadataRenameIndexService`, the
   `_rename` shorthand endpoint, the transport version and the security tests
   (D1, T9, T10).
7. Follow-ups: `RepositoryData` matching by UUID (D6); ILM and Upgrade
   Assistant adopting the rename.
