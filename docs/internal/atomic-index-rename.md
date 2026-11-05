# Atomic index rename: proof of concept

Status: proof of concept, not for merging as is. This document describes what
the prototype does, how it works, the tradeoffs it takes and what is still
missing before any of it is production quality. It is written for someone on
the Elasticsearch team who has not looked at the rename work before.

- Tracking issue: elastic/elasticsearch-team#1743 (public ask:
  elastic/elasticsearch#37880). The 2025 spikes by Niels Bauman and Mary
  Gouseti that this builds on are summarized there.
- Prototype PR: elastic/elasticsearch#164343.
- Serverless companion PR: elastic/elasticsearch-serverless#8144, on a fork
  branch of the same name so that the serverless check on the Elasticsearch
  PR pairs the two. Its own CI compiles against the Elasticsearch commit
  pinned in serverless `main`, so it cannot pass until the Elasticsearch PR
  merges.

## What it does

A rename is an action of the aliases API:

```
POST _aliases
{
  "actions": [
    { "rename_index": { "index": "my-index", "new_index": "my-index-renamed" } }
  ]
}
```

`POST /{index}/_rename/{target}` is a shorthand that builds a request with that
one action. Both run as `indices:admin/aliases` through
`TransportIndicesAliasesAction`, so a rename is applied in the same cluster
state update as the `remove_index` and alias actions of the same request. That
is what the zero-downtime reindex use case needs: delete the original, move its
alias and give the reindexed copy the original's name, atomically.

```
POST _aliases
{
  "actions": [
    { "remove_index": { "index": "my-index" } },
    { "add":          { "index": "my-index-reindexed", "alias": "my-alias" } },
    { "rename_index": { "index": "my-index-reindexed", "new_index": "my-index" } }
  ]
}
```

The index keeps its UUID, shards, settings, mappings, aliases, blocks, ILM
state and data stream membership; only the name under which it is addressed
changes. No shard is closed, recovered or relocated. Indexing and search
continue across the rename: requests in flight complete, and requests sent
under the old name afterwards fail with index not found.

What may be renamed: open and closed plain indices, data stream backing and
failure indices (write indices included), CCR leaders and followers, mounted
searchable snapshot indices, resize sources with a resize in flight, hidden
indices that a feature owns by naming convention, since delete allows those
too, and indices with a snapshot, restore or stateless reshard in flight.
A restore or reshard carries on under the new name; a snapshot carries on
as an image of the cluster as it started (T5). What is refused: a data stream or an alias as the source,
and system indices and system index names. Index metadata blocks (`read_only`,
`metadata`) refuse a rename the way they refuse a delete, because a rename is
a metadata write; a `write` block alone does not.

The caller needs `manage` on both the current and the new name. The cluster
must consist only of nodes that publish the `index.rename` node feature.

## How it works

The index UUID is the identity of an index. Everything already keyed on the
UUID survives a rename untouched: node data paths, persisted cluster state,
the stateless object store, and the data node's `IndicesService#indices` map.
Everything keyed on the name is rewritten in the cluster state update
(`ProjectMetadata#indices`, `RoutingTable`, `ClusterBlocks`, `DataStream`
index lists) or matched by UUID afterwards (a snapshot in flight).

### Master side

`TransportIndicesAliasesAction` makes the checks that do not depend on the
state the rename is eventually applied to, once, before the request is queued:
the feature gate, the source is a regular index rather than an alias, data
stream or system index, and the new name is well formed and not a system index
name. The aliases executor (`MetadataIndexAliasesService`) applies the actions
of a request in three phases, whatever their order in the request:
`remove_index` first, then alias changes, then renames. Every name in a request
therefore refers to the state before the request: an alias action cannot
address the new name, but a rename can take over the name of an index the same
request removed, and two renames in one request chain.

`MetadataRenameIndexService` makes the state-dependent checks inside the
executor, against the state produced by the actions before the rename (the
source exists, the new name passes `validateIndexName`), and then:

1. builds a new `IndexRoutingTable` for `new Index(target, uuid)` by rewriting
   the `ShardId` of every `ShardRouting`, and removes the old one;
2. moves the index blocks from the old name to the new name;
3. puts the `IndexMetadata` under the new name, with `index.provided_name`
   updated, and removes the old entry;
4. if the index belongs to a data stream, replaces it in place through
   `DataStream#renameIndex`, which keeps the generation (the generation counts
   rollovers and names the next backing index, so a rename must not move it);
5. renames the index in every `RestoreInProgress` entry that includes it, so
   that a restore in flight finishes under the new name. A `SnapshotsInProgress`
   entry is deliberately left alone (T5).

Nothing in the cluster state marks the rename. The request is acknowledged
once every node has applied the state, like any other aliases request. The
action type travels as a byte on the wire under the `rename_index_alias_action`
transport version, so a node refuses to send a rename to a node that would not
understand it with a clear message rather than a deserialization failure.

### Data node side

`ClusterChangedEvent#indicesRenamed()` reports every index whose previous name
is gone from a project while an index with the same UUID is still present
there, using a lazily built by-UUID map on `ProjectMetadata`
(`ProjectMetadata#indexByUuid`). `indicesDeleted()` excludes those indices.
`IndicesClusterStateService#renameIndices` runs first in the applier and calls
`IndicesService#renameIndex` for each, which updates the `IndexService`
metadata and calls `IndexShard#renameTo` on every shard.

Detection by UUID holds however many cluster states a node missed: a node that
was partitioned during a rename and rejoins later still finds the UUID under
the new name.

## Tradeoffs

Each item states what the prototype does, why, and what it costs.

### T1. `Index` identity is the UUID alone

`Index#equals` and `hashCode` ignore the name whenever the UUID is known (the
name is used only for the `_na_` placeholder), and `ShardId` therefore compares
by UUID and shard number. This is what lets data node structures keyed on
`Index` or `ShardId` (routing nodes, shard maps, search contexts, replication
groups, pending deletes) keep working after a rename without being rebuilt.
The alternative, keeping name-and-UUID equality and swapping every `Index`
instance on rename, means finding every such map across server, modules and
x-pack, and is why previous attempts stalled.

Costs:

- An `Index` object's `getName()` may be stale, so code that prints the name
  from a cached `Index` can print the old name. T4 lists where that happens.
- Unit test fixtures that give two indices the same UUID, usually `_na_`, now
  describe one index. Mary's elastic/elasticsearch#132443 cleans those up and
  should land first; the prototype fixed the few it hit.
- Cluster state diffs decide whether a component changed with `equals`, so a
  `Diffable` that holds `Index` objects looks unchanged after a rename and the
  diff carries nothing. `DataStream.DataStreamIndices#equals` and `hashCode`
  now compare names as well as UUIDs for this reason, with a diff round-trip
  test. `IndexMetadata` and the routing table are diffed as maps keyed by name
  and are safe; `RestoreInProgress` is copied whole on change. A
  `SnapshotsInProgress` entry diffs its shard map by `ShardId`, which compares
  by UUID, so a diff cannot carry a change of shard key names; that is one of
  the reasons a rename does not rewrite the entries (T5).
  Every future cluster state component that stores an `Index` needs the same
  care.
- A `HashMap#put` whose key is equal to an existing key refreshes the value and
  keeps the first key object, so a long-lived map keyed by `Index` keeps the
  creation-time name in its keys while its values are current. Code that takes
  the name from the key reads a stale name with no exception until it is used
  in a request. The serverless `SearchMetricsService`, which feeds the replicas
  autoscaler, had exactly this: it scaled a renamed index up and then failed
  to scale it down. The fix at each such holder is to re-key the entries that
  `indicesRenamed()` reports; in server the `Index`-keyed maps are per-task
  and do not outlive a cluster state.

### T2. Lookups by `Index` fall back to the UUID

`ProjectMetadata#index(Index)`, `hasIndex(Index)` and `getIndexSafe(Index)`,
and `RoutingTable#index(Index)` and `hasIndex(Index)`, look the name up and
return that index only if its UUID is the one asked for; otherwise, whether
the name is gone or now belongs to a different index, they fall back to the
by-UUID map. `Metadata#lookupProject(Index)` and `projectFor(Index)` get this
for free. The fallback never applies to the `_na_` placeholder.

The UUID check on the name hit is what makes a swap safe: rename `a` to `c`
and then `b` to `a`, and a request that resolved `Index(a, uuid-of-c)` before
the swap still reaches the index that is now `c`, never the one that is now
`a`. A bulk resolves its targets to `Index` objects when it is accepted and
every lookup on the write path takes an `Index`, so the documents land in the
index the request was accepted for, under its new name, which is the same
semantics an alias swap has today (`RenameIndexSwapIT` holds a bulk in flight
across a swap and checks that its documents land in the renamed index and
that the renamed index's blocks apply;
`TransportReplicationActionTests#testBlocksAfterIndexNamesSwapped` checks the
reroute phase directly; `RenameIndexIT#testBulkIndexingDuringRename` runs
bulks across a plain rename).

A bulk reports each item's `_index` as the name the index has when the
documents are written, not the name it had when the bulk was routed. The items
are written without a shard id of their own and read back with the one of the
`BulkShardResponse` they travel in, so that response is built from the
primary's own shard id (`BulkPrimaryExecutionContext#buildShardResponse`). Built
from the request's shard id it reported the old name, which in a swap is the
name of a different index: after `a` becomes `c` and `b` becomes `a`, a bulk
accepted for the first `a` would report `_index: a` while its documents are in
`c`. The reshard split helper still builds its merged response from the
original request's shard id, so a bulk that is split by a reshard while its
index is renamed reports the old name; that is cosmetic and not yet fixed.

This is what makes stale `Index` copies acceptable: a component that captured
an `Index` before a rename keeps finding its metadata and routing, and the
only thing it gets wrong is the name it prints. It was forced by stateless,
where the first bulk after a rename hung because `TranslogReplicator` resolved
the shard's creation-time `Index` by name and skipped the sync notification
that acknowledges the write; the same pattern existed in roughly twenty-five
other stateless call sites and eleven in server, and fixing them one by one
is the whack-a-mole that stalled earlier attempts. The common path is
unchanged, since the fallback only runs after a name miss. The one semantic
change is that a stale `Index` whose name was later reused by a different
index now resolves to the renamed index rather than to nothing
(`StaleIndexLookupTests`).

Three rules come with the fallback:

- A name string taken from a shard id must not be fed into a name-keyed
  lookup. The `Index` lookups check the UUID; a `String` lookup cannot, and
  after a swap it consults the other index without any error. The replication
  reroute phase checked index blocks that way, so a write routed before a
  swap was checked against the blocks of the index that took over the old
  name and not against those of its own index; shard store deletion read the
  index settings that way; and the stateless split state updates checked
  blocks that way. All now resolve the index through its `Index` first and
  use the current name.

  This was found by grepping server, x-pack and the modules for name-keyed
  lookups (blocks, the indices lookup, `index(String)`, name-keyed maps) whose
  key comes from a shard id or an `Index`, and reading each hit, and it is a
  review aid, not a proof. The rest were either built and consulted from one
  cluster state or one request's snapshot of names (health indicators,
  allocation, search and field-caps dispatch, broadcast replication, the
  transform checkpoint action), keyed by UUID with the name kept only to
  rebuild a `ShardId` (the searchable snapshots persistent cache), or only
  report a name.
- Code that detects a rename or a deletion by asking whether an `Index` is
  still in the metadata must look up by name explicitly, because
  `index(Index)` answers yes for a renamed index. `indicesRenamed()` does so.
- Authorization must never use it. `RBACEngine#shardIdAuthorized` grants
  access for the name in a shard id and then proves that the shard id belongs
  to that index; with the fallback, a forged shard id pairing an authorized
  name with another index's UUID resolved to that other index and passed
  (caught by `RemoteClusterSecurityFcActionAuthorizationIT`). The check now
  resolves by name and compares the UUID strictly. The production change
  should give the strict form its own method and audit every caller that uses
  an `Index` as proof of identity rather than as a key.
  That check only runs for cross-cluster API key requests, so it does not
  protect an action that is authorized by the index name in its request and then
  routed by the shard id inside it. Two single-shard actions were routed by the
  shard id's `Index`, and so by the fallback: the retention lease actions and
  `ShardChangesAction`. A user with access to one index name could name it and
  pair it with another index's UUID to remove a lease from, or read operations
  of, an index they could not see, with no rename involved, and a shard id
  captured before a rename kept working after the index moved outside the
  user's name pattern. Both now route by the index the authorized name resolves
  to and fail with index not found if the shard id's UUID does not agree
  (`RenameIndexSecurityTests#testForgedShardNameCannotRemoveAnotherIndicesRetentionLease`,
  `testStaleShardNameDoesNotRetainPrivilegesAfterRename`). The rule for any
  action that is authorized by name: route by what the name resolves to, and
  treat the shard id only as a claim to check.

### T3. Rename detection costs a by-UUID map per cluster state

`ProjectMetadata#indexByUuid` and its counterpart on `RoutingTable` are built
lazily on first use and cached, the same pattern as `indicesLookup`, and are
not serialized. At 15,000 indices the metadata map adds roughly 256 KB per
cluster state and a sub-millisecond build on first lookup. The builder asserts
that no two indices share a UUID; the production change should turn that into
a validation, since `indexByUuid` would silently merge them.

### T4. Data node state is mutated in place

`IndexShard#renameTo` replaces the shard's `ShardId` and `ShardRouting`
together under the shard's `mutex`, the same lock `updateShardState` takes for
relocations and promotions, so a rename never interleaves with a routing
update. `AbstractIndexShardComponent#shardId` is `volatile`, and
`IndexSettings#getIndex()` reads through the volatile `indexMetadata` field.
The rename reaches every shard component: the shard itself, its
`ShardGetService`, `Store`, `ReplicationTracker`, `ShardIndexWarmerService`,
`ShardBitsetFilterCache` and `RecoveryState`, and the current engine, which
renames itself, its translog and its merge scheduler. Each component's logger
is re-created with the new prefix (a Log4j marker cannot be changed in place),
after one `INFO` line under the old prefix naming the new shard id, so that
someone reading the log by the old name finds the pointer.

The alternative, an immutable `ShardId` with the name treated as advisory, is
not the cheaper option it looks like: `DocWriteResponse` writes `_index` from
the data node's shard id, so every write after a rename would report the old
name to the client, and the recovery API groups by the shard id's name.

What still keeps the creation-time name, all cosmetic because the UUID is the
identity:

| Holder | Where the name shows |
|---|---|
| `EngineConfig#getShardId` (the config is immutable; an engine created after the rename gets the new id) | An engine config read back by tooling |
| Readers opened before the rename, merges already running | Hot threads output of in-flight merges |
| A point in time or scroll opened before the rename | `_index` of hits from that context, until it is closed |

Everything else a user sees comes from state that is updated: write responses,
shard-level stats and `_cat` output, slow logs, recovery output, and request
and query cache statistics.

### T5. A renamed index is a new name to a snapshot repository

A repository identifies an index by an `IndexId` (`name`, `id`): the `id` is
minted the first time a name is snapshotted, every blob of the index lives
under `indices/<id>/<shard>/`, and the repository data is keyed by name, one
`id` per name. A renamed index is therefore a new name: it gets a new `id`, and
its first snapshot uploads every segment file again, so the first backup after
renaming a large index costs what the first backup of a new index does. Later
snapshots are incremental again. The snapshots taken under the old name keep
it and restore under it (`RenameIndexSnapshotIT#testSnapshotAndRestoreRenamedIndex`,
`testSnapshotAfterRenameUploadsUnderANewId`).

Giving the new name the old `id` would avoid the upload, and an earlier version
of the prototype did, but it cannot be made safe in this format. A later
index that takes the old name is, by name, the old `id` again, so two live
indices in one snapshot resolve to the same shard container and the same
index metadata blob, and restoring one gives it the other's data
(`testSnapshotAfterOriginalNameIsReused`). The name cannot be pointed at a
fresh `id` instead, because the repository data is keyed by name. The
master also keeps concurrent operations on one repository shard apart
through `RepositoryShardId`, which includes the name, so two names sharing a
container would not be seen as the same shard. Doing it properly takes a
format change that makes `id` the key and stores the name per snapshot, with
a minimum-version marker so that older nodes do not read it; that is the
follow-up if the re-upload turns out to matter.

A snapshot or restore in flight does not block the rename, which matters in
serverless, where backups run on a schedule the user neither sees nor
controls. A restore entry names the index it creates, so the rename rewrites
it (step 5 of the master side) and the restore finishes under the new name
(`testRenameWhileRestoring`). A snapshot entry is left alone, because it also
names the repository `IndexId` the shards are being written under, and that
cannot change while they are: renaming it would make the same `id` belong to
two names in the repository (`testRenameWhileSnapshottingAnIndexTheRepositoryKnows`
fails with an inconsistent repository when it does), and would hide the running
shard operations from the serialization above. The snapshot is instead an image
of the cluster as it started, with the index under the name it had then.
Whatever reads a shard of an entry against the cluster state goes through the
`Index` or `ShardId`, which compare by UUID, so the entry's stale names do not
matter to it. Finalization is the one place that looks an index up by the
entry's name, and `SnapshotsServiceUtils#projectForSnapshot` now looks it up
by UUID, stores its metadata under the entry's name, and rewrites the data
streams that contain it the same way, so that a restore of the data stream finds
its backing indices (`testRenameWhileSnapshotting`,
`testNameTakenOverWhileSnapshotting`, `ProjectForSnapshotTests`). A name that
another index took over in the meantime does not mix the two up.

The checks that protect an index with a snapshot in flight find the index the
same way. Deleting, closing or restoring over an index with a snapshot running
is refused through `SnapshotsServiceUtils#snapshottingIndicesBySnapshot`, which
used to look each entry index up by the name the entry holds. Since the entry
keeps its starting name, that missed a renamed index and wrongly blocked an
unrelated one that took the old name; it now compares the `Index` the entry
recorded by UUID. Likewise, a snapshot queued behind a repository deletion gets
its shard assignments when the deletion ends through `SnapshotsServiceUtils#shards`,
which now finds each index by the one the entry recorded and keeps the
shard's original UUID (`ProjectForSnapshotTests`). An entry that has no shards
yet has recorded no index, only a name, and is checked by name.

An alias cannot share a name with an index, and the view puts an index back under
a name it no longer has, so an alias added since the snapshot started with that
name, typically the old name kept as an alias for a migration, would make
finalization fail and discard a long backup. Finalization leaves such aliases
out of the view, for every index in it including the ones outside the snapshot
that a global-state snapshot carries, and does so before the metadata is built,
since building already rejects an alias named like its own index
(`testAliasReusesOriginalSnapshotName`, `testAliasWithTheOriginalNameOfAnotherIndex`).

Leaving an alias out makes the metadata differ from what the cluster has while its
versions stay the same, and the repository reuses an index metadata blob for any
later snapshot of the index that computes the same identifier from the UUID and
versions. An index that was not renamed keeps its repository id, so the next
snapshot, taken with no further change, would have reused the filtered blob and
restored the index without the alias, with both snapshots reporting success
(`testLaterSnapshotKeepsAliasFilteredFromEarlierSnapshot`). Finalization therefore
reports which indices it adjusted in this way
(`SnapshotsServiceUtils#indicesWithAdjustedMetadata`, carried by
`FinalizeSnapshotContext`), and the repository writes their metadata under an
identifier that is unique to the snapshot, the way it already does for metadata
adjusted for a resharding in progress.

One more consequence of an index appearing under two ids: the repository finds
an index metadata blob by an identifier built from the index UUID and the
versions of its metadata, but the blob lives in the container of one `id`. A
snapshot that was running across the rename is finalized under the old `id`
with the metadata as it is then, and the next snapshot, under the new `id`,
has the same metadata, hence the same identifier, and used to be pointed at a
blob in the other container. `BlobStoreRepository#identifierForContainer`
makes the identifier specific to the `id` when another `id` already holds it;
for any index that has only ever had one `id`, which is every index on a
cluster that has never renamed one, it returns the identifier it was given.

Costs: the first snapshot after a rename is a full upload, and the old-name
snapshots hold their own copy of the data until they are deleted.

### T6. Security authorizes both names, nothing more

A `rename_index` action reports both the current and the new name through
`indices()`, so the caller needs `manage` on both. That is the same bar as
creating the destination and deleting the source. Because the rename runs as
`indices:admin/aliases`, a role cannot grant alias management without
granting rename; nothing is lost, since both already require `manage`.
Document-level and field-level security rules are attached to index name
patterns, so a rename can move an index into or out of a user's DLS/FLS scope
exactly as creating it under that name would.

For a data stream backing index, `manage` on the data stream covers the
source name, as any backing index privilege does; the destination name does
not exist yet and belongs to no data stream, so it must be matched by a role
pattern directly. After the rename the index is still the data stream's
backing index, so access through the data stream name continues.
`RenameIndexSecurityTests` covers the model.

Document and field level security is applied to a shard by the name of its
index. The reader of a shard keeps the name the shard had when it was created,
while a request is authorized for the name the index has now, so a lookup by the
reader's name found no restrictions after a rename and returned the reader
unwrapped. `SecurityIndexReaderWrapper` now looks the permissions up by the
index's current name first and by the reader's name second
(`RenameIndexDlsFlsTests`).

### T7. Cross-cluster replication follows by UUID across a rename

CCR already identifies indices by UUID where it matters (the follower stores
`leader_index_uuid`, every shard-changes fetch is checked against the leader's
history UUID, auto-follow records followed leaders by UUID). Names were only
used as addresses, and that is where a rename broke things. A leader that
disappears fails the shard follow task fatally; a rename keeps the identity,
so it belongs with the retried cases. What was name-based and now resolves
through the UUID: the follower's leader metadata requests (mapping, settings
and alias sync), which on an `IndexNotFoundException` ask the leader cluster
for the current name by UUID once and remember it, as do the shard changes
requests, the retention lease renewals and additions of the follow task, and the
lease removal on unfollow: the leader's `ShardChangesAction` and
`RetentionLeaseActions` are authorized by the name in the request, so they
route by that name and refuse a shard id whose UUID disagrees (T2), which leaves
the caller to find the current name; and pause, unfollow and follow-stats, which
match tasks by UUID. The requests
the follow task sends to change the follower itself (put mapping, update
settings, the close, update and open sequence for non-dynamic settings, and
alias changes) are addressed by name, and the name in the task parameters is
the one the follower had when the task started. Each now resolves the
follower's current name by UUID when it sends the request, the three steps of
the close, update and open sequence each on their own, and fails if the
follower no longer exists rather than falling back to the old name, which an
unrelated index may by then have taken: before this, a renamed follower
stopped receiving leader mapping, settings and alias changes, and the
unrelated index with the old name received them, or was closed and reopened.
The name-keyed APIs leave a window between resolving the name and the request
being applied in which a second rename could still redirect it. The retention
lease one matters beyond CCR: every renewal failed after a leader rename, and
the lease would have expired after the retention period, at which point the
leader could discard history the follower still needed.

Resuming a paused follower after a leader rename has to find the leader too.
`TransportResumeFollowAction` fetches the leader's metadata by the name the
follower recorded when following began. It still tries that name first, as one
cheap request, and falls back to a lookup by the recorded UUID when the name no
longer denotes the leader: the fetch finds nothing, it finds an alias or data
stream, or it finds an unrelated index that took the old name, which validation
would reject as a different leader. The follow tasks it then creates address the
leader by its current name. `CcrRenameIT#testResumeAfterLeaderRename` and
`testResumeAfterLeaderNameIsReused` cover both.

A leader rename while the follower is still bootstrapping from the leader
fails that bootstrap, because the CCR repository addresses the leader by the
name in the snapshot index id; the follow request has to be retried.
`CcrRenameIT` covers a leader rename mid-replication, a follower rename, a
mapping, dynamic setting, alias and non-dynamic setting change on the leader
after a follower rename with an unrelated index created under the follower's old
name (which must be left untouched), and auto-follow with a renamed leader.

### T8. The `createIndex` rename trick is always on

`ESIntegTestCase#createIndex` creates a temporary index and renames it to the
requested name, unless the name matches a composable template, starts with
`.` or `<`, the creation settings carry an API block that forbids metadata
writes, or the test opts out (per call, or per class through
`renameIndexOnCreate()`). Every integration test that creates an index
through that helper therefore exercises the rename, and the sweeps described
under test coverage are what found most of the bugs above. It cannot be merged
as is: it slows every test by two extra cluster state updates and a health
check, and it makes unrelated failures look rename-related. Production should
make it a randomized or opt-in behavior, like other `ESIntegTestCase`
randomizations.

### T9. Mixed-version clusters are gated

A node without the `index.rename` feature treats a renamed index as deleted
(`indicesDeletedFromClusterState` compares by name) and deletes its shard
data. The prototype refuses renames until every node publishes the feature. A
node that joins a cluster after a rename is safe, because it starts from a
tombstone-based diff rather than a name diff. `IndexRenameRollingUpgradeIT`
checks that a rename is refused in a mixed cluster and works once the upgrade
completes. Whether the gate should also hold serverless rollouts in some way
needs a check with the control plane owners; the object store is keyed by
UUID and is unaffected.

## Decisions made

- **The API is an aliases action, with `_rename` as shorthand.** ILM's shrink
  and searchable snapshot steps and the Upgrade Assistant need rename,
  `remove_index` and alias moves to be one atomic cluster state update, which a
  standalone endpoint cannot give. There is no dedicated transport action, so
  there is one privilege path and one audit action name for both surfaces.
- **Data node state is mutated rather than treated as advisory.** See T4.
- **Renames are allowed wherever a delete is.** Hidden indices that a feature
  owns by naming convention (`.ml-*`, `.transform-*`, `.enrich-*`, profiling),
  and indices a feature is still building under a name it stored in its own
  state (ILM's shrink and searchable snapshot actions, downsample tasks), may
  be renamed by anyone with `manage` on both names. Renaming them breaks the
  feature the way deleting them would, and a delete is allowed today with the
  same privilege, so refusing would be stricter than the status quo for no
  clear gain. The reference docs should carry the same warning delete carries.
  Only system indices and names are refused.
- **The index need not be read-only.** The zero-downtime reindex use case
  depends on renaming a live index, and nothing in the write path keys on the
  name (`RenameIndexIT#testBulkIndexingDuringRename`).
- **Data streams themselves are not renamed, for now.** Mechanically it would
  be cheap: a data stream has no UUID or shards, so a rename is a metadata
  re-key plus the alias rewrite, and restore with `rename_pattern` already does
  all of it, backing index names included. The problem is the name's meaning.
  The new name must match a composable template with a data stream
  definition, because the next rollover resolves the template by name, and a
  rename to a differently covered name silently changes the next backing
  index's mappings and settings. Fleet encodes type, dataset and namespace in
  the name and writes the same values into every document as constant
  keywords, which a rename cannot fix. A CCR follower creates its local data
  stream from the leader's name, so renaming a leader splits the follower's
  history at the next rollover. And the reindex use case is already served for
  data streams by the modify data stream API and the reindex-data-stream
  migration built on it. If revisited: rename the data stream and all of its
  indices atomically as a separate `rename_data_stream` action, run the
  create-time name validation on the new name including the template match,
  refuse system and replicated data streams, and document that the template
  and Fleet meaning of the name is the caller's responsibility.

## Known gaps before production

- **A forged shard-level write can reach an index the sender may not access.**
  The replication reroute phase resolves the index of a shard-level request such
  as `indices:data/write/bulk[s]` through `ProjectMetadata#index(Index)`, which
  falls back to the UUID. A sender authorized for the name `old-public` who pairs
  it with the UUID of `new-private` writes to `new-private`
  (`RenameIndexSecurityTests` in the fourth review reproduces it through the
  transport action; there is no REST entry point). On main the strict lookup made
  such a request fail. It was accepted as a limitation, for two reasons. Only a
  principal that can send shard-level actions directly can use it, which in
  practice means a node that the cluster already trusts. And the replication
  layer cannot tell the forged pair from a bulk that legitimately follows a
  rename, whose old name after a swap also denotes another index, so closing it
  there would break what `RenameIndexSwapIT` asserts. The place to close it is
  authorization: apply the strict name and UUID check that
  `RBACEngine#shardIdAuthorized` makes for cross-cluster requests to every
  authentication type, for a shard-level request that arrives without a parent
  action that was already authorized.
- **The rename trick must become randomized or opt-in (T8).**
- **A strict by-name lookup method for authorization code (T2)**, so that
  reviewers do not have to spot the lenient `index(Index)` in security code.
- **Validation that no two indices share a UUID (T3)**, in place of the
  builder assertion.
- **Snapshot-based peer recovery cannot use snapshots taken before a rename.**
  `IndexSnapshotsService` finds the latest snapshot of a shard through the
  repository's `IndexId` for the shard's current index name, so for a renamed
  index it sees only snapshots taken under the new name, and none until the
  first one after the rename. Recovery then falls back to the ordinary peer
  path, which is slower but correct. Swapped names are safe: the recovery
  plan reuses a snapshot file only when its metadata matches the source's, as
  after a delete and re-create.
- **The reshard split helper and the shutdown progress tracker key by the
  name the request carried**, which can be stale during a rename. Both only
  affect the name reported or logged.
- **CCR's `leader_index_name` custom metadata** keeps the name the leader had
  when following started, and `_ccr/info` shows it; it should show the
  current name. Resuming a follower finds the leader by its recorded UUID, so
  nothing but the display depends on it.
- **A rename that lands between resolving a CCR follower's name and applying
  the request** can still redirect it. The window is narrow and cannot be
  closed while the follower-side APIs are addressed by name.
- **Serverless metering** records usage by index name; how billing should
  treat a rename is undecided. The metering tests opt out of the trick for
  now.
- **Features that store index names in their own configuration** (enrich,
  ML datafeeds, transform, watcher actions) stay the user's problem, as they
  are with reindex; the reference docs should say so.
- Public reference docs and a changelog; whether `_rename` is exposed in
  serverless.

## Rules for new code

These are what keep the next listener, map or lookup from needing rename
handling of its own:

- Key per-index or per-shard state by `Index` or `ShardId`, never by name. If
  the map outlives a cluster state and anything reads the name from its keys,
  re-key the entries that `indicesRenamed()` reports.
- React to `indicesDeleted()`, never to "this name is gone".
- Look indices up with `index(Index)`, except in authorization code, and in
  an action that is authorized by the name in its request, which must resolve by
  name and compare the UUID strictly. Never pass a shard id's
  name into a name-keyed lookup (blocks, the indices lookup, settings): a
  swap makes it find the wrong index with no error.
- A cluster state component that stores an `Index` must compare names in its
  `equals`, or its diff will not carry a rename.
- Anything that captures an index name at `IndexModule` creation time
  (plugin hooks, `IndexSettings` consumers) keeps the creation-time name.

## Test coverage

Direct tests of the rename:

- `RenameIndexIT`: simple and multiple renames, chains and swaps, recreating
  the original name, mapping changes and blocks across a rename, a metadata
  block refusing a rename, and bulk requests in flight across a rename.
- `RenameIndexSwapIT`: a bulk held by an action filter on the coordinating node
  while one aliases request swaps two names through a temporary one. The
  documents land in the renamed index, the items report its new name, none
  land in the index that took over the old name, and a write block put on the renamed index afterwards refuses the held
  bulk (without the reroute-phase fix this test fails: the block is looked up
  by the old name and missed). A control holds and releases a bulk with no
  rename. `TransportReplicationActionTests#testBlocksAfterIndexNamesSwapped`
  checks the reroute phase directly.
- `RenameIndexSnapshotIT` (rename during a snapshot and during a restore,
  including an index the repository already knows and a name taken over by
  another index, snapshot after rename and after the old name is reused),
  `ProjectForSnapshotTests` (finalization by UUID, data streams), `RenameIndexResizeIT` (clone, shrink and split from
  a renamed source, rename of the source while a clone target recovers),
  `RenameSystemIndexIT` (system source and system destination name refused),
  `SearchableSnapshotsRenameIT` (mounted index renamed and recovered again
  after a full restart), `DataStreamRenameIT` and `DataStreamRenameMetadataTests`
  (backing and write index renames, rollover afterwards), `CcrRenameIT` (T7),
  `StatelessRenameIndexIT` (writes, refresh and search through the search tier,
  and a search immediately after the acknowledgement),
  `StatelessReshardIT#testRenameDuringReshard` (a split held at its hand-off
  while the index is renamed completes under the new name: the resharding
  state lives in the index metadata and the split services address shards by
  UUID), `ClusterInfoServiceIT`
  (shard sizes refreshed after a rename), `RenameIndexSecurityTests` (T6),
  `IndexLifecycleInitialisationTests#testRenameIndexWhileWaitingOnStep` (ILM
  carrying on under the new name), `IndexRenameRollingUpgradeIT` (T9),
  `StaleIndexLookupTests` (T2), `IndexShardTests#testRenameToReCreatesLoggers`
  (T4), `MetadataIndexAliasesServiceTests` and `AliasActionsTests` (action
  parsing, validation and ordering), and the YAML suite under
  `rest-api-spec/test/indices.rename/`.

Suite-wide coverage comes from the rename trick (T8). The server, module,
plugin, x-pack and stateless internal cluster test suites and about 62,000
unit tests were run with it enabled, and the Elasticsearch and serverless CI
pipelines ran it on every push. The stale-name bugs it found, all fixed and
listed here as the places where names hide:

- `TranslogReplicator` and some twenty-five other stateless call sites
  resolving a captured `Index` by name (T2).
- `RecoveryState` copying the `ShardId`, so `GET _recovery` listed the index
  under its old name (T4).
- `index.provided_name`, which tooling reads back as the index name.
- `InternalClusterInfoService` keying shard sizes by `ShardId#toString`, so
  allocation saw no size for the renamed shards until the next 30-second
  refresh; it now refreshes when the state it applies carries a rename.
- ILM's `ExecuteStepsUpdateTask` handing steps the `Index` the step was
  queued with, which wait steps resolved by name; a step queued just before a
  rename failed once and moved the index to the ERROR step. It now hands them
  the index as it is in the state being updated.
- CCR retention lease renewals and leader metadata requests by name (T7).
- `RBACEngine#shardIdAuthorized` accepting the lenient lookup (T2).
- The serverless `SearchMetricsService` keeping stale names in map keys (T1).
- `ShardStartedTaskExecutor` keying the timestamp ranges it updates by the
  `Index` in the shard-started message, which carries the name the index had
  when the recovery began; a shard finishing recovery across a rename failed
  to start once and was retried on the next cluster state. It now uses the
  index from the matched routing.
- `ExpectedShardSizeEstimator` finding a clone, shrink or split source's
  metadata by `Index` but its routing table by the name recorded in the target,
  so renaming the source before the target allocated made the reroute throw and
  left the target unallocated (`RenameIndexResizeIT#testRenameSourceBeforeCloneCanAllocate`
  holds allocation off to hit that window).
- Bulk item responses reporting the name the index had when the bulk was
  routed, because the shard response was built from the request's shard id.
- Name-string lookups fed from a shard id (T2): the replication reroute
  phase's block check, shard store deletion's settings lookup and the
  stateless split state block checks, found by auditing for the swap case
  after a review comment pointed at it.

Tests that opt out of the trick, each with a comment saying why, fall into two
classes: tests that wait for an engine or commit event filtered by index name
(`StatelessPreFlushRelocationIT`, `StatelessAbortRunningMergesOnRelocationIT`,
`ObsoleteSegmentCacheEvictionIT`, `StatelessRealTimeGetIT`), and tests whose
timing or master-queue assumptions the extra cluster state updates break
(`ShardStateActionIT`, `SkippedClusterStateIT`, `RejectionActionIT`,
`WriteLoadConstraintDeciderIT`, and the serverless metering tests).

## Suggested sequence of production PRs

Each step is independently reviewable and leaves `main` working. The rename
becomes reachable at step 6; everything before it is identity and lookup
groundwork that is inert without the action, and everything after it widens
what a rename may do.

1. Test cleanup replacing `_na_` index UUIDs with unique ones (revive
   elastic/elasticsearch#132443). The serverless fixtures that share a UUID
   (`ReplicaRankingContextTests`, `ReplicasUpdaterServiceTests`) go in a
   serverless PR at the same time.
2. `Index#equals`/`hashCode` by UUID (T1), with the `ShardId` follow-through
   and the `DataStreamIndices` equality fix.
3. By-UUID index lookup and the lenient `index(Index)` (T2, T3), the strict
   variant for authorization with the `RBACEngine` fix, the UUID uniqueness
   validation, `ClusterChangedEvent#indicesRenamed()`, and the `index.rename`
   node feature (T9). Add a test that an ungated node refuses a state
   containing a rename rather than deleting data. The serverless
   `SearchMetricsService` re-keying goes in a serverless PR once
   `indicesRenamed()` exists.
4. Data node and master rename handling (T4): `IndexShard#renameTo` with the
   logger re-creation, the `RecoveryState`, `ClusterInfo` and
   `ShardStartedTaskExecutor` fixes, and the ILM and CCR fixes (T7). None of
   it is reachable yet, but it is the bulk of the review.
5. The `rename_index` aliases action, `MetadataRenameIndexService`, the
   `_rename` shorthand endpoint, the transport version and the security tests
   (T6). It does not refuse an index with a snapshot or restore in flight; the
   snapshot side needs the finalization by UUID and the metadata identifier
   fix of T5, so they go in the same step.
6. The rename-on-create trick in `ESIntegTestCase` (T8), randomized or
   opt-in, extended to the module and x-pack internal cluster test suites. It
   needs the action from step 5 to do anything, which is why it comes after
   it; the opt-outs listed under test coverage come with it.
7. Optional, if the re-upload after a rename matters: a repository format
   that makes the `id` the key and keeps the name per snapshot, with a
   minimum-version marker (T5).
8. ILM and Upgrade Assistant adopting the rename.
