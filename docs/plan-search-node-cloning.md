# Plan: Search Node Cloning

## Goal

Provide an API that, when called on a search node, freezes its local blob cache, flushes
all occupied slot data to the cloud network-attached volume, writes a compact metadata
file describing the current cache state (including the node's own ES node ID), and then
calls the cloud provider's volume snapshot API. The API returns the snapshot ID.

A new replacement node starts from that snapshot volume. On startup it reads the metadata
file, restores the in-memory cache index from it, and advertises via a node attribute
that it is the cache-warm replacement for the original node. The cluster allocator
preferentially moves the original node's shard allocations to the replacement node. Once
all shards have transferred, the original node leaves the cluster. The replacement node
serves those shards immediately from its warm cache without re-fetching from the object
store.

---

## Assumptions & storage prerequisites

This mechanism assumes the `shared_snapshot_cache` file resides on a **cloud
network-attached block volume** (AWS EBS, GCP Persistent Disk, Azure Managed Disk, or
equivalent). On these volumes the storage backend acknowledges a write only after it has
reached persistent media, so `sync_file_range(WAIT_AFTER)` provides the same durability
guarantee as `fdatasync` from the application's perspective — there is no volatile
device-side write-back DRAM buffer that survives independently of the block device
acknowledgement.

**Do not enable this mechanism on nodes whose cache file lives on local ephemeral NVMe
SSDs.** On local NVMe, `sync_file_range` does not flush the device write-back cache,
so the metadata file may describe ranges whose bytes were never durably persisted,
leading to corrupted (not merely stale) cache entries on restore.

**The snapshotted volume must be the node's full data path root** — the single directory
that contains both `shared_snapshot_cache` (the cache file) and `nodes/0/` (which holds
`node.json` and persistent node state). `SharedBytes` asserts `nodeDataPaths().length == 1`
and resolves the cache file from `nodeDataPaths()[0]`. A configuration that mounts the
cache file on a separate block volume from `nodes/0/` is not supported: the startup script
would find no `node.json` on the cache volume and the identity-swap logic would not work.

The mechanism is disabled by default and must be explicitly opted in via a setting
(see Step 0). The setting serves as an operator assertion that the storage prerequisites
above are satisfied.

---

## Key classes

| Class | Module | Role |
|---|---|---|
| `SharedBlobCacheService<K>` | `blob-cache` | Generic LFU cache; `freeRegions`, `keyMapping`, `CacheFileRegion`; gains `slotToRegion[]`, `StampedLock`, freeze/unfreeze |
| `SharedBytes.IO` | `blob-cache` | One physical slot; `pageStart = sharedBytesPos * regionSize`; gains `getSlotIndex()` |
| `SparseFileTracker` | `blob-cache` | Tracks completed byte ranges per region |
| `CacheFileRegion<K>` | `blob-cache` | One logical region; `tracker` is `final`; gains restore constructor |
| `LFUCache` | `blob-cache` (inner) | `assignToSlot` assigns slots; `closeInternal` returns them; gains `restoreEntries()` |
| `StatelessSharedBlobCacheService` | `stateless` | Concrete subclass; gains startup restore and `snapshotService` wiring |
| `FileCacheKey` | `stateless` | `record(ShardId, long primaryTerm, String fileName)` |
| `CacheSnapshotService` | `stateless` *(new)* | Freeze+flush+write+cloud-API sequence; file I/O |
| `CloudVolumeSnapshotProvider` | `stateless` *(new)* | Per-cloud implementation of `createSnapshot()` |
| `TransportCacheSnapshotAction` | `stateless` *(new)* | Node transport action; returns `snapshotId` |
| `CacheRestoredAllocationDecider` | `stateless` *(new)* | `canAllocate(ShardRouting, RoutingNode, RoutingAllocation)` — labels matching (shard, node) pairs for audit trail in `GET /_cluster/allocation/explain`; does not affect numeric ranking |
| `StatelessBalancingWeightsFactory` | `stateless` | Blanket weight reduction for any node with `es_cache_restored_from_node` set, ranking the replacement above other eligible nodes |

---

## Design overview

```
[guarded by stateless.cache_snapshot.enabled = true]

Normal operation (unchanged — zero overhead)

Snapshot API call (POST /_stateless/cache/snapshot on the source node)
  1. Acquire cache freeze lock
       — blocks all mutation and search-I/O entry points (fetchRegion, CacheFile.populate, forceEvict, shard-close, …)
       — spin-waits until all in-flight gap-fill async completions land (activeGapFills → 0)
       — after this point the set of completed ranges across all slots is stable
  2. Phase 1: sync_file_range(WRITE) for each occupied slot   [non-blocking: start I/O]
  3. Phase 2: sync_file_range(WAIT_AFTER) for each occupied slot   [wait per slot]
  4. Write CacheSnapshotFile to volume:
       header(numRegions, regionSize, sourceNodeId) +
       for each occupied slot: (slot, key, region, completedRanges)
  5. fsync CacheSnapshotFile
  6. Call cloud volume snapshot API  →  snapshot ID
  7. Release cache freeze lock
  8. Return snapshotId to caller

Replacement node startup (from snapshot volume)
  1. Read CacheSnapshotFile header — extract sourceNodeId
  2. If node.json nodeId == sourceNodeId: delete node.json   [force fresh identity]
  3. ES generates new nodeId; node sets node attribute:
       node.attr.es_cache_restored_from_node = <sourceNodeId>
  4. Full read of CacheSnapshotFile → restore occupied slots (O(n))
  5. Delete CacheSnapshotFile (state is now in memory)
  6. Node joins cluster; master sees the attribute

Shard handoff (operator-triggered)
  1. Operator decommissions source: exclude._id = sourceNodeId
  2. DesiredBalanceShardsAllocator recomputes desired balance
  3. CacheRestoreAwareWeightFunction (searchWeightFunction) gives replacement node
       a blanket negative weight delta → replacement ranked above other search nodes
  4. Shards relocate to replacement node (stateless: no data transfer)
  5. Operator clears exclusion; source node terminated
  6. Attribute is harmless after source departure; absent from next restart
```

---

## Step 0 — Feature flag

The entire snapshot mechanism — freeze lock, metadata file, and transport action
registration — is gated by a single node-scope boolean setting:

```java
// StatelessSharedBlobCacheService.java
public static final Setting<Boolean> STATELESS_CACHE_SNAPSHOT_ENABLED_SETTING = Setting.boolSetting(
    "stateless.cache_snapshot.enabled",
    false,
    Setting.Property.NodeScope
);
```

Default is `false`. An operator sets it to `true` only on nodes where the cache file
resides on a cloud network-attached block volume (see "Assumptions" above).

When the setting is `false`:
- The freeze lock and `CacheSnapshotService` are never instantiated.
- The transport action is registered but returns an immediate error if called, explaining
  the setting requirement.
- Normal operation is completely unaffected — zero overhead.
- The pre-warming skip guards (Step 8) remain active independently of this flag.

---

## Step 1 — `SharedBytes.IO.getSlotIndex()`

**`SharedBytes.java`** — add one accessor (same as v1):

```java
public int getSlotIndex() {
    return Math.toIntExact(pageStart / regionSize);
}
```

---

## Step 2 — Freeze mechanism and always-on slot-to-region index

### 2a. Always-on `slotToRegion` array

`regionOwners` in `SharedBlobCacheService` is only populated when `Assertions.ENABLED`
(i.e., never in production). To read the current occupant of a physical slot at freeze
time, add an always-on parallel array:

```java
// SharedBlobCacheService.java
@SuppressWarnings("unchecked")
private final CacheFileRegion<KeyType>[] slotToRegion =
    (CacheFileRegion<KeyType>[]) new CacheFileRegion<?>[numRegions];
```

Wire it in `assignToSlot` (line 2368), after `entry.chunk.volatileIO(freeSlot)`:
```java
slotToRegion[freeSlot.getSlotIndex()] = entry.chunk;
```

Clear it in `CacheFileRegion.closeInternal` (line 1144), before `freeRegions.add(io)`:
```java
blobCacheService.slotToRegion[io.getSlotIndex()] = null;
```

`getCompletedRangesForSlot` now uses this array instead of the null-in-production
`regionOwners`:

```java
protected SortedSet<ByteRange> getCompletedRangesForSlot(int slot) {
    CacheFileRegion<KeyType> region = slotToRegion[slot];
    return region != null ? region.tracker.getCompletedRanges()
                          : Collections.emptySortedSet();
}
```

### 2b. Cache freeze lock

The freeze mechanism has two parts: a `StampedLock` that blocks new slot mutations, and
an `AtomicInteger` counter that tracks in-flight gap fills. Both are needed for
correctness.

#### Why two mechanisms

Acquiring the write stamp blocks new calls to all entry points that can trigger slot
assignment, eviction, or gap-fill dispatch. These include the `SharedBlobCacheService`
fetch methods (`fetchRegion`, `maybeFetchRegion`, `fetchRange`, `maybeFetchRange`) and the
`CacheFile` I/O methods (`CacheFile.populate`, `CacheFile.populateAndRead`), which are
called directly by `CacheFileReader` on the active search path. The stamp on the `CacheFile`
methods also drains in-flight search reads: `freeze()` cannot acquire the write stamp until
all threads holding a read stamp (including active searches) have released it. New searches
block waiting for a read stamp until the freeze releases.

In addition, `populate()` and `populateAndRead()` dispatch gap-fill runnables that kick
off async blob-store I/O before returning. A gap fill outstanding when the write stamp was
acquired continues writing bytes into the cache file until the blob-store response arrives.
If `sync_file_range` runs before those bytes land, and the metadata is written after the
ranges are marked complete in `SparseFileTracker`, the metadata describes ranges whose
bytes were not flushed. The `activeGapFills` counter — decremented in each gap's
completion listener, not when the runnable method exits — drains those async fills before
`sync_file_range` starts.

#### `StampedLock`

```java
// SharedBlobCacheService.java
private final StampedLock freezeLock = new StampedLock();
```

**All mutation entry points** acquire a read stamp before any `synchronized` block. The
list below covers the known entry points that can trigger slot assignment, eviction, or
gap-fill dispatch. **During implementation, verify this list against the actual
codebase** — any entry point that calls `populate()` or `populateAndRead()` (directly or
transitively) and is not in the table leaves gap fills unguarded by the freeze:

| Entry point | Mutation |
|---|---|
| `fetchRegion()` / `maybeFetchRegion()` | slot assignment via `assignToSlot`; gap-fill dispatch via `populate` |
| `fetchRange()` / `maybeFetchRange()` | gap-fill dispatch via `populate` / `populateAndRead` |
| `CacheFile.populate()` / `CacheFile.populateAndRead()` | gap-fill dispatch via `CacheFileRegion.populateAndRead`; called by `CacheFileReader` directly on the active search I/O path |
| `maybeEvictLeastUsed()` | eviction via `closeInternal`; called inside `fetchRegion` before `synchronized` |
| `forceEvict()` / `forceEvictAsync()` | eviction via `closeInternal` |
| shard-close eviction path (stateless `removeFromCache`) | eviction via `closeInternal` |

Each of these acquires the read stamp at its outermost point, before any `synchronized`
block. Acquiring inside the synchronized block deadlocks: the write-lock waits for all
read stamps to drain, but a thread holding the global lock cannot release its stamp until
it exits the synchronized block while the write-lock attempt is pending.

```java
// Sketch — applies to all fetch entry points, CacheFile I/O methods, forceEvict,
// and shard-close paths:
long stamp = freezeLock.readLock();
try {
    // ... synchronized(this) block and executor dispatch happen inside here ...
} finally {
    freezeLock.unlockRead(stamp);
}
```

#### `activeGapFills` counter

```java
// SharedBlobCacheService.java
private final AtomicInteger activeGapFills = new AtomicInteger(0);
```

The counter is managed entirely inside `fillGapRunnable` — one increment per gap at
creation time, one decrement when the gap's data has **actually landed in the page cache**
and `SparseFileTracker` has marked the range complete. Call sites (`populate`,
`populateAndRead`) require no `activeGapFills` bookkeeping.

`fillGapRunnable` wraps the passed listener with `ActionListener.runAfter(listener,
activeGapFills::decrementAndGet)`. `ActionListener.run(countedListener, ...)` calls the
listener on every exit path: blob-store success (bytes written, `gap.onCompletion()`
called), blob-store failure, and `SparseFileTracker` abort. This is correct — "complete"
means bytes are in the page cache and the range is marked done, not when the dispatch
`Runnable` method returns.

```java
// SharedBlobCacheService.java (CacheFileRegion inner class) — modified fillGapRunnable
private Runnable fillGapRunnable(
    SparseFileTracker.Gap gap,
    RangeMissingHandler writer,
    @Nullable SourceInputStreamFactory streamFactory,
    ActionListener<Void> listener
) {
    activeGapFills.incrementAndGet();   // one per gap; decremented via countedListener
    final ActionListener<Void> countedListener =
        ActionListener.runAfter(listener, activeGapFills::decrementAndGet);
    return () -> ActionListener.run(countedListener, l -> {
        var ioRef = nonVolatileIO();
        int start = Math.toIntExact(gap.start());
        writer.fillCacheRange(
            ioRef, start, streamFactory, start,
            Math.toIntExact(gap.end() - start),
            progress -> gap.onProgress(start + progress),
            l.<Void>map(unused -> {
                blobCacheService.writeCount.increment();
                gap.onCompletion();   // bytes in page cache, range marked complete
                return null;
            }).delegateResponse((delegate, e) -> failGapAndListener(gap, delegate, e))
        );
    });
}
```

The only unhandled path is executor rejection (runnable never executed), which does not
occur with internal ES thread pools that do not have backpressure rejection semantics.

#### `freeze()` / `unfreeze()`

`freeze()` first acquires the write stamp (blocking all new read stamps, i.e., all new
entry-point calls), then spin-waits until `activeGapFills` reaches zero. At that point:
- No new gap fills can be dispatched (all entry points are blocked).
- All in-flight gap fills have written their bytes to the page cache and their
  `SparseFileTracker` completion listeners have fired.
- The set of completed ranges across all slots is stable.
- `sync_file_range` can proceed with a consistent view.

```java
/** Acquires the write stamp and drains in-flight gap fills. */
long freeze() {
    long stamp = freezeLock.writeLock();
    // all entry points are now blocked; drain any gap fills already in flight
    while (activeGapFills.get() > 0) {
        Thread.onSpinWait();
    }
    return stamp;
}

/** Releases the write stamp acquired by freeze(). */
void unfreeze(long stamp) {
    freezeLock.unlockWrite(stamp);
}
```

The spin-wait terminates because: once the write stamp is held, no new gap fills can be
dispatched, and the bounded set of already-in-flight async gap fills will eventually
complete their blob-store fetches, write bytes to the cache file, and call their
completion listeners — decrementing the counter to zero.

These are package-private methods on `SharedBlobCacheService`, called only by
`CacheSnapshotService`. Warm hits that read directly from already-cached data (via
`CacheFile.tryRead`, which acquires no entry-point stamp) are unaffected by the freeze.
Cache-miss reads that go through `CacheFile.populate()` or `CacheFile.populateAndRead()`
block while the write stamp is held, then proceed normally once the freeze ends; any gap
fills they would dispatch are also blocked during the freeze window.

---

## Step 3 — `getNumRegions()`, snapshot read API, and restore

**`SharedBlobCacheService.java`**:

```java
public int getNumRegions() { return numRegions; }
public int getRegionSize()  { return regionSize; }
```

`CacheIndexEntry` record (no `assignSeq` field needed — the freeze lock eliminates the
slot-reuse race that required sequence tracking in the journal design):

```java
public record CacheIndexEntry<K>(
    int physicalSlot, K key, int region, int effectiveRegionSize, SortedSet<ByteRange> completedRanges
) {}
```

`restoreOccupancy(List<CacheIndexEntry<KeyType>>)` — delegates to
`LFUCache.restoreEntries(...)` (Step 4).

`CacheFileRegion` restore constructor (unchanged from v1):
```java
CacheFileRegion(..., SortedSet<ByteRange> completedRanges) {
    ...
    tracker = new SparseFileTracker("file", regionSize, completedRanges);
}
```

---

## Step 4 — O(n) `LFUCache.restoreEntries()`

The original plan called `freeRegions.remove(io)` for each entry — O(n) per call, O(n²)
total. The fix: drain `freeRegions` once into a `Map`, then restore.

```java
void restoreEntries(List<CacheIndexEntry<KeyType>> entries) {
    // Build slot-index → IO map from all free regions (O(n), one pass)
    final Map<Integer, SharedBytes.IO> slotMap = new HashMap<>(numRegions * 2);
    SharedBytes.IO io;
    while ((io = freeRegions.poll()) != null) {
        slotMap.put(io.getSlotIndex(), io);
    }

    int restoredCount = 0;
    synchronized (SharedBlobCacheService.this) {
        for (CacheIndexEntry<KeyType> entry : entries) {
            final int slot = entry.physicalSlot();
            if (slot < 0 || slot >= numRegions || entry.completedRanges().isEmpty()) continue;

            final SharedBytes.IO freeSlot = slotMap.remove(slot);  // O(1)
            if (freeSlot == null) {
                logger.warn("skipping restored slot {}: already claimed", slot);
                continue;
            }
            final RegionKey<KeyType> regionKey = new RegionKey<>(entry.key(), entry.region());
            final CacheFileRegion<KeyType> chunk = new CacheFileRegion<>(
                SharedBlobCacheService.this, regionKey,
                entry.effectiveRegionSize(), UNKNOWN_TIMESTAMP, entry.completedRanges()
            );
            final LFUCacheEntry lfuEntry = new LFUCacheEntry(chunk, epoch.get());
            if (keyMapping.computeIfAbsent(entry.key().shardId(), regionKey, k -> lfuEntry) != lfuEntry) {
                slotMap.put(slot, freeSlot);   // collision: return slot to map
                continue;
            }
            slotToRegion[slot] = chunk;   // keep always-on index current
            pushEntryToBack(lfuEntry);
            chunk.volatileIO(freeSlot);
            evictionPolicy.onCached(chunk);
            restoredCount++;
        }
    }

    // Return all unclaimed slots to freeRegions (O(n), one pass)
    freeRegions.addAll(slotMap.values());
    initialFreeRegions.addAndGet(-restoredCount);
}
```

Total cost: O(n) to drain, O(k) for restore loop with O(1) map lookups, O(n−k) to
re-add remaining slots. Overall O(n).

---

## Step 5 — `CacheSnapshotService` (new class, in `stateless` plugin)

**`x-pack/plugin/stateless/…/cache/CacheSnapshotService.java`**

Owns the freeze+flush+write+cloud-API sequence. Constructed and held by
`StatelessSharedBlobCacheService` when the feature flag is enabled.

### 5a. Snapshot metadata file format

Written as a JSON document using `XContentBuilder` (JSON XContent type) to a fixed path
alongside `shared_snapshot_cache` (e.g., `shared_snapshot_cache.snapshot`). The file is
written entirely to a `.tmp` sibling and then renamed, so a crash mid-write leaves the
previous file intact or no file at all — never a partial file.

```json
{
  "version": 1,
  "num_regions": 65536,
  "region_size": 16777216,
  "node_id": "xBKf3nMbQF-7...",
  "entries": [
    {
      "slot": 0,
      "key": {
        "index": "my-index",
        "shard": 0,
        "primary_term": 3,
        "file_name": "_0.cfs"
      },
      "region": 4,
      "ranges": [
        { "start": 0, "end": 1048576 },
        { "start": 2097152, "end": 3145728 }
      ]
    }
  ]
}
```

`version` allows future format evolution — a reader that does not recognise the version
discards the file and starts cold. `num_regions` and `region_size` are validated against
the running node's configuration on read; a mismatch discards the file and starts cold.
A truncated or malformed JSON document fails parsing and is also discarded.

The `node_id` field is the persistent ES node ID of the node that created the snapshot
(`NodeEnvironment.nodeId()`). Because the file is plain JSON, the startup script can
extract it with a single `jq` call without any custom binary parsing (see Step 7a).

### 5b. `CloudVolumeSnapshotProvider` interface

The cloud API call is injected to keep `CacheSnapshotService` cloud-agnostic:

```java
public interface CloudVolumeSnapshotProvider {
    /**
     * Initiates a snapshot of the volume that backs the cache file.
     * Returns the provider-assigned snapshot ID once the snapshot request
     * has been accepted (the actual data copy may still be in progress
     * in the background on the cloud side).
     */
    String createSnapshot() throws IOException;
}
```

One implementation per cloud provider (AWS EBS, GCP PD, Azure). The active
implementation is resolved at node startup from `NodeEnvironment` / node settings and
injected into `CacheSnapshotService`.

### 5c. Snapshot sequence

```java
SnapshotResult snapshot(StatelessSharedBlobCacheService cache) throws IOException {
    // 1. Acquire the write stamp — drains all in-flight slot ops and blocks new ones
    long stamp = cache.freeze();
    try {
        // 2. Capture source node identity and enumerate occupied slots
        String sourceNodeId = cache.getNodeId();   // NodeEnvironment.nodeId()
        List<Integer> occupiedSlots = cache.getOccupiedSlots();   // reads slotToRegion[]

        // 3. Phase 1: kick off writeback for all occupied slots (non-blocking)
        for (int slot : occupiedSlots) {
            cache.syncSlotRange(slot, SYNC_FILE_RANGE_WRITE);
        }

        // 4. Phase 2: wait for writeback to complete per slot
        for (int slot : occupiedSlots) {
            cache.syncSlotRange(slot, SYNC_FILE_RANGE_WAIT_AFTER);
        }

        // 5. Collect completed ranges and write metadata file (includes sourceNodeId)
        List<CacheIndexEntry<FileCacheKey>> entries = new ArrayList<>(occupiedSlots.size());
        for (int slot : occupiedSlots) {
            SortedSet<ByteRange> ranges = cache.getCompletedRangesForSlot(slot);
            if (ranges.isEmpty()) continue;
            CacheFileRegion<FileCacheKey> region = cache.getSlotRegion(slot);
            entries.add(new CacheIndexEntry<>(slot, region.regionKey().file(),
                region.regionKey().region(), cache.getRegionSize(), ranges));
        }
        writeSnapshotFile(sourceNodeId, entries);   // → tmp → force → atomic rename
        // snapshot file is now durable on the volume

        // 6. Call cloud snapshot API — if this fails, delete the metadata file.
        //    The file accurately describes the current slot state, but once the freeze
        //    is released, evictions and reassignments will proceed. If the node later
        //    restarts from the original volume, the metadata could map a slot to a key
        //    whose data has since been overwritten, causing silent corruption on restore.
        //    Deleting the file forces a cold start instead.
        String snapshotId;
        try {
            snapshotId = cloudProvider.createSnapshot();
        } catch (Exception e) {
            try {
                Files.deleteIfExists(snapshotFilePath);
            } catch (IOException ioe) {
                e.addSuppressed(ioe);
            }
            throw e;
        }

        logger.info("cache snapshot [{}] captured {} regions", snapshotId, entries.size());
        return snapshotId;
    } finally {
        // 7. Always release the freeze lock, even on error
        cache.unfreeze(stamp);
    }
}
```

`getOccupiedSlots()` is a new package-private method on `SharedBlobCacheService` that
iterates `slotToRegion[]` and returns the indices of non-null entries — O(numRegions).

`getSlotRegion(int slot)` is a new package-private method that returns `slotToRegion[slot]`
— O(1). Called only under the freeze write stamp, so no additional synchronization is
needed.

`writeSnapshotFile(sourceNodeId, entries)` builds the JSON document using
`XContentBuilder`, writes it to a `.tmp` file via a `FileOutputStream`, calls
`FileChannel.force(false)` on the underlying channel, then does an atomic rename over
the real path.

---

## Step 6 — `CacheSnapshotService.readSnapshotFile()` — O(n) restore

The read returns a `SnapshotMetadata` record carrying the entries and the source node ID.
The file is either a valid, complete JSON document or it is discarded entirely — a
truncated or malformed document fails XContent parsing and triggers a cold start.
The file is streamed rather than loaded fully into memory, so heap usage is bounded
regardless of the number of entries.

```java
record SnapshotMetadata(
    String sourceNodeId,
    List<CacheIndexEntry<FileCacheKey>> entries
) {
    static final SnapshotMetadata EMPTY = new SnapshotMetadata(null, List.of());
}

static SnapshotMetadata readSnapshotFile(
    Path snapshotFile, int numRegions, int regionSize
) {
    if (Files.notExists(snapshotFile)) return SnapshotMetadata.EMPTY;
    try (InputStream is = Files.newInputStream(snapshotFile);
         XContentParser parser = XContentType.JSON.xContent()
             .createParser(XContentParserConfiguration.EMPTY, is)) {

        parser.nextToken();  // START_OBJECT

        String sourceNodeId = null;
        List<CacheIndexEntry<FileCacheKey>> result = new ArrayList<>();

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String field = parser.currentName();
            parser.nextToken();
            switch (field) {
                case "version" -> {
                    if (parser.intValue() != 1) {
                        logger.warn("unsupported snapshot version; starting cold");
                        return SnapshotMetadata.EMPTY;
                    }
                }
                case "num_regions" -> {
                    if (parser.intValue() != numRegions) {
                        logger.warn("snapshot num_regions mismatch; starting cold");
                        return SnapshotMetadata.EMPTY;
                    }
                }
                case "region_size" -> {
                    if (parser.intValue() != regionSize) {
                        logger.warn("snapshot region_size mismatch; starting cold");
                        return SnapshotMetadata.EMPTY;
                    }
                }
                case "node_id"  -> sourceNodeId = parser.text();
                case "entries"  -> result = parseEntries(parser, numRegions, regionSize);
                default         -> parser.skipChildren();
            }
        }

        if (sourceNodeId == null) {
            logger.warn("snapshot missing node_id; starting cold");
            return SnapshotMetadata.EMPTY;
        }
        return new SnapshotMetadata(sourceNodeId, result);
    } catch (IOException e) {
        logger.warn("failed to read cache snapshot; starting cold", e);
        return SnapshotMetadata.EMPTY;
    }
}
```

`parseEntries` iterates the `entries` JSON array, constructing a `CacheIndexEntry` per
object. Unknown fields are skipped for forward compatibility. Entries with an out-of-range
slot or empty ranges list are discarded.

After `restoreOccupancy` completes, delete the snapshot file:

```java
Files.deleteIfExists(snapshotFile);
```

---

## Step 7 — Wire into `StatelessSharedBlobCacheService` and transport action

### 7a. Pre-startup: node.json deletion and attribute injection (startup script responsibility)

This step happens **before the ES process starts**, so it cannot be performed by a plugin
hook. The `nodeId` in `node.json` is read by `NodeEnvironment` during early bootstrap —
before plugins are even loaded — so any plugin code would be too late to influence the
node's identity.

The snapshot file is already present on the volume the replacement node boots from.
Because the file is plain JSON, the startup script (Kubernetes init container, deployment
entrypoint) can extract `sourceNodeId` with a single `jq` call — no binary parsing, no
operator input needed. It is responsible for two actions before starting ES:

**1. Delete `node.json` if it matches `sourceNodeId`**

The snapshot volume contains `node.json` from the source node. If the replacement node
starts with that file, it presents the same ES node ID as the still-live source node.
Two nodes with the same ID cannot coexist: the join is rejected.

```bash
SNAPSHOT="${ES_PATH_DATA}/nodes/0/shared_snapshot_cache.snapshot"
NODE_JSON="${ES_PATH_DATA}/nodes/0/node.json"

SOURCE_NODE_ID=$(jq -r '.node_id' "${SNAPSHOT}")

# node.json may not exist (e.g. snapshot volume was prepared without one)
if [ -f "${NODE_JSON}" ]; then
    CURRENT_NODE_ID=$(jq -r '.node.id' "${NODE_JSON}")
    if [ "${SOURCE_NODE_ID}" = "${CURRENT_NODE_ID}" ]; then
        rm "${NODE_JSON}"
    fi
fi
```

ES then generates a fresh ID at startup.

**2. Set the allocation attribute**

Pass the attribute via `-E` on the ES command line (or the equivalent env-var mechanism
used by the deployment). Do **not** append to `elasticsearch.yml` — repeated clones
would accumulate duplicate entries:

```bash
exec elasticsearch -E "node.attr.es_cache_restored_from_node=${SOURCE_NODE_ID}"
```

This attribute is included in the `DiscoveryNode` sent during cluster join and is visible
to the master's allocator immediately.

### 7b. Construction

By the time the `StatelessSharedBlobCacheService` constructor runs, the operator has
already deleted `node.json` (if needed) and set the node attribute. The constructor
restores the cache state from the snapshot file if one is present:

```java
// After super() returns (freeRegions populated, slotToRegion[] allocated):
final boolean snapshotEnabled = STATELESS_CACHE_SNAPSHOT_ENABLED_SETTING.get(settings);

if (snapshotEnabled) {
    Path snapshotFile = snapshotFilePath(environment);
    SnapshotMetadata metadata =
        CacheSnapshotService.readSnapshotFile(snapshotFile, getNumRegions(), getRegionSize());

    if (metadata.sourceNodeId() != null) {
        // File was successfully read and structurally valid (has a node_id).
        // Restore entries if present; delete the file in either case to prevent
        // re-reading on future restarts (even if entries happened to be empty).
        if (metadata.entries().isEmpty() == false) {
            restoreOccupancy(metadata.entries());
        }
        Files.deleteIfExists(snapshotFile);
        logger.info("restored {} cache regions from snapshot of node [{}]",
            metadata.entries().size(), metadata.sourceNodeId());
    }

    CloudVolumeSnapshotProvider cloudProvider = resolveCloudProvider(settings, environment);
    this.snapshotService = new CacheSnapshotService(snapshotFile, cloudProvider);
} else {
    this.snapshotService = null;
    logger.debug("cache snapshot disabled (stateless.cache_snapshot.enabled = false)");
}
```

No `close()` override is needed — there is nothing to flush at shutdown.

### 7c. Transport action — `TransportCacheSnapshotAction`

A new node-targeted transport action invokes the snapshot sequence on the local node:

```
Action type string:  cluster:admin/stateless/cache/snapshot
```

**Request**: `CacheSnapshotRequest` — no parameters (targets the local node by default;
node routing handled by the REST layer via `?node_id=`).

**Response**: `CacheSnapshotResponse`

```java
public record CacheSnapshotResponse(String snapshotId) implements Writeable { ... }
```

**Handler** (`TransportCacheSnapshotAction.nodeOperation`):

```java
protected CacheSnapshotResponse nodeOperation(CacheSnapshotRequest request) {
    if (snapshotService == null) {
        throw new IllegalStateException(
            "cache snapshot is not enabled on this node " +
            "(set stateless.cache_snapshot.enabled = true)");
    }
    String snapshotId = snapshotService.snapshot(cacheService);
    return new CacheSnapshotResponse(snapshotId);
}
```

### 7d. REST handler — `RestCacheSnapshotAction`

```
POST /_stateless/cache/snapshot?node_id={node_id}
```

`node_id` is required — the source node must be specified explicitly. The REST handler
returns `400 Bad Request` if the parameter is absent.

Response body:
```json
{
  "snapshot_id": "snap-0a1b2c3d"
}
```

The action is registered in the stateless plugin's `getRestHandlers()` list and follows
the `Rest*Action` naming convention — `RestCacheSnapshotAction`.

---

## Step 8 — Skip pre-warming for regions already in the cache

Three services warm the cache proactively after a shard is assigned:

- `StatelessOnlinePrewarmingService` — warms region 0 (and optionally region 1) of every
  segment file on the hot query path.
- `SearchCommitPrefetcher` — prefetches commit data in the background when new commits
  arrive.
- `SharedBlobCacheWarmingService` — broader warming on shard recovery.

All three ultimately call `maybeFetchRange` / `fetchRange`, which internally calls
`CacheFileRegion.populate()`. `populate()` calls `SparseFileTracker.waitForRange()`: if
the requested range is already fully complete the call is a no-op — the listener fires
immediately and no blob store I/O occurs.

However, the overhead is not zero: each call still dispatches a task to a
`ThrottledTaskRunner`, acquires the `SparseFileTracker` lock, runs a `RefCountingListener`
chain, and touches the LFU freq list. On a restored node with a warm cache, every one of
the hundreds of pre-warm calls per shard is a fast no-op, but the cumulative scheduling
noise consumes thread-pool slots and delays other work during the burst immediately after
restart.

### 8a. Add `isRangeFullyCached()` to `SharedBlobCacheService`

```java
/**
 * Returns true if the given absolute-byte-range within the given region is already
 * fully present in the cache. This is a fast read-only check for use in pre-warming
 * paths to skip task scheduling when data is already warm.
 *
 * @param cacheKey  the logical key
 * @param region    the region index
 * @param range     the byte range in absolute file coordinates
 */
public boolean isRangeFullyCached(KeyType cacheKey, int region, ByteRange range) {
    var entry = cache.getIfPresent(cacheKey, region);
    if (entry == null || entry.chunk().isEvicted()) return false;
    long regionStart = getRegionStart(region);
    ByteRange regionRelative = ByteRange.of(range.start() - regionStart, range.end() - regionStart);
    return entry.chunk().tracker.getAbsentBytesWithin(regionRelative) == 0;
}
```

`cache` is the private `LFUCache` inner class; `getIfPresent` is an internal method used
here directly within `SharedBlobCacheService`. No additional access modifier changes
are needed.

### 8b. Guard `StatelessOnlinePrewarmingService`

In the inner loop over regions (currently line 188), add before calling
`cacheService.maybeFetchRange`:

```java
if (cacheService.isRangeFullyCached(cacheKey, i, range)) {
    logger.trace("online prewarming skipped for key [{}] region [{}]: already cached", cacheKey, i);
    continue;
}
```

This skips the `ThrottledTaskRunner.enqueueTask` call entirely for warm regions.

### 8c. Guard `SearchCommitPrefetcher`

The same check applies wherever `fetchRange` / `maybeFetchRange` is called per region.
Add an `isRangeFullyCached` guard before each dispatch point using the same pattern.

### 8d. Guard `SharedBlobCacheWarmingService`

`SharedBlobCacheWarmingService` also loops over regions and calls `fetchRange`. Add the
same guard at each dispatch site.

### Why not rely solely on the existing short-circuit inside `populate()`?

`populate()` short-circuits without I/O, but it still acquires the `SparseFileTracker`
lock, runs the listener chain, and uses a thread-pool slot via `ThrottledTaskRunner`. On
a freshly restored node, a single shard may trigger hundreds of pre-warm calls in a short
burst. Skipping the dispatch for already-warm regions avoids this burst of no-op overhead
entirely and leaves thread-pool capacity for genuine warming work on regions that are not
yet in the cache.

---

## Step 9 — Allocation preference via `CacheRestoredAllocationDecider`

`StatelessExistingShardsAllocator.allocateUnassigned` is a deliberate no-op in stateless.
Numeric ranking is controlled by `StatelessBalancingWeightsFactory`, which replaces
`searchWeightFunction` at construction time with `CacheRestoreAwareWeightFunction` — a
subclass whose `calculateNodeWeightWithIndex` checks `ModelNode.getRoutingNode().node()
.getAttributes()` and subtracts a constant boost for attributed nodes. This is the shared
instance used by `NodeSorter` in `decideMove`, so all relocation ranking picks up the boost.

For audit purposes, `AllocationDecider.canAllocate(ShardRouting shard, RoutingNode node,
RoutingAllocation allocation)` provides shard context. It is called for every
(shard, candidate-node) pair during exclusion-triggered relocation, exposes
`shard.currentNodeId()`, and can emit a labelled `Decision.YES` that appears in
`GET /_cluster/allocation/explain`.

### 9a. `CacheRestoredAllocationDecider` (new)

```java
public class CacheRestoredAllocationDecider extends AllocationDecider {

    static final String NAME = "cache_restored";

    @Override
    public Decision canAllocate(ShardRouting shard,
                                RoutingNode node,
                                RoutingAllocation allocation) {
        String restoredFrom =
            node.node().getAttributes().get("es_cache_restored_from_node");
        if (restoredFrom == null) {
            return Decision.YES;
        }
        if (restoredFrom.equals(shard.currentNodeId())) {
            // This node's cache was restored from the shard's current home —
            // explicitly signal that it is the preferred destination.
            return allocation.decision(Decision.YES, NAME,
                "node has warm cache restored from source node [%s]", restoredFrom);
        }
        return Decision.YES;   // no preference; still allowed as fallback
    }
}
```

Both the matching and non-matching cases return `Decision.YES`, so the decider never
hard-blocks fallback placement. The labelled `Decision.YES` for the matching case provides
an audit trail in `GET /_cluster/allocation/explain`. Preference ranking is handled by
`StatelessBalancingWeightsFactory`.

### 9b. `StatelessBalancingWeightsFactory` — concrete weight boost

`StatelessBalancingWeightsFactory.create()` returns a `StatelessBalancingWeights` object.
`StatelessBalancingWeights` constructs two `WeightFunction` instances
(`searchWeightFunction`, `indexingWeightFunction`) that are passed directly to
`NodeSorter` in `createNodeSorters`. **`NodeSorter` is what `decideMove` uses to rank
relocation targets** — it is built with these shared instances, not with per-node results
from `weightFunctionForNode`. `weightFunctionForNode` is only called for metrics
(node weight stats collection); overriding it has no effect on relocation ranking.

The correct hook is to replace `searchWeightFunction` at construction time with a
`CacheRestoreAwareWeightFunction` subclass. That same instance is used by both
`NodeSorter` (relocation ranking) and `weightFunctionForShard` (shard-role weight
queries). The subclass overrides `calculateNodeWeightWithIndex`, which receives a
`ModelNode` with access to `getRoutingNode().node().getAttributes()`. The subclass is
constructed with the **same balance factors** as the base `searchWeightFunction`, so the
package-private `minWeightDelta()` (inherited unchanged from `WeightFunction`) computes
correctly for rebalance threshold checks — no separate override is needed.

```java
// StatelessBalancingWeightsFactory.java — new private static inner class
/**
 * WeightFunction subclass that applies a constant boost (negative delta) to nodes
 * whose attribute indicates they are a cache-restored replacement. Used as the
 * shared searchWeightFunction so that NodeSorter and weightFunctionForShard both
 * see the boost. calculateNodeWeightWithIndex checks node attributes on each call.
 */
private static final class CacheRestoreAwareWeightFunction extends WeightFunction {

    static final float CACHE_RESTORED_BOOST = 10.0f;

    CacheRestoreAwareWeightFunction(float shardBalance, float indexBalance,
                                    float writeLoadBalance, float diskUsageBalance) {
        // Same factors as the base searchWeightFunction — thetas are identical,
        // so minWeightDelta() (package-private, inherited) returns correct values.
        super(shardBalance, indexBalance, writeLoadBalance, diskUsageBalance);
    }

    @Override
    public float calculateNodeWeightWithIndex(
            BalancedShardsAllocator.Balancer balancer,
            BalancedShardsAllocator.ModelNode node,
            BalancedShardsAllocator.ProjectIndex index) {
        float base = super.calculateNodeWeightWithIndex(balancer, node, index);
        if (node.getRoutingNode().node().getAttributes()
                .containsKey("es_cache_restored_from_node")) {
            return base - CACHE_RESTORED_BOOST;
        }
        return base;
    }
}

// StatelessBalancingWeights constructor — replace searchWeightFunction instantiation:
this.searchWeightFunction = new CacheRestoreAwareWeightFunction(
    searchTierShardBalanceFactor,
    indexBalanceFactor,
    searchTierWriteLoadBalanceFactor,
    diskUsageBalanceFactor
);
// indexingWeightFunction unchanged (index nodes are never replacement targets)
```

`weightFunctionForNode` is **not** overridden — the existing implementation returns
`searchWeightFunction` / `indexingWeightFunction` per node role, which is correct and
now automatically uses the boost-aware instance for search nodes.

**Tradeoff acknowledged:** the blanket boost affects all shard placements on the
replacement node, not only shards that were on `sourceNodeId`. If other shards happen
to be rebalancing concurrently, some may also be steered toward the replacement node.
This is acceptable because:
- The attribute is short-lived (present only during the replacement window, absent after
  the next restart).
- The replacement node is sized identically to the source, so it can absorb the same
  shard set.
- At most one replacement node per source carries the attribute at any given time (the
  operator performs one clone at a time).

If stricter per-shard routing is required, a follow-on iteration can extend
`CacheRestoredAllocationDecider` to return `Decision.NO` for shards not from
`sourceNodeId`, but this is not needed for the initial implementation.

### 9c. Triggering relocation while the source is live

Having a weight preference does not by itself cause the master to relocate shards away
from a healthy source node. The operator must explicitly decommission the source to
trigger relocation:

```
PUT /_cluster/settings
{
  "transient": {
    "cluster.routing.allocation.exclude._id": "<sourceNodeId>"
  }
}
```

This causes all shards on the source to become candidates for relocation. The
`DesiredBalanceShardsAllocator` recomputes the desired balance; the weight boost directs
those shards to the replacement node. After all shards have relocated, the operator
removes the exclusion and terminates the source node.

### Attribute lifecycle

The `es_cache_restored_from_node` attribute persists for the lifetime of the node process.
It is absent from the next restart (the replacement starts fresh without a snapshot file)
and needs no explicit cleanup.

### Source node draining — complete sequence

The orchestration layer is responsible for:

1. Call `POST /_stateless/cache/snapshot?node_id=X` → receive `snapshotId`
2. Provision replacement node from `snapshotId`
3. Wait for replacement node to join — observe `es_cache_restored_from_node` in `GET /_nodes`
4. Decommission source: `PUT /_cluster/settings {"transient": {"cluster.routing.allocation.exclude._id": "<sourceNodeId>"}}`
5. Wait for all shards to relocate from source to replacement — observe routing table
6. Clear exclusion: `PUT /_cluster/settings {"transient": {"cluster.routing.allocation.exclude._id": null}}`
7. Terminate source node

If the source node leaves before step 4, shards are allocated normally to other nodes
(cold misses, but correct behaviour).

---

## Correctness guarantees

| Invariant | How it is maintained |
|---|---|
| Metadata describes only durable data | `sync_file_range(WAIT_AFTER)` per slot completes before `writeSnapshotFile` is called; bytes are on the volume before the metadata file is written |
| Metadata file is atomic | Written to `.tmp`, fsynced, then atomically renamed — never partially visible |
| Metadata and data are consistent | `freeze()` blocks all mutation and search-I/O entry points (including `CacheFile.populate`/`populateAndRead` reached by `CacheFileReader`) and spin-waits until `activeGapFills == 0`; the counter is decremented in each gap's async completion listener (after bytes are written to the file and the `SparseFileTracker` range is marked done), so `activeGapFills == 0` means all in-flight blob-store writes have landed; `sync_file_range` and `getCompletedRanges` therefore see a stable, consistent view |
| Truncation detected on read | A truncated or malformed JSON document fails XContent parsing and discards the file — no partial restore |
| Cloud snapshot captures both | The cloud snapshot is initiated after both the data flush and the metadata fsync are complete; the snapshot includes the metadata file and all flushed data |
| Safe on cloud snapshot volume | The snapshot is taken after `sync_file_range(WAIT_AFTER)` acknowledges all bytes have reached the block device; cloud network volumes do not have a volatile device cache between the OS and persistent storage |
| Stale metadata never persists if snapshot fails | If `createSnapshot()` throws, the metadata file is deleted before re-throwing; a subsequent restart from the original volume starts cold rather than restoring with a snapshot ID that does not exist |
| Replacement node gets a fresh identity | The startup script reads `sourceNodeId` from the snapshot file header, compares it to `node.json`, and deletes `node.json` if they match; no operator input is required; the node generates a new ID and cannot collide with the still-running source node |
| Replacement node gets the correct shards | `CacheRestoreAwareWeightFunction` (injected as `searchWeightFunction`) gives the replacement node a blanket negative weight delta; operator decommissions source to trigger relocation; `NodeSorter` ranks replacement above other search nodes for all shard placements |
| Preference is advisory, not blocking | If the replacement node is unavailable, the allocator falls back to normal placement; shard allocation is never indefinitely stalled waiting for a specific node |

---

## Implementation edge cases

| Case | Note |
|---|---|
| `fillGapRunnable` never executed | If the `Runnable` returned by `fillGapRunnable` is submitted to an executor that rejects it before calling `run()`, the `countedListener` is never called and `activeGapFills` never decrements — `freeze()` then spin-waits forever. Moving the increment inside `run()` would avoid this leak but introduces a freeze race: `freeze()` acquires the write stamp after all entry-point read stamps are released, but a queued-but-not-yet-executing runnable has not yet incremented the counter, so `freeze()` sees zero and proceeds to `sync_file_range` while a gap fill is still pending. Factory-time increment is therefore required for correctness. The never-run case is accepted because internal ES thread pools do not reject; if this invariant needs hardening, use `AbstractRunnable.onRejection` to decrement explicitly. |
| Freeze spin-wait timeout | The `while (activeGapFills.get() > 0) { Thread.onSpinWait(); }` loop has no timeout. If a blob-store I/O hangs (e.g., S3 request stuck), `freeze()` spins indefinitely on a CPU core. Consider a bounded wait with an interrupt or logged warning after a threshold (e.g., 30 s). |
| `slotToRegion` on failed assign | `assignToSlot` can evict an existing entry and then fail to assign the new one (e.g., evicted-during-allocation path). Ensure `slotToRegion[slot]` is cleared in every exit path of `assignToSlot`, not only on success. |
| Snapshot file with zero entries | Handled in Step 7b: deletion is conditioned on `metadata.sourceNodeId() != null`, so a valid file with no entries is still deleted after processing. |
| Cloud provider scope | Shipping three cloud provider implementations (AWS EBS, GCP PD, Azure Managed Disk) in the initial PR is a large slice. Consider shipping with one fully implemented provider and a `NoOpCloudVolumeSnapshotProvider` stub (returns a fixed token, logs a warning) for the others until they are needed. |

---

## Files changed / created

| File | Change |
|---|---|
| `libs/native/…/NativeAccess.java` (interface + Linux impl) | `syncFileRange(FileChannel, long offset, long nbytes, int flags)`; Linux impl calls `sync_file_range(2)` via Panama FFI; default impl falls back to `fc.force(false)` |
| `blob-cache/…/SharedBytes.java` | `IO.getSlotIndex()`, `SharedBytes.syncRange(int slot, int flags)`, `SYNC_FILE_RANGE_WRITE`, `SYNC_FILE_RANGE_WAIT_AFTER` constants |
| `blob-cache/…/SharedBlobCacheService.java` | `slotToRegion[]` array + wiring in `assignToSlot` and `closeInternal`; `activeGapFills` counter + centralized increment/decrement in `fillGapRunnable`; `getNumRegions()`, `getRegionSize()`, `getSlotRegion()`, `isRangeFullyCached()`, `getOccupiedSlots()`, `getCompletedRangesForSlot()`, `syncSlotRange()`, `freeze()`/`unfreeze()` (StampedLock + activeGapFills drain), read-stamp call sites at all mutation and search-I/O entry points (`fetchRegion`, `maybeFetchRegion`, `fetchRange`, `maybeFetchRange`, `CacheFile.populate`, `CacheFile.populateAndRead`, `maybeEvictLeastUsed`, `forceEvict`, `forceEvictAsync`, shard-close eviction), `CacheIndexEntry` record, `restoreOccupancy()`, `CacheFileRegion` restore constructor |
| `stateless/…/StatelessSharedBlobCacheService.java` | `STATELESS_CACHE_SNAPSHOT_ENABLED_SETTING`, flag-gated `CacheSnapshotService` construction and startup restore, `snapshotService` field |
| `stateless/…/cache/CacheSnapshotService.java` *(new)* | `snapshot()` sequence; `writeSnapshotFile()` using `XContentBuilder` (JSON); `readSnapshotFile()` using streaming `XContentParser`; `CloudVolumeSnapshotProvider` interface |
| `stateless/…/cache/CloudVolumeSnapshotProvider.java` *(new)* | Interface + per-cloud implementations (AWS EBS, GCP PD, Azure Managed Disk) |
| `stateless/…/action/CacheSnapshotRequest.java` *(new)* | Transport request |
| `stateless/…/action/CacheSnapshotResponse.java` *(new)* | Transport response (`snapshotId`) |
| `stateless/…/action/TransportCacheSnapshotAction.java` *(new)* | Node-level transport action; invokes `CacheSnapshotService.snapshot()` |
| `stateless/…/action/RestCacheSnapshotAction.java` *(new)* | `POST /_stateless/cache/snapshot?node_id=…` REST handler (`node_id` required) |
| `stateless/…/cache/StatelessOnlinePrewarmingService.java` | `isRangeFullyCached` guard before `ThrottledTaskRunner` dispatch |
| `stateless/…/cache/SearchCommitPrefetcher.java` | `isRangeFullyCached` guard before each dispatch |
| `stateless/…/cache/SharedBlobCacheWarmingService.java` | `isRangeFullyCached` guard before each dispatch |
| `stateless/…/allocation/CacheRestoredAllocationDecider.java` *(new)* | `canAllocate(ShardRouting, RoutingNode, RoutingAllocation)` — labels matching (shard, node) pairs for audit; registered in stateless plugin's `createAllocationDeciders()` |
| `stateless/…/allocation/StatelessBalancingWeightsFactory.java` | Blanket weight reduction for nodes with `es_cache_restored_from_node` attribute set |

---

## Test plan

| Test class | What it covers |
|---|---|
| `SharedBytesTests` | `IO.getSlotIndex()` correctness |
| `SharedBlobCacheServiceTests` | `slotToRegion[]` populated on assign, cleared on evict; `freeze()` blocks all entry points and drains in-flight gap fills before returning; `activeGapFills` counter incremented/decremented correctly; `isRangeFullyCached` returns true/false correctly; `getOccupiedSlots()` matches live slot state |
| `CacheSnapshotServiceTests` *(new)* | Round-trip: populate cache → `snapshot()` → verify snapshot file is valid JSON, entries match live state; missing snapshot file → `readSnapshotFile` returns empty; malformed JSON → discarded, starts cold; `version` mismatch → discarded; `num_regions` / `region_size` mismatch → discarded |
| `CacheSnapshotFreezeTests` *(new)* | Freeze blocks all mutation and search-I/O entry points (including `CacheFile.populate`); in-flight async gap fills complete before freeze returns (`activeGapFills` drains to zero); warm hits via `CacheFile.tryRead` proceed while freeze is pending; cache-miss reads via `CacheFile.populate` block until freeze releases; freeze releases on error (finally block); completed-range snapshot is stable after freeze |
| `CacheSnapshotFlushOrderingTests` *(new)* | Assert `sync_file_range(WRITE)` issued for all occupied slots before any `sync_file_range(WAIT_AFTER)`; assert `writeSnapshotFile` called only after all WAIT_AFTER calls return; intercept `NativeAccess.syncFileRange` to verify ordering |
| `CacheSnapshotRestoreTests` *(new)* | End-to-end: populate → snapshot → re-create service → verify `freeRegionCount` reduced, `getIfPresent` non-null, completed ranges match; snapshot file deleted after successful restore; no snapshot file → cold start |
| `TransportCacheSnapshotActionTests` *(new)* | Feature flag disabled → `IllegalStateException`; enabled → delegates to `CacheSnapshotService`; response contains `snapshotId`; cloud provider error propagated; metadata file deleted on cloud provider failure |
| `StatelessOnlinePrewarmingServiceTests` | Restored regions: `maybeFetchRange` never called (skip guard fires); non-restored regions: `maybeFetchRange` called as before |
| `CacheSnapshotNodeReplacementTests` *(new)* | Startup script logic: snapshot header parsed correctly; `node.json` with matching `sourceNodeId` is deleted; `node.json` with a different ID is left untouched; `es_cache_restored_from_node` attribute set from snapshot header value; attribute absent when no snapshot file is present |
| `CacheRestoredAllocationDeciderTests` *(new)* | Returns labelled `Decision.YES` for (shard, node) pair where `es_cache_restored_from_node == shard.currentNodeId()`; returns plain `Decision.YES` for non-matching nodes (fallback allowed); neutral when attribute absent |
| `StatelessBalancingWeightsFactoryTests` | `NodeSorter` built with `CacheRestoreAwareWeightFunction` places attributed node at head of sort order; `calculateNodeWeightWithIndex` returns lower value for attributed node; boost does not override hard NO decisions from other deciders |
| `SearchNodeCloningAllocationIT` *(new, integration)* | Full flow: source node cloned → replacement joins with `es_cache_restored_from_node` attribute → source excluded via `exclude._id` → all shards relocate to replacement (not to other search nodes); verify via routing table; clear exclusion; source terminated cleanly |
