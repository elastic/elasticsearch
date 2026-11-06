/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/// Maps a document id to the latest version of this document.
public final class LiveVersionMap implements ReferenceManager.RefreshListener, Accountable {

    /// A per-document lock that needs to be held while working with the map.
    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private final LiveVersionMapArchive archive;

    LiveVersionMap() {
        this(LiveVersionMapArchive.NOOP_ARCHIVE);
    }

    LiveVersionMap(LiveVersionMapArchive archive) {
        this.archive = archive;
    }

    public static final class VersionLookup {
        /// Tracks bytes used by current map, i.e. what is freed on refresh. For deletes, which are also added to tombstones,
        /// we only account for the CHM entry here, and account for BytesRef/VersionValue against the tombstones, since refresh will not
        /// clear tombstone version.
        final AtomicLong ramBytesUsed = new AtomicLong();

        private static final VersionLookup EMPTY = new VersionLookup(Collections.emptyMap());
        private final Map<BytesRef, VersionValue> map;

        /// Each version lookup has a notion of being unsafe which means that a version was skipped and not added to it.
        /// This is useful when using auto-generated document ids (e.g. storing metrics) because we know that there won't ever be
        /// duplicate documents and as such we don't really need versions.
        ///
        /// An unsafe version lookup can be converted to safe using [#enforceSafeAccess] and performing a refresh.
        /// In safe access mode all versions are recorded in the version lookup.
        /// However, if safe access is no longer required versions can be skipped from the map (see [#maybePutIndexUnderLock])
        /// and it will be marked unsafe again.
        ///
        /// Note how this is not volatile. This is because we are concerned with this value on a per-id basis.
        /// Per-id writes and reads take a per-id lock, write of `unsafe` happens before releasing a per-id lock,
        /// releasing a per-id lock happens before acquiring it during the next operation.
        /// So it is not possible _per id_ to see `unsafe` as false if any unsafe operations were performed which is what we need.
        ///
        /// An operation on _different_ id can set `unsafe` to `true` which may not be visible, but again,
        /// this won't impact correctness of any operations.
        private boolean unsafe;

        /// Minimum timestamp of delete operations applied to this map (see [#updateMinDeletedTimestamp]).
        /// This is used to check if a tombstone (in [#tombstones]) is referring to a version that was not yet applied in Lucene.
        /// Such a tombstone can't be deleted, since its version information is already removed from version lookups
        /// and Lucene still contains stale document.
        private final AtomicLong minDeleteTimestamp = new AtomicLong(Long.MAX_VALUE);

        // Modifies the map of this instance by merging with the given VersionLookup
        public void merge(VersionLookup versionLookup) {
            long existingEntriesSize = 0;
            for (var entry : versionLookup.map.entrySet()) {
                var existingValue = map.get(entry.getKey());
                existingEntriesSize += existingValue == null ? 0 : mapEntryBytesUsed(entry.getKey(), existingValue);
            }
            map.putAll(versionLookup.map);
            adjustRamUsage(versionLookup.ramBytesUsed() - existingEntriesSize);
            minDeleteTimestamp.accumulateAndGet(versionLookup.minDeleteTimestamp(), Math::min);
        }

        // Visible for testing
        VersionLookup(Map<BytesRef, VersionValue> map) {
            this.map = map;
        }

        public VersionValue get(BytesRef key) {
            return map.get(key);
        }

        VersionValue put(BytesRef key, VersionValue value) {
            long ramAccounting = mapEntryBytesUsed(key, value);
            VersionValue previousValue = map.put(key, value);
            ramAccounting += previousValue == null ? 0 : -mapEntryBytesUsed(key, previousValue);
            adjustRamUsage(ramAccounting);
            return previousValue;
        }

        public boolean isEmpty() {
            return map.isEmpty();
        }

        int size() {
            return map.size();
        }

        public boolean isUnsafe() {
            return unsafe;
        }

        void markAsUnsafe() {
            unsafe = true;
        }

        VersionValue remove(BytesRef uid) {
            VersionValue previousValue = map.remove(uid);
            if (previousValue != null) {
                adjustRamUsage(-mapEntryBytesUsed(uid, previousValue));
            }
            return previousValue;
        }

        public void updateMinDeletedTimestamp(DeleteVersionValue delete) {
            minDeleteTimestamp.accumulateAndGet(delete.time, Math::min);
        }

        public long minDeleteTimestamp() {
            return minDeleteTimestamp.get();
        }

        void adjustRamUsage(long value) {
            if (value != 0) {
                long v = ramBytesUsed.addAndGet(value);
                assert v >= 0 : "bytes=" + v;
            }
        }

        public long ramBytesUsed() {
            return ramBytesUsed.get();
        }

        public static long mapEntryBytesUsed(BytesRef key, VersionValue value) {
            return (BASE_BYTES_PER_BYTESREF + key.bytes.length) + (BASE_BYTES_PER_CHM_ENTRY + value.ramBytesUsed());
        }

        // Used only for testing
        Map<BytesRef, VersionValue> getMap() {
            return map;
        }
    }

    private static final class Maps {
        /// Records the most recent writes (a change to a particular document).
        final VersionLookup current;

        /// This lookup contains all versions that will be available using an internal searcher after the refresh.
        /// But _during_ a refresh they are still needed, and we read from both `current` and `old`.
        /// Once the refresh is complete `old` can be removed.
        final VersionLookup old;

        // this is not volatile since we don't need to maintain a happens before relationship across doc IDs so it's enough to
        // have the volatile read of the Maps reference to make it visible even across threads.
        boolean needsSafeAccess;
        final boolean previousMapsNeededSafeAccess;

        Maps(VersionLookup current, VersionLookup old, boolean previousMapsNeededSafeAccess) {
            this.current = current;
            this.old = old;
            this.previousMapsNeededSafeAccess = previousMapsNeededSafeAccess;
        }

        Maps() {
            this(new VersionLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency()), VersionLookup.EMPTY, false);
        }

        boolean isSafeAccessMode() {
            return needsSafeAccess || previousMapsNeededSafeAccess;
        }

        boolean shouldInheritSafeAccess() {
            final boolean mapHasNotSeenAnyOperations = current.isEmpty() && current.isUnsafe() == false;
            return needsSafeAccess
                // we haven't seen any ops and map before needed it so we maintain it
                || (mapHasNotSeenAnyOperations && previousMapsNeededSafeAccess);
        }

        /// Builds a new map for the refresh transition. This should be called in [beforeRefresh()].
        Maps buildTransitionMap() {
            return new Maps(
                new VersionLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.size())),
                current,
                shouldInheritSafeAccess()
            );
        }

        /// Similar to [invalidateOldMap] but used only on the `unsafeKeysMap` which is used for assertions.
        Maps invalidateOldMapForAssert() {
            return new Maps(current, VersionLookup.EMPTY, previousMapsNeededSafeAccess);
        }

        /// Builds a new map that invalidates the old map but maintains the current. This should be called in [#afterRefresh].
        Maps invalidateOldMap(LiveVersionMapArchive archive) {
            archive.afterRefresh(old);
            return new Maps(current, VersionLookup.EMPTY, previousMapsNeededSafeAccess);
        }

        void put(BytesRef uid, VersionValue version) {
            current.put(uid, version);
        }

        void remove(BytesRef uid, DeleteVersionValue deleted) {
            current.remove(uid);
            current.updateMinDeletedTimestamp(deleted);
            if (old != VersionLookup.EMPTY) {
                // we also need to remove it from the old map here to make sure we don't read this stale value while
                // we are in the middle of a refresh. Most of the time the old map is an empty map so we can skip it there.
                old.remove(uid);
            }
        }

        long getMinDeleteTimestamp() {
            return Math.min(current.minDeleteTimestamp.get(), old.minDeleteTimestamp.get());
        }

        long ramBytesUsed() {
            return current.ramBytesUsed.get() + old.ramBytesUsed.get();
        }
    }

    /// Tracks the version that deleted the document (a tombstone).
    /// Tombstones are stored separately because they need to be preserved even after a refresh.
    /// This is because it is possible to receive a put operation after a delete operation
    /// (e.g. when using external versions or receiving replicated operations out of order on a replica).
    /// In this case the put should not be successful and to do that we need to know the
    /// version of the delete operation to compare it to the version of the put.
    /// If we didn't preserve tombstones the following could happen:
    /// 1. Deletion is recorded only in `current`
    /// 2. A refresh happens and cleans up `current`
    /// 3. Now there is nothing in the version map and in Lucene and a put can succeed.
    ///
    /// See [pruneTombstones] for removal logic.
    private final Map<BytesRef, DeleteVersionValue> tombstones = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private volatile Maps maps = new Maps();
    /// A second map that only receives the updates that are not stored in the actual map (unsafe operations.)
    /// This map is only maintained if assertions are enabled.
    private volatile Maps unsafeKeysMap = new Maps();

    /// Bytes consumed for each BytesRef UID.
    /// In this base value, we account for the [BytesRef] object itself as well as the header of the byte[] array it holds,
    /// and some lost bytes due to object alignment.
    /// So consumers of this constant just have to add the length of the byte[] (assuming it is not shared between multiple instances).
    private static final long BASE_BYTES_PER_BYTESREF = RamUsageEstimator.shallowSizeOfInstance(BytesRef.class)
        // With an alignment size (-XX:ObjectAlignmentInBytes) of 8 (default), there could be between 0 and 7 lost bytes,
        // so we account for 3 lost bytes on average.
        + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + 3;

    /// Estimate of the footprint of a single entry in a `ConcurrentHashMap`.
    private static final long BASE_BYTES_PER_CHM_ENTRY;

    static {
        // We use `newConcurrentMapWithAggressiveConcurrency` in the implementation so use it for estimations too.
        Map<Integer, Integer> map = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        map.put(0, 0);
        long chmEntryShallowSize = RamUsageEstimator.shallowSizeOf(map.entrySet().iterator().next());
        // Assume a hash table load factor of 50%
        // We count two object references per entry, one for the actual entry itself and one for the free space
        // in the hash table that exists due to the load factor.
        BASE_BYTES_PER_CHM_ENTRY = chmEntryShallowSize + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }

    private final AtomicLong ramBytesUsedForTombstones = new AtomicLong();

    @Override
    public void beforeRefresh() throws IOException {
        // We are about to perform a refresh meaning that all document versions stored in the `current`
        // will be visible using an internal searcher.
        // We don't know what is the exact "cutoff" point of documents that will be included in the refresh
        // since we only see the "before" and "after" refresh state.
        // So we proactively move all `current` documents to `old` _before_ refresh so that we can be completely sure
        // that _all_ of them are included in the refresh.
        // This allows us to drop `old` after the refresh.
        //
        // Some of the versions that will be inserted right after this call will also be included in the refresh
        // but there is nothing incorrect about this.
        // These versions will be present in `current` and read from it.

        maps = maps.buildTransitionMap();
        assert (unsafeKeysMap = unsafeKeysMap.buildTransitionMap()) != null;
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // As explained in `beforeRefresh()` everything that is stored in `old` is now visible through the internal searcher
        // and we can drop `old`.
        // Even if `didRefresh` is false, which means Lucene did not actually open a new reader because it detected no changes,
        // it's possible `old` has some entries in it. This is because we create a new `current` lookup _before_ the refresh, and
        // so writes are recorded in `current` even though they are already refreshed.
        // If there are no more writes, during next refresh `current` moves to `old` but there is nothing to refresh.

        maps = maps.invalidateOldMap(archive);
        assert (unsafeKeysMap = unsafeKeysMap.invalidateOldMapForAssert()) != null;
    }

    /// Returns the live version (put or delete) for this uid.
    VersionValue getUnderLock(final BytesRef uid) {
        return getUnderLock(uid, maps);
    }

    private VersionValue getUnderLock(final BytesRef uid, Maps currentMaps) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        VersionValue value = currentMaps.current.get(uid);
        if (value != null) {
            return value;
        }

        value = currentMaps.old.get(uid);
        if (value != null) {
            return value;
        }

        // We first check the tombstones, then the archive, since the archive accumulates versions from the old map even beyond refresh.
        // We clean up `current` and `old` in `putDeleteUnderLock` so we can trust them above, but we don't clean up the archive
        // (it is needed for archive logic).
        // Therefore, tombstones have priority over archive or we could read a stale version from the archive.
        value = tombstones.get(uid);
        if (value != null) {
            return value;
        }

        return archive.get(uid);
    }

    VersionValue getVersionForAssert(final BytesRef uid) {
        VersionValue value = getUnderLock(uid, maps);
        if (value == null) {
            value = getUnderLock(uid, unsafeKeysMap);
        }
        return value;
    }

    boolean isUnsafe() {
        // Archive is only relevant for realtime gets in stateless and so for writes the map can be marked safe
        // after an internal refresh.
        return maps.current.isUnsafe() || maps.old.isUnsafe();
    }

    /// Returns if the map is unsafe specifically in the context of executing a realtime get.
    /// See `StatelessLiveVersionMap`, [InternalEngine#getVersionForRead], [org.elasticsearch.action.get.TransportGetFromTranslogAction].
    boolean isUnsafeForGets() {
        return maps.current.isUnsafe() || maps.old.isUnsafe() || archive.isUnsafe();
    }

    /// Returns the committed segment generation that contains every unsafe operation this map has no record of,
    /// or -1 if the live maps are unsafe (their operations are not committed yet) or no committed generation
    /// covers the archive's unsafe operations. See [LiveVersionMapArchive#safeGenerationForGets].
    long safeGenerationForGets(long lastCommittedGeneration) {
        Maps maps = this.maps;
        if (maps.current.isUnsafe() || maps.old.isUnsafe()) {
            return -1;
        }
        return archive.safeGenerationForGets(lastCommittedGeneration);
    }

    /// Signals that the map should record versions rather than skip them as an optimization.
    /// See [maybePutIndexUnderLock].
    void enforceSafeAccess() {
        maps.needsSafeAccess = true;
    }

    boolean isSafeAccessRequired() {
        return maps.isSafeAccessMode();
    }

    /// Adds this uid/version to the map iff the map needs safe access.
    /// In some modes of operations, there are no updates/deletes/realtime gets (e.g. storing metrics).
    /// To optimize such a workflow we allow to skip storing a version in the map.
    /// However, if at some point an operation that does need versions happens,
    /// it shouldn't trust the map.
    /// This is what the `unsafe` flag means and `markAsUnsafe` sets it.
    void maybePutIndexUnderLock(BytesRef uid, IndexVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        Maps maps = this.maps;
        if (maps.isSafeAccessMode()) {
            putIndexUnderLock(uid, version);
        } else {
            // Even though we don't store a record of the indexing operation (and mark as unsafe),
            // we should still remove any previous delete for this uuid.
            // Latest version is removed from `current` and `old` in `putDeleteUnderLock` and
            // if we didn't do that `getUnderLock` would read a stale entry from `tombstones`.
            // Note that this should not hurt performance because the tombstones map is small (or empty) when unsafe is relevant.
            removeTombstoneUnderLock(uid);
            maps.current.markAsUnsafe();
            assert putAssertionMap(uid, version);
        }
    }

    void putIndexUnderLock(BytesRef uid, IndexVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        maps.put(uid, version);
        removeTombstoneUnderLock(uid);
    }

    private boolean putAssertionMap(BytesRef uid, IndexVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        unsafeKeysMap.put(uid, version);
        return true;
    }

    void putDeleteUnderLock(BytesRef uid, DeleteVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        putTombstone(uid, version);
        maps.remove(uid, version);
    }

    private void putTombstone(BytesRef uid, DeleteVersionValue version) {
        long uidRamBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;
        final VersionValue prevTombstone = tombstones.put(uid, version);
        long ramBytes = (BASE_BYTES_PER_CHM_ENTRY + version.ramBytesUsed() + uidRamBytesUsed);
        // Deduct bytes used for the tombstone version we just removed or replaced.
        if (prevTombstone != null) {
            ramBytes -= (BASE_BYTES_PER_CHM_ENTRY + prevTombstone.ramBytesUsed() + uidRamBytesUsed);
        }
        if (ramBytes != 0) {
            long v = ramBytesUsedForTombstones.addAndGet(ramBytes);
            assert v >= 0 : "bytes=" + v;
        }
    }

    void removeTombstoneUnderLock(BytesRef uid) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        long uidRamBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;
        final VersionValue prev = tombstones.remove(uid);
        if (prev != null) {
            assert prev.isDelete();
            long v = ramBytesUsedForTombstones.addAndGet(-(BASE_BYTES_PER_CHM_ENTRY + prev.ramBytesUsed() + uidRamBytesUsed));
            assert v >= 0 : "bytes=" + v;
        }
    }

    private boolean canRemoveTombstone(long maxTimestampToPrune, long maxSeqNoToPrune, DeleteVersionValue versionValue) {
        // check if the value is old enough and safe to be removed
        final boolean isTooOld = versionValue.time < maxTimestampToPrune;
        final boolean isSafeToPrune = versionValue.seqNo <= maxSeqNoToPrune;
        // Version value can't be removed if it was executed while the current map was active, meaning that it is not present in Lucene.
        final boolean isNotTrackedByCurrentMaps = versionValue.time < maps.getMinDeleteTimestamp();
        final boolean isNotTrackedByArchive = versionValue.time < archive.getMinDeleteTimestamp();
        return isTooOld && isSafeToPrune && isNotTrackedByCurrentMaps && isNotTrackedByArchive;
    }

    /// Try to prune tombstones whose timestamp is less than `maxTimestampToPrune` and seqNo is at most the maxSeqNoToPrune.
    void pruneTombstones(long maxTimestampToPrune, long maxSeqNoToPrune) {
        for (Map.Entry<BytesRef, DeleteVersionValue> entry : tombstones.entrySet()) {
            // Perform a check before acquiring the lock. This way we don't need to acquire the lock for tombstones that are not
            // prune-able.
            // It is possible that the entry changes in the meantime. However, if we can't prune the current tombstone
            // then we won't be able to prune the changed one as well since it must be newer than this one.
            if (canRemoveTombstone(maxTimestampToPrune, maxSeqNoToPrune, entry.getValue())) {
                final BytesRef uid = entry.getKey();
                try (Releasable lock = keyedLock.tryAcquire(uid)) {
                    // Tombstone pruning is best-effort so we use `tryAcquire` to not disrupt currently ongoing operations.
                    //
                    // This method is also called from refresh via `InternalEngine#maybePruneDeletes()`.
                    // During a realtime get or update, if the map is unsafe, a refresh is performed
                    // (e.g. in `InternalEngine#getVersionForRead`) while holding a document `uid` lock
                    // and synchronized on `versionMap`.
                    // So it is possible that:
                    // 1. thread A is performing a refresh holding document `uid1` lock and `versionMap` lock
                    // 2. thread B performs another operation, acquires document `uid2` lock, sees that the map is unsafe,
                    // and waits for `versionMap` lock
                    // 3. during a refresh initiated by thread A, `pruneTombstones` is called
                    // 4. this loop tries to acquire document `uid2` lock
                    // 5. deadlock
                    // See #28714.
                    if (lock != null) {
                        // Must re-get it here, vs using entry.getValue(), in case the uid was changed/deleted since we pulled the iterator.
                        final DeleteVersionValue versionValue = tombstones.get(uid);
                        if (versionValue != null) {
                            if (canRemoveTombstone(maxTimestampToPrune, maxSeqNoToPrune, versionValue)) {
                                removeTombstoneUnderLock(uid);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Called when this shard is closed.
    synchronized void clear() {
        maps = new Maps();
        tombstones.clear();
        // NOTE: we can't zero this here, because a refresh thread could be calling InternalEngine.pruneDeletedTombstones at the same time,
        // and this will lead to an assert trip. Presumably it's fine if our ramBytesUsedForTombstones is non-zero after clear since the
        // shard is being closed.
        // ramBytesUsedForTombstones.set(0);
    }

    @Override
    public long ramBytesUsed() {
        return maps.ramBytesUsed() + ramBytesUsedForTombstones.get() + ramBytesUsedForArchive();
    }

    /**
     * Returns how much RAM is used by refresh. This is the RAM usage of the current and old version maps, and the RAM usage of the
     * archive, if any.
     */
    long ramBytesUsedForRefresh() {
        return maps.ramBytesUsed() + archive.getRamBytesUsed();
    }

    /**
     * Returns how much RAM could be reclaimed from the version map.
     * <p>
     * In stateful, this is the RAM usage of the current version map, and could be reclaimed by refreshing. It doesn't include tombstones
     * since they don't get cleared on refresh, nor the old version map that is being reclaimed.
     * <p>
     * In stateless, this is the RAM usage of current and old version map plus the RAM usage of the parts of the archive that require
     * a new unpromotable refresh. To reclaim all three components we need to refresh AND flush.
     */
    long reclaimableRefreshRamBytes() {
        return archive == LiveVersionMapArchive.NOOP_ARCHIVE
            ? maps.current.ramBytesUsed.get()
            : maps.ramBytesUsed() + archive.getReclaimableRamBytes();
    }

    /**
     * Returns how much RAM would be freed up by cleaning out the LiveVersionMapArchive.
     */
    long ramBytesUsedForArchive() {
        return archive.getRamBytesUsed();
    }

    /**
     * Returns how much RAM is current being freed up by refreshing. In Stateful, this is the RAM usage of the previous version map
     * that needs to stay around until operations are safely recorded in the Lucene index. In Stateless, this is the RAM usage of a
     * fraction of the Archive entries that are kept around until an ongoing unpromotable refresh is finished.
     */
    long getRefreshingBytes() {
        return archive == LiveVersionMapArchive.NOOP_ARCHIVE ? maps.old.ramBytesUsed.get() : archive.getRefreshingRamBytes();
    }

    /**
     * Returns the current internal versions as a point in time snapshot
     */
    Map<BytesRef, VersionValue> getAllCurrent() {
        return maps.current.map;
    }

    /** Iterates over all deleted versions, including new ones (not yet exposed via reader) and old ones
     *  (exposed via reader but not yet GC'd). */
    Map<BytesRef, DeleteVersionValue> getAllTombstones() {
        return tombstones;
    }

    /**
     * Acquires a releasable lock for the given uId. All *UnderLock methods require
     * this lock to be held by the caller otherwise the visibility guarantees of this version
     * map are broken. We assert on this lock to be held when calling these methods.
     *
     * @see KeyedLock
     */
    Releasable acquireLock(BytesRef uid) {
        return keyedLock.acquire(uid);
    }

    /**
     * Try-acquire variant that rejects re-entry by the current thread. Used by batch indexing to ensure
     * duplicate UIDs cannot share a sub-batch (which would let later operations plan against stale
     * version-map state read at the start of the sub-batch).
     */
    Releasable tryAcquireLock(BytesRef uid) {
        return keyedLock.tryAcquireNoReentrancy(uid);
    }

    boolean assertKeyedLockHeldByCurrentThread(BytesRef uid) {
        assert keyedLock.isHeldByCurrentThread(uid) : "Thread [" + Thread.currentThread().getName() + "], uid [" + uid.utf8ToString() + "]";
        return true;
    }

    // visible for testing purposes only
    LiveVersionMapArchive getArchive() {
        return archive;
    }
}
