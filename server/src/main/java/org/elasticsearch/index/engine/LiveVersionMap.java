/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

/** Maps _uid value to its version information. */
public final class LiveVersionMap implements ReferenceManager.RefreshListener, Accountable {

    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private final LiveVersionMapArchive archive;

    LiveVersionMap() {
        this(LiveVersionMapArchive.NOOP_ARCHIVE);
    }

    LiveVersionMap(LiveVersionMapArchive archive) {
        this.archive = archive;
    }

    public static final class VersionLookup {

        /** Tracks bytes used by current map, i.e. what is freed on refresh. For deletes, which are also added to tombstones,
         *  we only account for the CHM entry here, and account for BytesRef/VersionValue against the tombstones, since refresh would not
         *  clear this RAM. */
        final AtomicLong ramBytesUsed = new AtomicLong();

        private static final VersionLookup EMPTY = new VersionLookup(Collections.emptyMap());
        private final Map<BytesRef, VersionValue> map;

        // each version map has a notion of safe / unsafe which allows us to apply certain optimization in the auto-generated ID usecase
        // where we know that documents can't have any duplicates so we can skip the version map entirely. This reduces
        // the memory pressure significantly for this use-case where we often get a massive amount of small document (metrics).
        // if the version map is in safeAccess mode we track all version in the version map. yet if a document comes in that needs
        // safe access but we are not in this mode we force a refresh and make the map as safe access required. All subsequent ops will
        // respect that and fill the version map. The nice part here is that we are only really requiring this for a single ID and since
        // we hold the ID lock in the engine while we do all this it's safe to do it globally unlocked.
        // NOTE: these values can both be non-volatile since it's ok to read a stale value per doc ID. We serialize changes in the engine
        // that will prevent concurrent updates to the same document ID and therefore we can rely on the happens-before guanratee of the
        // map reference itself.
        private boolean unsafe;

        // minimum timestamp of delete operations that were made while this map was active. this is used to make sure they are kept in
        // the tombstone
        private final AtomicLong minDeleteTimestamp = new AtomicLong(Long.MAX_VALUE);

        // Modifies the map of this instance by merging with the given VersionLookup
        public void merge(VersionLookup versionLookup) {
            map.putAll(versionLookup.map);
            minDeleteTimestamp.accumulateAndGet(versionLookup.minDeleteTimestamp(), Math::min);
        }

        private VersionLookup(Map<BytesRef, VersionValue> map) {
            this.map = map;
        }

        public VersionValue get(BytesRef key) {
            return map.get(key);
        }

        VersionValue put(BytesRef key, VersionValue value) {
            return map.put(key, value);
        }

        public boolean isEmpty() {
            return map.isEmpty();
        }

        int size() {
            return map.size();
        }

        boolean isUnsafe() {
            return unsafe;
        }

        void markAsUnsafe() {
            unsafe = true;
        }

        public VersionValue remove(BytesRef uid) {
            return map.remove(uid);
        }

        public void updateMinDeletedTimestamp(DeleteVersionValue delete) {
            minDeleteTimestamp.accumulateAndGet(delete.time, Math::min);
        }

        public long minDeleteTimestamp() {
            return minDeleteTimestamp.get();
        }
    }

    private static final class Maps {

        // All writes (adds and deletes) go into here:
        final VersionLookup current;

        // Used while refresh is running, and to hold adds/deletes until refresh finishes. We read from both current and old on lookup:
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

        /**
         * Builds a new map for the refresh transition this should be called in beforeRefresh()
         */
        Maps buildTransitionMap() {
            return new Maps(
                new VersionLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.size())),
                current,
                shouldInheritSafeAccess()
            );
        }

        /**
         * similar to `invalidateOldMap` but used only for the `unsafeKeysMap` used for assertions
         */
        Maps invalidateOldMapForAssert() {
            return new Maps(current, VersionLookup.EMPTY, previousMapsNeededSafeAccess);
        }

        /**
         * builds a new map that invalidates the old map but maintains the current. This should be called in afterRefresh()
         */
        Maps invalidateOldMap(LiveVersionMapArchive archive) {
            archive.afterRefresh(old);
            return new Maps(current, VersionLookup.EMPTY, previousMapsNeededSafeAccess);
        }

        void put(BytesRef uid, VersionValue version) {
            long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;
            long ramAccounting = BASE_BYTES_PER_CHM_ENTRY + version.ramBytesUsed() + uidRAMBytesUsed;
            VersionValue previousValue = current.put(uid, version);
            ramAccounting += previousValue == null ? 0 : -(BASE_BYTES_PER_CHM_ENTRY + previousValue.ramBytesUsed() + uidRAMBytesUsed);
            adjustRam(ramAccounting);
        }

        void adjustRam(long value) {
            if (value != 0) {
                long v = current.ramBytesUsed.addAndGet(value);
                assert v >= 0 : "bytes=" + v;
            }
        }

        void remove(BytesRef uid, DeleteVersionValue deleted) {
            VersionValue previousValue = current.remove(uid);
            current.updateMinDeletedTimestamp(deleted);
            if (previousValue != null) {
                long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;
                adjustRam(-(BASE_BYTES_PER_CHM_ENTRY + previousValue.ramBytesUsed() + uidRAMBytesUsed));
            }
            if (old != VersionLookup.EMPTY) {
                // we also need to remove it from the old map here to make sure we don't read this stale value while
                // we are in the middle of a refresh. Most of the time the old map is an empty map so we can skip it there.
                old.remove(uid);
            }
        }

        long getMinDeleteTimestamp() {
            return Math.min(current.minDeleteTimestamp.get(), old.minDeleteTimestamp.get());
        }
    }

    // All deletes also go here, and delete "tombstones" are retained after refresh:
    private final Map<BytesRef, DeleteVersionValue> tombstones = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private volatile Maps maps = new Maps();
    // we maintain a second map that only receives the updates that we skip on the actual map (unsafe ops)
    // this map is only maintained if assertions are enabled
    private volatile Maps unsafeKeysMap = new Maps();

    /**
     * Bytes consumed for each BytesRef UID:
     * In this base value, we account for the {@link BytesRef} object itself as
     * well as the header of the byte[] array it holds, and some lost bytes due
     * to object alignment. So consumers of this constant just have to add the
     * length of the byte[] (assuming it is not shared between multiple
     * instances).
     */
    private static final long BASE_BYTES_PER_BYTESREF =
        // shallow memory usage of the BytesRef object
        RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) +
        // header of the byte[] array
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER +
            // with an alignment size (-XX:ObjectAlignmentInBytes) of 8 (default),
            // there could be between 0 and 7 lost bytes, so we account for 3
            // lost bytes on average
            3;

    /**
     * Bytes used by having CHM point to a key/value.
     */
    private static final long BASE_BYTES_PER_CHM_ENTRY;

    static {
        // use the same impl as the Maps does
        Map<Integer, Integer> map = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
        map.put(0, 0);
        long chmEntryShallowSize = RamUsageEstimator.shallowSizeOf(map.entrySet().iterator().next());
        // assume a load factor of 50%
        // for each entry, we need two object refs, one for the entry itself
        // and one for the free space that is due to the fact hash tables can
        // not be fully loaded
        BASE_BYTES_PER_CHM_ENTRY = chmEntryShallowSize + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }

    /**
     * Tracks bytes used by tombstones (deletes)
     */
    private final AtomicLong ramBytesUsedTombstones = new AtomicLong();

    @Override
    public void beforeRefresh() throws IOException {
        // Start sending all updates after this point to the new
        // map. While reopen is running, any lookup will first
        // try this new map, then fallback to old, then to the
        // current searcher:
        maps = maps.buildTransitionMap();
        assert (unsafeKeysMap = unsafeKeysMap.buildTransitionMap()) != null;
        // This is not 100% correct, since concurrent indexing ops can change these counters in between our execution of the previous
        // line and this one, but that should be minor, and the error won't accumulate over time:
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // We can now drop old because these operations are now visible via the newly opened searcher. Even if didRefresh is false, which
        // means Lucene did not actually open a new reader because it detected no changes, it's possible old has some entries in it, which
        // is fine: it means they were actually already included in the previously opened reader, so we can still safely drop them in that
        // case. This is because we assign new maps (in beforeRefresh) slightly before Lucene actually flushes any segments for the
        // reopen, and so any concurrent indexing requests can still sneak in a few additions to that current map that are in fact
        // reflected in the previous reader. We don't touch tombstones here: they expire on their own index.gc_deletes timeframe:

        maps = maps.invalidateOldMap(archive);
        assert (unsafeKeysMap = unsafeKeysMap.invalidateOldMapForAssert()) != null;

    }

    /**
     * Returns the live version (add or delete) for this uid.
     */
    VersionValue getUnderLock(final BytesRef uid) {
        return getUnderLock(uid, maps);
    }

    private VersionValue getUnderLock(final BytesRef uid, Maps currentMaps) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        // First try to get the "live" value:
        VersionValue value = currentMaps.current.get(uid);
        if (value != null) {
            return value;
        }

        value = currentMaps.old.get(uid);
        if (value != null) {
            return value;
        }

        // We first check the tombstone then the archive since the archive accumulates ids from the old map, and we
        // makes sure in `putDeleteUnderLock` the old map does not hold an entry that is in tombstone, archive also wouldn't have them.
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
        return maps.current.isUnsafe() || maps.old.isUnsafe();
    }

    void enforceSafeAccess() {
        maps.needsSafeAccess = true;
    }

    boolean isSafeAccessRequired() {
        return maps.isSafeAccessMode();
    }

    /**
     * Adds this uid/version to the pending adds map iff the map needs safe access.
     */
    void maybePutIndexUnderLock(BytesRef uid, IndexVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        Maps maps = this.maps;
        if (maps.isSafeAccessMode()) {
            putIndexUnderLock(uid, version);
        } else {
            // Even though we don't store a record of the indexing operation (and mark as unsafe),
            // we should still remove any previous delete for this uuid (avoid accidental accesses).
            // Not this should not hurt performance because the tombstone is small (or empty) when unsafe is relevant.
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
        long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;
        // Also enroll the delete into tombstones, and account for its RAM too:
        final VersionValue prevTombstone = tombstones.put(uid, version);
        long accountRam = (BASE_BYTES_PER_CHM_ENTRY + version.ramBytesUsed() + uidRAMBytesUsed);
        // Deduct tombstones bytes used for the version we just removed or replaced:
        if (prevTombstone != null) {
            accountRam -= (BASE_BYTES_PER_CHM_ENTRY + prevTombstone.ramBytesUsed() + uidRAMBytesUsed);
        }
        if (accountRam != 0) {
            long v = ramBytesUsedTombstones.addAndGet(accountRam);
            assert v >= 0 : "bytes=" + v;
        }
    }

    /**
     * Removes this uid from the pending deletes map.
     */
    void removeTombstoneUnderLock(BytesRef uid) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;
        final VersionValue prev = tombstones.remove(uid);
        if (prev != null) {
            assert prev.isDelete();
            long v = ramBytesUsedTombstones.addAndGet(-(BASE_BYTES_PER_CHM_ENTRY + prev.ramBytesUsed() + uidRAMBytesUsed));
            assert v >= 0 : "bytes=" + v;
        }
    }

    private boolean canRemoveTombstone(long maxTimestampToPrune, long maxSeqNoToPrune, DeleteVersionValue versionValue) {
        // check if the value is old enough and safe to be removed
        final boolean isTooOld = versionValue.time < maxTimestampToPrune;
        final boolean isSafeToPrune = versionValue.seqNo <= maxSeqNoToPrune;
        // version value can't be removed it's
        // not yet flushed to lucene ie. it's part of this current maps object
        final boolean isNotTrackedByCurrentMaps = versionValue.time < maps.getMinDeleteTimestamp();
        final boolean isNotTrackedByArchive = versionValue.time < archive.getMinDeleteTimestamp();
        return isTooOld && isSafeToPrune && isNotTrackedByCurrentMaps & isNotTrackedByArchive;
    }

    /**
     * Try to prune tombstones whose timestamp is less than maxTimestampToPrune and seqno at most the maxSeqNoToPrune.
     */
    void pruneTombstones(long maxTimestampToPrune, long maxSeqNoToPrune) {
        for (Map.Entry<BytesRef, DeleteVersionValue> entry : tombstones.entrySet()) {
            // we do check before we actually lock the key - this way we don't need to acquire the lock for tombstones that are not
            // prune-able. If the tombstone changes concurrently we will re-read and step out below since if we can't collect it now w
            // we won't collect the tombstone below since it must be newer than this one.
            if (canRemoveTombstone(maxTimestampToPrune, maxSeqNoToPrune, entry.getValue())) {
                final BytesRef uid = entry.getKey();
                try (Releasable lock = keyedLock.tryAcquire(uid)) {
                    // we use tryAcquire here since this is a best effort and we try to be least disruptive
                    // this method is also called under lock in the engine under certain situations such that this can lead to deadlocks
                    // if we do use a blocking acquire. see #28714
                    if (lock != null) { // did we get the lock?
                        // Must re-get it here, vs using entry.getValue(), in case the uid was indexed/deleted since we pulled the iterator:
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

    /**
     * Called when this index is closed.
     */
    synchronized void clear() {
        maps = new Maps();
        tombstones.clear();
        // NOTE: we can't zero this here, because a refresh thread could be calling InternalEngine.pruneDeletedTombstones at the same time,
        // and this will lead to an assert trip. Presumably it's fine if our ramBytesUsedTombstones is non-zero after clear since the
        // index is being closed:
        // ramBytesUsedTombstones.set(0);
    }

    @Override
    public long ramBytesUsed() {
        return maps.current.ramBytesUsed.get() + maps.old.ramBytesUsed.get() + ramBytesUsedTombstones.get();
    }

    /**
     * Returns how much RAM would be freed up by refreshing. This is the RAM usage of the current version map. It doesn't include tombstones
     * since they don't get cleared on refresh, nor the old version map that is being reclaimed.
     */
    long ramBytesUsedForRefresh() {
        return maps.current.ramBytesUsed.get();
    }

    /**
     * Returns how much RAM is current being freed up by refreshing. This is the RAM usage of the previous version map that needs to stay
     * around until operations are safely recorded in the Lucene index.
     */
    long getRefreshingBytes() {
        return maps.old.ramBytesUsed.get();
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
     * Acquires a releaseable lock for the given uId. All *UnderLock methods require
     * this lock to be hold by the caller otherwise the visibility guarantees of this version
     * map are broken. We assert on this lock to be hold when calling these methods.
     * @see KeyedLock
     */
    Releasable acquireLock(BytesRef uid) {
        return keyedLock.acquire(uid);
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
