/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.KeyedLock;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Maps _uid value to its version information. */
final class LiveVersionMap implements ReferenceManager.RefreshListener, Accountable {

    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private static final class VersionLookup {

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

        private VersionLookup(Map<BytesRef, VersionValue> map) {
            this.map = map;
        }

        VersionValue get(BytesRef key) {
            return map.get(key);
        }

        VersionValue put(BytesRef key, VersionValue value) {
            return map.put(key, value);
        }

        boolean isEmpty() {
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
    }

    private static final class Maps {

        // All writes (adds and deletes) go into here:
        final VersionLookup current;

        // Used while refresh is running, and to hold adds/deletes until refresh finishes.  We read from both current and old on lookup:
        final VersionLookup old;

        // this is not volatile since we don't need to maintain a happens before relation ship across doc IDs so it's enough to
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
            return new Maps(new VersionLookup(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(current.size())),
                current, shouldInheritSafeAccess());
        }

        /**
         * builds a new map that invalidates the old map but maintains the current. This should be called in afterRefresh()
         */
        Maps invalidateOldMap() {
            return new Maps(current, VersionLookup.EMPTY, previousMapsNeededSafeAccess);
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
     * Tracks bytes used by current map, i.e. what is freed on refresh. For deletes, which are also added to tombstones, we only account
     * for the CHM entry here, and account for BytesRef/VersionValue against the tombstones, since refresh would not clear this RAM.
     */
    final AtomicLong ramBytesUsedCurrent = new AtomicLong();

    /**
     * Tracks bytes used by tombstones (deletes)
     */
    final AtomicLong ramBytesUsedTombstones = new AtomicLong();

    @Override
    public void beforeRefresh() throws IOException {
        // Start sending all updates after this point to the new
        // map.  While reopen is running, any lookup will first
        // try this new map, then fallback to old, then to the
        // current searcher:
        maps = maps.buildTransitionMap();
        assert (unsafeKeysMap = unsafeKeysMap.buildTransitionMap()) != null;
        // This is not 100% correct, since concurrent indexing ops can change these counters in between our execution of the previous
        // line and this one, but that should be minor, and the error won't accumulate over time:
        ramBytesUsedCurrent.set(0);
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // We can now drop old because these operations are now visible via the newly opened searcher.  Even if didRefresh is false, which
        // means Lucene did not actually open a new reader because it detected no changes, it's possible old has some entries in it, which
        // is fine: it means they were actually already included in the previously opened reader, so we can still safely drop them in that
        // case.  This is because we assign new maps (in beforeRefresh) slightly before Lucene actually flushes any segments for the
        // reopen, and so any concurrent indexing requests can still sneak in a few additions to that current map that are in fact reflected
        // in the previous reader.   We don't touch tombstones here: they expire on their own index.gc_deletes timeframe:

        maps = maps.invalidateOldMap();
        assert (unsafeKeysMap = unsafeKeysMap.invalidateOldMap()) != null;

    }

    /**
     * Returns the live version (add or delete) for this uid.
     */
    VersionValue getUnderLock(final BytesRef uid) {
        return getUnderLock(uid, maps);
    }

    private VersionValue getUnderLock(final BytesRef uid, Maps currentMaps) {
        assert keyedLock.isHeldByCurrentThread(uid);
        // First try to get the "live" value:
        VersionValue value = currentMaps.current.get(uid);
        if (value != null) {
            return value;
        }

        value = currentMaps.old.get(uid);
        if (value != null) {
            return value;
        }

        return tombstones.get(uid);
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
    void maybePutUnderLock(BytesRef uid, VersionValue version) {
        assert keyedLock.isHeldByCurrentThread(uid);
        Maps maps = this.maps;
        if (maps.isSafeAccessMode()) {
            putUnderLock(uid, version, maps);
        } else {
            maps.current.markAsUnsafe();
            assert putAssertionMap(uid, version);
        }
    }

    private boolean putAssertionMap(BytesRef uid, VersionValue version) {
        putUnderLock(uid, version, unsafeKeysMap);
        return true;
    }

    /**
     * Adds this uid/version to the pending adds map.
     */
    void putUnderLock(BytesRef uid, VersionValue version) {
        Maps maps = this.maps;
        putUnderLock(uid, version, maps);
    }

    /**
     * Adds this uid/version to the pending adds map.
     */
    private void putUnderLock(BytesRef uid, VersionValue version, Maps maps) {
        assert keyedLock.isHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;
        long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;
        final VersionValue prev = maps.current.put(uid, version);
        if (prev != null) {
            // Deduct RAM for the version we just replaced:
            long prevBytes = BASE_BYTES_PER_CHM_ENTRY;
            if (prev.isDelete() == false) {
                prevBytes += prev.ramBytesUsed() + uidRAMBytesUsed;
            }
            ramBytesUsedCurrent.addAndGet(-prevBytes);
        }

        // Add RAM for the new version:
        long newBytes = BASE_BYTES_PER_CHM_ENTRY;
        if (version.isDelete() == false) {
            newBytes += version.ramBytesUsed() + uidRAMBytesUsed;
        }
        ramBytesUsedCurrent.addAndGet(newBytes);

        final VersionValue prevTombstone;
        if (version.isDelete()) {
            // Also enroll the delete into tombstones, and account for its RAM too:
            prevTombstone = tombstones.put(uid, (DeleteVersionValue) version);

            // We initially account for BytesRef/VersionValue RAM for a delete against the tombstones, because this RAM will not be freed up
            // on refresh. Later, in removeTombstoneUnderLock, if we clear the tombstone entry but the delete remains in current, we shift
            // the accounting to current:
            ramBytesUsedTombstones.addAndGet(BASE_BYTES_PER_CHM_ENTRY + version.ramBytesUsed() + uidRAMBytesUsed);

            if (prevTombstone == null && prev != null && prev.isDelete()) {
                // If prev was a delete that had already been removed from tombstones, then current was already accounting for the
                // BytesRef/VersionValue RAM, so we now deduct that as well:
                ramBytesUsedCurrent.addAndGet(-(prev.ramBytesUsed() + uidRAMBytesUsed));
            }
        } else {
            // UID came back to life so we remove the tombstone:
            prevTombstone = tombstones.remove(uid);
        }

        // Deduct tombstones bytes used for the version we just removed or replaced:
        if (prevTombstone != null) {
            long v = ramBytesUsedTombstones.addAndGet(-(BASE_BYTES_PER_CHM_ENTRY + prevTombstone.ramBytesUsed() + uidRAMBytesUsed));
            assert v >= 0 : "bytes=" + v;
        }
    }

    /**
     * Removes this uid from the pending deletes map.
     */
    void removeTombstoneUnderLock(BytesRef uid) {
        assert keyedLock.isHeldByCurrentThread(uid);
        long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;

        final VersionValue prev = tombstones.remove(uid);
        if (prev != null) {
            assert prev.isDelete();
            long v = ramBytesUsedTombstones.addAndGet(-(BASE_BYTES_PER_CHM_ENTRY + prev.ramBytesUsed() + uidRAMBytesUsed));
            assert v >= 0 : "bytes=" + v;
        }
        final VersionValue curVersion = maps.current.get(uid);
        if (curVersion != null && curVersion.isDelete()) {
            // We now shift accounting of the BytesRef from tombstones to current, because a refresh would clear this RAM.  This should be
            // uncommon, because with the default refresh=1s and gc_deletes=60s, deletes should be cleared from current long before we drop
            // them from tombstones:
            ramBytesUsedCurrent.addAndGet(curVersion.ramBytesUsed() + uidRAMBytesUsed);
        }
    }

    /**
     * Caller has a lock, so that this uid will not be concurrently added/deleted by another thread.
     */
    DeleteVersionValue getTombstoneUnderLock(BytesRef uid) {
        assert keyedLock.isHeldByCurrentThread(uid);
        return tombstones.get(uid);
    }

    /**
     * Iterates over all deleted versions, including new ones (not yet exposed via reader) and old ones (exposed via reader but not yet GC'd).
     */
    Iterable<Map.Entry<BytesRef, DeleteVersionValue>> getAllTombstones() {
        return tombstones.entrySet();
    }

    /**
     * clears all tombstones ops
     */
    void clearTombstones() {
        tombstones.clear();
    }

    /**
     * Called when this index is closed.
     */
    synchronized void clear() {
        maps = new Maps();
        tombstones.clear();
        ramBytesUsedCurrent.set(0);

        // NOTE: we can't zero this here, because a refresh thread could be calling InternalEngine.pruneDeletedTombstones at the same time,
        // and this will lead to an assert trip.  Presumably it's fine if our ramBytesUsedTombstones is non-zero after clear since the index
        // is being closed:
        //ramBytesUsedTombstones.set(0);
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsedCurrent.get() + ramBytesUsedTombstones.get();
    }

    /**
     * Returns how much RAM would be freed up by refreshing. This is {@link #ramBytesUsed} except does not include tombstones because they
     * don't clear on refresh.
     */
    long ramBytesUsedForRefresh() {
        return ramBytesUsedCurrent.get();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        // TODO: useful to break down RAM usage here?
        return Collections.emptyList();
    }

    /**
     * Returns the current internal versions as a point in time snapshot
     */
    Map<BytesRef, VersionValue> getAllCurrent() {
        return maps.current.map;
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


    /**
     * Tries to acquires a releaseable lock for the given uId. If the lock can not be acquired this method returns <code>null</code>
     *
     * All *UnderLock methods require this lock to be hold by the caller otherwise the visibility guarantees of this version
     * map are broken. We assert on this lock to be hold when calling these methods.
     * @see KeyedLock
     */
    Releasable tryAcquireLock(BytesRef uid) {
        return keyedLock.tryAcquire(uid);
    }
}
