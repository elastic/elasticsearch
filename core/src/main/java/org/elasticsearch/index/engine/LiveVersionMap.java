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
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Maps _uid value to its version information. */
class LiveVersionMap implements ReferenceManager.RefreshListener, Accountable {

    private static class Maps {

        // All writes (adds and deletes) go into here:
        final Map<BytesRef,VersionValue> current;

        // Used while refresh is running, and to hold adds/deletes until refresh finishes.  We read from both current and old on lookup:
        final Map<BytesRef,VersionValue> old;

        Maps(Map<BytesRef,VersionValue> current, Map<BytesRef,VersionValue> old) {
           this.current = current;
           this.old = old;
        }

        Maps() {
            this(ConcurrentCollections.<BytesRef,VersionValue>newConcurrentMapWithAggressiveConcurrency(),
                 Collections.emptyMap());
        }
    }

    // All deletes also go here, and delete "tombstones" are retained after refresh:
    private final Map<BytesRef,DeleteVersionValue> tombstones = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private volatile Maps maps = new Maps();

    /** Bytes consumed for each BytesRef UID:
     * In this base value, we account for the {@link BytesRef} object itself as
     * well as the header of the byte[] array it holds, and some lost bytes due
     * to object alignment. So consumers of this constant just have to add the
     * length of the byte[] (assuming it is not shared between multiple
     * instances). */
    private static final long BASE_BYTES_PER_BYTESREF =
            // shallow memory usage of the BytesRef object
            RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) +
            // header of the byte[] array
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER +
            // with an alignment size (-XX:ObjectAlignmentInBytes) of 8 (default),
            // there could be between 0 and 7 lost bytes, so we account for 3
            // lost bytes on average
            3;

    /** Bytes used by having CHM point to a key/value. */
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

    /** Tracks bytes used by current map, i.e. what is freed on refresh. For deletes, which are also added to tombstones, we only account
     *  for the CHM entry here, and account for BytesRef/VersionValue against the tombstones, since refresh would not clear this RAM. */
    final AtomicLong ramBytesUsedCurrent = new AtomicLong();

    /** Tracks bytes used by tombstones (deletes) */
    final AtomicLong ramBytesUsedTombstones = new AtomicLong();

    @Override
    public void beforeRefresh() throws IOException {
        // Start sending all updates after this point to the new
        // map.  While reopen is running, any lookup will first
        // try this new map, then fallback to old, then to the
        // current searcher:
        maps = new Maps(ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency(maps.current.size()), maps.current);

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
        maps = new Maps(maps.current, Collections.emptyMap());
    }

    /** Returns the live version (add or delete) for this uid. */
    VersionValue getUnderLock(final BytesRef uid) {
        Maps currentMaps = maps;

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

    /** Adds this uid/version to the pending adds map. */
    void putUnderLock(BytesRef uid, VersionValue version) {
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
            prevTombstone = tombstones.put(uid, (DeleteVersionValue)version);

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
            assert v >= 0: "bytes=" + v;
        }
    }

    /** Removes this uid from the pending deletes map. */
    void removeTombstoneUnderLock(BytesRef uid) {

        long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;

        final VersionValue prev = tombstones.remove(uid);
        if (prev != null) {
            assert prev.isDelete();
            long v = ramBytesUsedTombstones.addAndGet(-(BASE_BYTES_PER_CHM_ENTRY + prev.ramBytesUsed() + uidRAMBytesUsed));
            assert v >= 0: "bytes=" + v;
        }
        final VersionValue curVersion = maps.current.get(uid);
        if (curVersion != null && curVersion.isDelete()) {
            // We now shift accounting of the BytesRef from tombstones to current, because a refresh would clear this RAM.  This should be
            // uncommon, because with the default refresh=1s and gc_deletes=60s, deletes should be cleared from current long before we drop
            // them from tombstones:
            ramBytesUsedCurrent.addAndGet(curVersion.ramBytesUsed() + uidRAMBytesUsed);
        }
    }

    /** Caller has a lock, so that this uid will not be concurrently added/deleted by another thread. */
    DeleteVersionValue getTombstoneUnderLock(BytesRef uid) {
        return tombstones.get(uid);
    }

    /** Iterates over all deleted versions, including new ones (not yet exposed via reader) and old ones (exposed via reader but not yet GC'd). */
    Iterable<Map.Entry<BytesRef, DeleteVersionValue>> getAllTombstones() {
        return tombstones.entrySet();
    }

    /** clears all tombstones ops */
    void clearTombstones() {
        tombstones.clear();
    }

    /** Called when this index is closed. */
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

    /** Returns how much RAM would be freed up by refreshing. This is {@link #ramBytesUsed} except does not include tombstones because they
     *  don't clear on refresh. */
    long ramBytesUsedForRefresh() {
        return ramBytesUsedCurrent.get();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        // TODO: useful to break down RAM usage here?
        return Collections.emptyList();
    }

    /** Returns the current internal versions as a point in time snapshot*/
    Map<BytesRef, VersionValue> getAllCurrent() {
        return maps.current;
    }}
