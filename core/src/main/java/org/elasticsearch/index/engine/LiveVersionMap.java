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
      
        public Maps(Map<BytesRef,VersionValue> current, Map<BytesRef,VersionValue> old) {
           this.current = current;
           this.old = old;
        }

        public Maps() {
            this(ConcurrentCollections.<BytesRef,VersionValue>newConcurrentMapWithAggressiveConcurrency(),
                 ConcurrentCollections.<BytesRef,VersionValue>newConcurrentMapWithAggressiveConcurrency());
        }
    }

    // All deletes also go here, and delete "tombstones" are retained after refresh:
    private final Map<BytesRef,VersionValue> tombstones = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private volatile Maps maps = new Maps();

    private ReferenceManager mgr;

    /** Bytes consumed for each BytesRef UID:
     *
     *  NUM_BYTES_OBJECT_HEADER + 2*NUM_BYTES_INT + NUM_BYTES_OBJECT_REF + NUM_BYTES_ARRAY_HEADER [ + bytes.length] */
    private static final int BASE_BYTES_PER_BYTESREF = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER +
        2*RamUsageEstimator.NUM_BYTES_INT +
        RamUsageEstimator.NUM_BYTES_OBJECT_REF + 
        RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

    /** Bytes used by having CHM point to a key/value:
     *
     *  CHM.Entry:
     *     + NUM_BYTES_OBJECT_HEADER + 3*NUM_BYTES_OBJECT_REF + NUM_BYTES_INT
     *
     *  CHM's pointer to CHM.Entry, double for approx load factor:
     *     + 2*NUM_BYTES_OBJECT_REF */
    private static final int BASE_BYTES_PER_CHM_ENTRY = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER +
        RamUsageEstimator.NUM_BYTES_INT +
        5*RamUsageEstimator.NUM_BYTES_OBJECT_REF;

    /** Tracks bytes used by current map, i.e. what is freed on refresh. For deletes, which are also added to tombstones, we only account
     *  for the CHM entry here, and account for BytesRef/VersionValue against the tombstones, since refresh would not clear this RAM. */
    final AtomicLong ramBytesUsedCurrent = new AtomicLong();

    /** Tracks bytes used by tombstones (deletes) */
    final AtomicLong ramBytesUsedTombstones = new AtomicLong();

    /** Sync'd because we replace old mgr. */
    synchronized void setManager(ReferenceManager newMgr) {
        if (mgr != null) {
            mgr.removeListener(this);
        }
        mgr = newMgr;

        // In case InternalEngine closes & opens a new IndexWriter/SearcherManager, all deletes are made visible, so we clear old and
        // current here.  This is safe because caller holds writeLock here (so no concurrent adds/deletes can be happeninge):
        maps = new Maps();

        // So we are notified when reopen starts and finishes
        mgr.addListener(this);
    }

    @Override
    public void beforeRefresh() throws IOException {
        // Start sending all updates after this point to the new
        // map.  While reopen is running, any lookup will first
        // try this new map, then fallback to old, then to the
        // current searcher:
        maps = new Maps(ConcurrentCollections.<BytesRef,VersionValue>newConcurrentMapWithAggressiveConcurrency(), maps.current);

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
        maps = new Maps(maps.current, ConcurrentCollections.<BytesRef,VersionValue>newConcurrentMapWithAggressiveConcurrency());
    }

    /** Returns the live version (add or delete) for this uid. */
    VersionValue getUnderLock(BytesRef uid) {
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

        long uidRAMBytesUsed = BASE_BYTES_PER_BYTESREF + uid.bytes.length;

        final VersionValue prev = maps.current.put(uid, version);
        if (prev != null) {
            // Deduct RAM for the version we just replaced:
            long prevBytes = BASE_BYTES_PER_CHM_ENTRY;
            if (prev.delete() == false) {
                prevBytes += prev.ramBytesUsed() + uidRAMBytesUsed;
            }
            ramBytesUsedCurrent.addAndGet(-prevBytes);
        }

        // Add RAM for the new version:
        long newBytes = BASE_BYTES_PER_CHM_ENTRY;
        if (version.delete() == false) {
            newBytes += version.ramBytesUsed() + uidRAMBytesUsed;
        }
        ramBytesUsedCurrent.addAndGet(newBytes);

        final VersionValue prevTombstone;
        if (version.delete()) {
            // Also enroll the delete into tombstones, and account for its RAM too:
            prevTombstone = tombstones.put(uid, version);

            // We initially account for BytesRef/VersionValue RAM for a delete against the tombstones, because this RAM will not be freed up
            // on refresh. Later, in removeTombstoneUnderLock, if we clear the tombstone entry but the delete remains in current, we shift
            // the accounting to current:
            ramBytesUsedTombstones.addAndGet(BASE_BYTES_PER_CHM_ENTRY + version.ramBytesUsed() + uidRAMBytesUsed);

            if (prevTombstone == null && prev != null && prev.delete()) {
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
            assert prev.delete();
            long v = ramBytesUsedTombstones.addAndGet(-(BASE_BYTES_PER_CHM_ENTRY + prev.ramBytesUsed() + uidRAMBytesUsed));
            assert v >= 0: "bytes=" + v;
        }
        final VersionValue curVersion = maps.current.get(uid);
        if (curVersion != null && curVersion.delete()) {
            // We now shift accounting of the BytesRef from tombstones to current, because a refresh would clear this RAM.  This should be
            // uncommon, because with the default refresh=1s and gc_deletes=60s, deletes should be cleared from current long before we drop
            // them from tombstones:
            ramBytesUsedCurrent.addAndGet(curVersion.ramBytesUsed() + uidRAMBytesUsed);
        }
    }

    /** Caller has a lock, so that this uid will not be concurrently added/deleted by another thread. */
    VersionValue getTombstoneUnderLock(BytesRef uid) {
        return tombstones.get(uid);
    }

    /** Iterates over all deleted versions, including new ones (not yet exposed via reader) and old ones (exposed via reader but not yet GC'd). */
    Iterable<Map.Entry<BytesRef,VersionValue>> getAllTombstones() {
        return tombstones.entrySet();
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

        if (mgr != null) {
            mgr.removeListener(this);
            mgr = null;
        }
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsedCurrent.get() + ramBytesUsedTombstones.get();
    }

    /** Returns how much RAM would be freed up by refreshing. This is {@link ramBytesUsed} except does not include tombstones because they
     *  don't clear on refresh. */
    long ramBytesUsedForRefresh() {
        return ramBytesUsedCurrent.get();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        // TODO: useful to break down RAM usage here?
        return Collections.emptyList();
    }
}
