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

package org.elasticsearch.index.engine.internal;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

/** Maps _uid value to its version information. */
class LiveVersionMap implements ReferenceManager.RefreshListener {

    // TODO: add implements Accountable with Lucene 4.9
    static {
        assert Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_48 : "Add 'implements Accountable' here";
    }

    // All writes (adds and deletes) go into here:
    private volatile Map<BytesRef,VersionValue> current = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // Used while refresh is running, and to hold adds/deletes until refresh finishes.  We read from both current and old on lookup:
    private volatile Map<BytesRef,VersionValue> old = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // All deletes also go here, and delete "tombstones" are retained after refresh:
    private volatile Map<BytesRef,VersionValue> tombstones = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private ReferenceManager mgr;

    /** Bytes consumed for each added UID + version, not including UID's bytes.
     *
     * VersionValue.ramBytesUsed() +
     *
     * BytesRef:
     *     + NUM_BYTES_OBJECT_HEADER + 2*NUM_BYTES_INT + NUM_BYTES_OBJECT_REF + NUM_BYTES_ARRAY_HEADER [ + uid.text().length()]
     *
     * CHM.Entry:
     *     + NUM_BYTES_OBJECT_HEADER + 3*NUM_BYTES_OBJECT_REF + NUM_BYTES_INT
     *
     * CHM's pointer to CHM.Entry, double for approx load factor:
     *     + 2*NUM_BYTES_OBJECT_REF */

    private static final int BASE_BYTES_PER_ADD = 2*RamUsageEstimator.NUM_BYTES_OBJECT_HEADER +
        3*RamUsageEstimator.NUM_BYTES_INT +
        6*RamUsageEstimator.NUM_BYTES_OBJECT_REF + 
        RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

    final AtomicLong ramBytesUsed = new AtomicLong();

    public void setManager(ReferenceManager newMgr) {
        if (mgr != null) {
            mgr.removeListener(this);
        }
        mgr = newMgr;

        // So we are notified when reopen starts and finishes
        mgr.addListener(this);
    }

    @Override
    public void beforeRefresh() throws IOException {
        old = current;
        ramBytesUsed.set(0);
        // Start sending all updates after this point to the new
        // map.  While reopen is running, any lookup will first
        // try this new map, then fallback to old, then to the
        // current searcher:
        current = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // We can now drop old because these operations are now visible via the newly opened searcher.  Even if didRefresh is false, it's
        // possible old has some entries in it, which is fien: it means they were actually already included in the previously opened reader,
        // so we can still safely drop them in that case.  We don't touch deletes here: they expire on their own index.gc_deletes
        // timeframe:
        old = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    }

    /** If includeDeletes is true we also check and return a tombstone delete. */
    public VersionValue getUnderLock(BytesRef uid) {
        // First try to get the "live" value:
        VersionValue value = current.get(uid);
        if (value != null) {
            return value;
        }

        value = old.get(uid);
        if (value != null) {
            return value;
        }

        return tombstones.get(uid);
    }

    /** Adds this uid/version to the pending adds map. */
    public void putUnderLock(BytesRef uid, VersionValue version) {
        VersionValue prev = current.put(uid, version);
        if (prev != null) {
            ramBytesUsed.addAndGet(-(BASE_BYTES_PER_ADD + uid.bytes.length + prev.ramBytesUsed()));
        }
        ramBytesUsed.addAndGet(BASE_BYTES_PER_ADD + uid.bytes.length + version.ramBytesUsed());
        if (version.delete()) {
            tombstones.put(uid, version);
        } else {
            // UID came back to life so we remove the tombstone:
            tombstones.remove(uid);
        }
    }

    /** Removes this uid from the pending deletes map. */
    public void removeTombstoneUnderLock(BytesRef uid) {
        tombstones.remove(uid);
    }

    /** Caller has a lock, so that this uid will not be concurrently added/deleted by another thread. */
    public VersionValue getTombstoneUnderLock(BytesRef uid) {
        return tombstones.get(uid);
    }

    /** Iterates over all old versions, i.e. versions that have already been exposed in a reader. */
    public Iterable<Map.Entry<BytesRef,VersionValue>> getAllTombstones() {
        return tombstones.entrySet();
    }

    /** Called when this index is closed. */
    public void clear() {
        current.clear();
        old.clear();
        tombstones.clear();
        ramBytesUsed.set(0);
        if (mgr != null) {
            mgr.removeListener(this);
            mgr = null;
        }
    }

    // @Override
    public long ramBytesUsed() {
        return ramBytesUsed.get();
    }
}
