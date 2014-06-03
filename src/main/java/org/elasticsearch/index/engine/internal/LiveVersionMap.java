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
import java.util.concurrent.ConcurrentMap;

import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

// TODO: use Lucene's LiveFieldValues, but we need to somehow extend it to handle SearcherManager changing, and to handle long-lasting (GC'd
// by time) tombstones

/** Maps _uid value to its version information. */
class LiveVersionMap implements ReferenceManager.RefreshListener {

    // All writes go into here:
    private volatile Map<BytesRef,VersionValue> addsCurrent = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // Only used while refresh is running:
    private volatile Map<BytesRef,VersionValue> addsOld = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // Holds tombstones for deleted docs, expiring by their own schedule; not private so InternalEngine can prune:
    private final Map<BytesRef,VersionValue> deletes = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private ReferenceManager mgr;

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
        addsOld = addsCurrent;
        // Start sending all updates after this point to the new
        // map.  While reopen is running, any lookup will first
        // try this new map, then fallback to old, then to the
        // current searcher:
        addsCurrent = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // Now drop all the old values because they are now
        // visible via the searcher that was just opened; if
        // didRefresh is false, it's possible old has some
        // entries in it, which is fine: it means they were
        // actually already included in the previously opened
        // reader.  So we can safely clear old here:
        addsOld = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    }

    /** Caller has a lock, so that this uid will not be concurrently added/deleted by another thread. */
    public VersionValue getUnderLock(BytesRef uid) {
        // First try to get the "live" value:
        VersionValue value = addsCurrent.get(uid);
        if (value != null) {
            return value;
        }

        value = addsOld.get(uid);
        if (value != null) {
            return value;
        }

        value = deletes.get(uid);
        if (value != null) {
            return value;
        }

        return null;
    }

    /** Adds this uid/version to the pending adds map. */
    public void putUnderLock(BytesRef uid, VersionValue version) {
        deletes.remove(uid);
        addsCurrent.put(uid, version);
    }

    /** Adds this uid/version to the pending deletes map. */
    public void putDeleteUnderLock(BytesRef uid, VersionValue version) {
        addsCurrent.remove(uid);
        addsOld.remove(uid);
        deletes.put(uid, version);
    }

    /** Returns the current deleted version for this uid. */
    public VersionValue getDeleteUnderLock(BytesRef uid) {
        return deletes.get(uid);
    }

    /** Removes this uid from the pending deletes map. */
    public void removeDeleteUnderLock(BytesRef uid) {
        deletes.remove(uid);
    }

    /** Iterates over all pending deletions. */
    public Iterable<Map.Entry<BytesRef,VersionValue>> getAllDeletes() {
        return deletes.entrySet();
    }

    /** Called when this index is closed. */
    public void clear() {
        addsCurrent.clear();
        addsOld.clear();
        deletes.clear();
        if (mgr != null) {
            mgr.removeListener(this);
            mgr = null;
        }
    }
}
