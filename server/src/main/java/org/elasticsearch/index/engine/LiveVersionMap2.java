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
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

public class LiveVersionMap2 implements ReferenceManager.RefreshListener, Accountable {
    // Per-document lock.
    private final KeyedLock<BytesRef> keyedLock = new KeyedLock<>();

    private final StampedLock stateLock = new StampedLock();
    private volatile State state = new State(new Generation(-1, false), new LinkedList<>());

    // Holds a version where this document was deleted.
    // Needs to survive refreshes bla bla bla.
    private final ConcurrentMap<BytesRef, DeleteVersionValue> tombstones = new ConcurrentHashMap<>();

    private final AtomicLong safeAccessRequests = new AtomicLong();
    private final Releasable safeAccessReleasable = () -> {
        long result = safeAccessRequests.decrementAndGet();
        assert result >= 0 : "decremented safeAccessRequests to below 0";
    };

    private record State(Generation current, List<Generation> previous) {}

    private static class Generation {
        private final ConcurrentMap<BytesRef, VersionValue> versions;

        private volatile boolean unsafe;

        Generation(int initialCapacity, boolean unsafe) {
            this.versions = initialCapacity < 0 ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(initialCapacity);
            this.unsafe = unsafe;
        }
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

    /// Returns the most recent version of the document if it has been recently modified.
    /// Returns `null` if this information is not available in the version map and needs to be obtained from Lucene.
    VersionValue getUnderLock(final BytesRef uid) {
        assert assertKeyedLockHeldByCurrentThread(uid);

        // We are holding a per-document lock here so concurrent modifications are not possible.
        // It is possible that any number of concurrent refreshes happen.
        // This is also fine because as established before, concurrent operations can't happen.
        // So either we'll read the latest version from state (even though it is maybe stale with regard to refreshes, it's still correct),
        // or we'll read `null` because of the refresh which is also correct.
        var state = this.state;
        return readVersion(state, uid);

        // long stamp;
        // VersionValue result;
        // do {
        // // Optimistically read from the current version of state.
        // // It is possible that a refresh invalidates this state which we'll see in validate(stamp) below, but we can just retry.
        // stamp = stateLock.tryOptimisticRead();
        // var state = this.state;
        // result = readVersion(state, uid);
        // } while (stateLock.validate(stamp) == false);
        //
        // return result;
    }

    private VersionValue readVersion(LiveVersionMap2.State state, BytesRef uid) {
        // We know here that the caller checked if the map is unsafe, enforced safe access and triggered refresh if needed.
        // TODO is this the best API?

        // The map can actually be unsafe at this point because concurrent append of a different document marked it as so.
        // That doesn't impact this read.
        // We know that callers called `enforceSafeAccess`, and then check for unsafe and refreshed if needed
        // under a key lock.
        // So while the map can be unsafe, it's definitely safe _for this specific document_:
        // * no unsafe operations could have been performed concurrently due to key lock
        // * if any such operations were performed before taking the lock, the refresh fixed that

        var currentVersion = state.current.versions.get(uid);
        if (currentVersion != null) {
            return currentVersion;
        }

        // During a refresh we move what was `current` at the time to `previous`.
        // It still contains valid data (unless it's unsafe) and we can use it.
        // TODO double check, what if this generation is unsafe? It's possible in stateless, right?
        for (var generation : state.previous) {
            // This generation can be unsafe, but the same logic as explained above applies.
            var version = generation.versions.get(uid);
            if (version != null) {
                return version;
            }
        }

        var tombstone = tombstones.get(uid);
        if (tombstone != null) {
            return tombstone;
        }

        return null;
    }

    /// Callers are expected to call `enforceSafeAccess()` and hold the releasable for the duration of the operation
    /// at this point.
    /// Because of that the next refresh has to produce a safe map.
    ///
    boolean isUnsafe() {
        long stamp;
        boolean unsafe = false;
        do {
            stamp = stateLock.tryOptimisticRead();
            var state = this.state;
            if (state.current.unsafe) {
                unsafe = true;
            } else {
                for (var generation : state.previous) {
                    if (generation.unsafe) {
                        unsafe = true;
                        break;
                    }
                }
            }
        } while (stateLock.validate(stamp) == false);

        return unsafe;
    }

    /// Returns if the map is unsafe specifically in the context of executing a realtime get.
    /// See `StatelessLiveVersionMap`, [InternalEngine#getVersionForRead], [org.elasticsearch.action.get.TransportGetFromTranslogAction].
    boolean isUnsafeForGets() {
        // TODO stateful only code
        return isUnsafe();
    }

    long safeGenerationForGets(long lastCommittedGeneration) {
        return -1;
    }

    /// Identifies that an operation is requesting safe access on the map until the provided [Releasable] is closed.
    /// This is a signal that the map needs to be kept safe across any number of concurrent refreshes.
    Releasable enforceSafeAccess() {
        safeAccessRequests.incrementAndGet();
        return safeAccessReleasable;
    }

    // Used in tests only outside of this class.
    boolean isSafeAccessRequired() {
        return safeAccessRequests.get() > 0;
    }

    void maybePutIndexUnderLock(BytesRef uid, IndexVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);

        long stamp;
        do {
            stamp = stateLock.tryOptimisticRead();
            var state = this.state;

            if (isSafeAccessRequired()) {
                // It is still possible that current generation is unsafe due to previous/concurrent unsafe operations.
                // A refresh should make it safe, and whoever requested safe access that we just read should refresh.
                // TODO we could skip this operation then??
                state.current.versions.put(uid, version);
                removeTombstoneUnderLock(uid);
            } else {

                // TODO why exactly is this required?
                // document it
                removeTombstoneUnderLock(uid);
                // Since we skipped recording a version, current generation is unsafe.
                state.current.unsafe = true;
            }
        } while (stateLock.validate(stamp) == false);
    }

    void putIndexUnderLock(BytesRef uid, IndexVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;

        long stamp;
        do {
            stamp = stateLock.tryOptimisticRead();
            var state = this.state;

            state.current.versions.put(uid, version);
        } while (stateLock.validate(stamp) == false);

        removeTombstoneUnderLock(uid);
    }

    void putDeleteUnderLock(BytesRef uid, DeleteVersionValue version) {
        assert assertKeyedLockHeldByCurrentThread(uid);
        assert uid.bytes.length == uid.length : "Oversized _uid! UID length: " + uid.length + ", bytes length: " + uid.bytes.length;

        // TODO does this order matter? Document if so, probably not
        putTombstone(uid, version);

        // We need to remove this document from both the current and previous maps.
        // Otherwise, we can read a stale version from a previous map.
        long stamp;
        do {
            stamp = stateLock.tryOptimisticRead();
            var state = this.state;

            /// This can run concurrently with the move of `current` to `previous` in [beforeRefresh()];
            /// It's okay since we were going to remove the version from `previous` anyway below.
            state.current.versions.remove(uid);

            /// Again, `state.previous` can point to a previous `state` instance.
            /// We need to remove the version from all previous generations anyway, and we'll retry
            /// to make sure we caught them all.
            for (var generation : state.previous) {
                generation.versions.remove(uid);
            }

        } while (stateLock.validate(stamp) == false);
    }

    private void putTombstone(BytesRef uid, DeleteVersionValue version) {
        // TODO memory accounting
        tombstones.put(uid, version);
    }

    void removeTombstoneUnderLock(BytesRef uid) {
        // TODO memory accounting, asserts
        tombstones.remove(uid);
    }

    void pruneTombstones(long maxTimestampToPrune, long maxSeqNoToPrune) {

    }

    void clear() {
        // TODO Does this even matter?
    }

    long ramBytesUsedForRefresh() {
        return 0;
    }

    long reclaimableRefreshRamBytes() {
        return 0;
    }

    long getRefreshingBytes() {
        return 0;
    }

    @Override
    public void beforeRefresh() throws IOException {
        // We are about to perform a refresh meaning that all recently indexed document versions
        // stored in the current generation will be visible using a searcher (making the generation obsolete).
        // We don't know what is the exact "cutoff" point of documents that will be included in the refresh
        // since we only see the "before" and "after" state.
        // So we proactively move all documents to the older generation here so be completely sure that _all_ of them
        // included in the refresh.
        // Some of the documents that will be indexed right after this call will also be included in the refresh
        // but there is nothing incorrect about this.
        long stamp = stateLock.writeLock();
        var state = this.state;

        // New map is safe by definition since it contains no unsafe operations.
        var newState = new State(new Generation(state.current.versions.size(), false), new LinkedList<>(state.previous));
        // Note that there are concurrent writes being made into `state.current` by methods that use optimistic read locks.
        // They'll retry and write the same version into the new `current` map so that's okay.
        newState.previous.addFirst(state.current);

        this.state = newState;

        stateLock.unlockWrite(stamp);
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        // Refresh happens under a refresh lock in Lucene so we know `afterRefresh` always follows `beforeRefresh`.
        // We know now that the last generation was refreshed.
        // It can be dropped if the retention implementation agrees.

        // TODO can we be clever here with this lock somehow?
        long stamp = stateLock.writeLock();
        var state = this.state;

        // TODO stateful only logic
        this.state = new State(state.current, new LinkedList<>());

        stateLock.unlockWrite(stamp);
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    // Test only methods below.

    VersionValue getVersionForAssert(final BytesRef uid) {
        return null;
    }

    Map<BytesRef, VersionValue> getAllCurrent() {
        return Map.of();
    }

    Map<BytesRef, DeleteVersionValue> getAllTombstones() {
        return Map.of();
    }

    boolean assertKeyedLockHeldByCurrentThread(BytesRef uid) {
        assert keyedLock.isHeldByCurrentThread(uid) : "Thread [" + Thread.currentThread().getName() + "], uid [" + uid.utf8ToString() + "]";
        return true;
    }
}
