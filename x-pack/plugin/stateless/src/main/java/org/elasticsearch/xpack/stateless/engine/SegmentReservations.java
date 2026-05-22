/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.core.Releasable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.ToLongFunction;

/**
 * Refcounted per-segment reservation tracker keyed by (segment name, doc-values generation). Matches how Lucene
 * shares {@code SegmentReader} instances across directory-reader generations: a segment held by multiple
 * readers is reserved once, freed when the last holder releases. All methods are synchronized because refresh
 * and reader-close callbacks run on different threads.
 *
 * <p>{@link #reserve} returns a {@link Reservation} that owns the underlying keys; the caller closes (or
 * {@link Reservation#release releases}) it from the reader-close listener instead of plumbing the key set
 * around the engine.
 */
public final class SegmentReservations {

    /**
     * Identifies a reservation entry by segment name and the doc-values generation of the reader holding it.
     * A bumped doc-values generation produces a fresh {@code SegmentReader} (and a fresh soft-delete bitset),
     * so it gets a distinct reservation from the prior generation.
     */
    public record SegmentKey(String segmentName, long docValuesGen) {

        public static SegmentKey of(SegmentCommitInfo sci) {
            return new SegmentKey(sci.info.name, sci.getDocValuesGen());
        }
    }

    private static final class Entry {
        final long bytes;
        int refCount;

        Entry(long bytes) {
            this.bytes = bytes;
            this.refCount = 1;
        }
    }

    private final Map<SegmentKey, Entry> entries = new HashMap<>();
    private long totalBytes;

    /**
     * Bytes that would be newly reserved if {@code infos} were accepted now. Does not mutate state.
     *
     * <p>{@code bytesFn} runs under this instance's monitor; it must be cheap (O(1), no I/O, no blocking).
     * An expensive {@code bytesFn} would extend the time refresh and reader-close callbacks contend on this lock.
     */
    public synchronized long predictDelta(SegmentInfos infos, ToLongFunction<SegmentCommitInfo> bytesFn) {
        long delta = 0L;
        for (SegmentCommitInfo sci : infos) {
            if (entries.containsKey(SegmentKey.of(sci)) == false) {
                delta += bytesFn.applyAsLong(sci);
            }
        }
        return delta;
    }

    /**
     * Reserve bytes for the given segments and refcount-bump segments already tracked. Returns a {@link Reservation}
     * carrying the keys to release on reader close and the bytes newly charged against the ledger.
     *
     * <p>{@code bytesFn} runs under this instance's monitor; it must be cheap (O(1), no I/O, no blocking).
     * An expensive {@code bytesFn} would extend the time refresh and reader-close callbacks contend on this lock.
     */
    public synchronized Reservation reserve(SegmentInfos infos, ToLongFunction<SegmentCommitInfo> bytesFn) {
        long delta = 0L;
        Set<SegmentKey> keys = new HashSet<>(infos.size() * 2);
        for (SegmentCommitInfo sci : infos) {
            SegmentKey key = SegmentKey.of(sci);
            keys.add(key);
            Entry existing = entries.get(key);
            if (existing == null) {
                long bytes = bytesFn.applyAsLong(sci);
                entries.put(key, new Entry(bytes));
                totalBytes += bytes;
                delta += bytes;
            } else {
                existing.refCount++;
            }
        }
        return new Reservation(this, keys, delta);
    }

    /** Decrement refcounts for the given segment keys, freeing bytes when refcount drops to zero. */
    synchronized long releaseKeys(Set<SegmentKey> segmentKeys) {
        long released = 0L;
        for (SegmentKey key : segmentKeys) {
            Entry existing = entries.get(key);
            assert existing != null && existing.refCount > 0 : "release of untracked segment " + key;
            existing.refCount--;
            if (existing.refCount == 0) {
                entries.remove(key);
                totalBytes -= existing.bytes;
                released += existing.bytes;
            }
        }
        return released;
    }

    public synchronized long totalBytes() {
        return totalBytes;
    }

    public synchronized int trackedSegmentCount() {
        return entries.size();
    }

    /**
     * Handle to a set of segment-key refcount bumps owned by a single reader. Closing or {@link #release releasing}
     * decrements those refcounts and reports the bytes actually freed from the ledger (zero when other readers
     * still pin the same segments). Idempotent — repeated calls return zero.
     */
    public static final class Reservation implements Releasable {
        private final SegmentReservations owner;
        private final Set<SegmentKey> keys;
        private final long bytesReserved;
        private boolean released;

        Reservation(SegmentReservations owner, Set<SegmentKey> keys, long bytesReserved) {
            this.owner = owner;
            this.keys = keys;
            this.bytesReserved = bytesReserved;
        }

        /** Bytes added to the ledger by this reservation (zero when all segments were already tracked). */
        public long bytesReserved() {
            return bytesReserved;
        }

        /** Segment keys held by this reservation. Useful for diagnostics and reclamation policies. */
        public Set<SegmentKey> keys() {
            return keys;
        }

        /**
         * Decrement the refcounts for this reservation's keys; returns bytes actually freed from the ledger
         * (zero when other readers still pin the same segments). Idempotent — repeated calls return zero.
         */
        public synchronized long release() {
            if (released) {
                return 0L;
            }
            released = true;
            return owner.releaseKeys(keys);
        }

        @Override
        public void close() {
            release();
        }
    }
}
