/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;

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
 */
public final class SegmentReservations {

    private static final class Entry {
        final long bytes;
        int refCount;

        Entry(long bytes) {
            this.bytes = bytes;
            this.refCount = 1;
        }
    }

    private final Map<String, Entry> entries = new HashMap<>();
    private long totalBytes;

    /** Bytes that would be newly reserved if {@code infos} were accepted now. Does not mutate state. */
    public synchronized long predictDelta(SegmentInfos infos, ToLongFunction<SegmentCommitInfo> bytesFn) {
        long delta = 0L;
        for (SegmentCommitInfo sci : infos) {
            if (entries.containsKey(key(sci)) == false) {
                delta += bytesFn.applyAsLong(sci);
            }
        }
        return delta;
    }

    /** Reserve bytes for new segments and refcount-bump segments already tracked. Returns bytes newly reserved. */
    public synchronized long reserve(SegmentInfos infos, ToLongFunction<SegmentCommitInfo> bytesFn) {
        long delta = 0L;
        for (SegmentCommitInfo sci : infos) {
            String key = key(sci);
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
        return delta;
    }

    /** Decrement refcounts for the given segment keys, freeing bytes when refcount drops to zero. */
    public synchronized long release(Set<String> segmentKeys) {
        long released = 0L;
        for (String key : segmentKeys) {
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

    /**
     * Extract the segment keys for {@code infos} — one short string per segment, sufficient to release the
     * reservation later without retaining a reference to the full {@link SegmentInfos}.
     */
    public static Set<String> keysOf(SegmentInfos infos) {
        Set<String> keys = new HashSet<>(infos.size() * 2);
        for (SegmentCommitInfo sci : infos) {
            keys.add(key(sci));
        }
        return keys;
    }

    public synchronized long totalBytes() {
        return totalBytes;
    }

    public synchronized int trackedSegmentCount() {
        return entries.size();
    }

    private static String key(SegmentCommitInfo sci) {
        return sci.info.name + ":" + sci.getDocValuesGen();
    }
}
