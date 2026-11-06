/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.index.engine.LiveVersionMap;
import org.elasticsearch.index.engine.VersionValue;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Supplier;

/**
 * An archive that keeps every evacuated old map by reference rather than copying its entries into a per-generation map. Selected with
 * {@code stateless.live_version_map.archive: by_reference}; see {@link StatelessLiveVersionMapArchive} for the per-generation variant.
 */
public class ByReferenceLiveVersionMapArchive implements StatelessLiveVersionMapArchiveApi {
    // Used to keep track of VersionValues while a refresh on unpromotable shards is pending.
    // Keeps every evacuated old map by reference, oldest first, tagged with the generation its entries go into once a flush happens,
    // to decide which evacuated maps can be removed once the unpromotable shards are refreshed. The maps are not copied: a writer
    // that read the maps before a refresh may still write into an old map after it was handed over (under its uid lock), and every
    // lookup, unsafe flag and min delete timestamp is read from the maps themselves so that such late writes are seen.
    private final Deque<Entry> archive = new ConcurrentLinkedDeque<>();
    // Provides the generation for the currently ongoing commit or the last successful commit.
    // See {@link org.elasticsearch.index.engine.InternalEngine#getPreCommitSegmentGeneration}.
    private final Supplier<Long> preCommitGenerationSupplier;
    // Refreshed as a side effect of getRamBytesUsed(), which the memory and disk controllers call periodically, for the node metrics.
    private volatile CachedStats cachedStats = CachedStats.ZERO;

    // Entry and deque node. As before, the lookup object itself (all an empty lookup costs) is not accounted for.
    private static final long BASE_BYTES_PER_ENTRY = RamUsageEstimator.shallowSizeOfInstance(Entry.class)
        + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF; // deque node

    record Entry(long generation, LiveVersionMap.VersionLookup map) {}

    ByReferenceLiveVersionMapArchive(Supplier<Long> preCommitGenerationSupplier) {
        this.preCommitGenerationSupplier = preCommitGenerationSupplier;
    }

    @Override
    public void afterRefresh(LiveVersionMap.VersionLookup old) {
        // We keep every map, even an empty one: it may be unsafe, have seen a delete, or be written to after this call. The
        // LiveVersionMap does not rotate (and so never hands over) a map that has not seen any operation.
        // We record the generation that these entries would go into once a flush happens.
        // todo: this can lead to requiring two flushes/commits to clear up an unsafe map since we conservatively wait for +1
        // generation to ensure safety when parallel indexing and local refreshes happen between the local lucene commit and the
        // refresh that happens as part of the flush.
        final long generation = preCommitGenerationSupplier.get() + 1;
        // Refreshes are serialized, so there is no concurrent afterRefresh call between this check and the addLast below.
        assert archive.isEmpty() || archive.peekLast().generation() <= generation
            : "generation went backwards: " + archive.peekLast().generation() + " > " + generation;
        archive.addLast(new Entry(generation, old));
    }

    @Override
    public void afterUnpromotablesRefreshed(long generation) {
        // Only remove the head we peeked, so that concurrent calls never remove an entry newer than their own generation.
        Entry head;
        while ((head = archive.peekFirst()) != null && head.generation() <= generation) {
            archive.remove(head);
        }
    }

    static long archiveEntryBytesUsed(LiveVersionMap.VersionLookup versionLookup) {
        return BASE_BYTES_PER_ENTRY + versionLookup.ramBytesUsed();
    }

    @Override
    public VersionValue get(BytesRef uid) {
        // newest first
        for (Iterator<Entry> it = archive.descendingIterator(); it.hasNext();) {
            VersionValue v = it.next().map().get(uid);
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    @Override
    public long getMinDeleteTimestamp() {
        long min = Long.MAX_VALUE;
        for (Entry entry : archive) {
            min = Math.min(min, entry.map().minDeleteTimestamp());
        }
        return min;
    }

    // package private for testing
    Deque<Entry> archive() {
        return archive;
    }

    /**
     * Returns the generation that we need to receive an unpromotable refresh for in order to consider the archive safe, i.e. the
     * generation of the newest unsafe map held, or -1 if no unsafe map is held.
     */
    // visible for testing
    @Override
    public long getMinSafeGeneration() {
        for (Iterator<Entry> it = archive.descendingIterator(); it.hasNext();) {
            Entry entry = it.next();
            if (entry.map().isUnsafe()) {
                return entry.generation();
            }
        }
        return -1;
    }

    @Override
    public boolean isUnsafe() {
        return getMinSafeGeneration() != -1;
    }

    @Override
    public long safeGenerationForGets(long lastCommittedGeneration) {
        final long minSafeGeneration = getMinSafeGeneration();
        if (minSafeGeneration == -1 || lastCommittedGeneration < minSafeGeneration) {
            return -1;
        }
        return minSafeGeneration;
    }

    @Override
    public long getRamBytesUsed() {
        long memBytesUsed = 0;
        long mapsHeld = 0;
        for (Entry entry : archive) {
            memBytesUsed += archiveEntryBytesUsed(entry.map());
            mapsHeld++;
        }
        // Lock-free walk: count and bytes are consistent with each other only up to concurrent hand-offs and acks.
        cachedStats = new CachedStats(mapsHeld, memBytesUsed);
        return memBytesUsed;
    }

    @Override
    public CachedStats cachedStats() {
        return cachedStats;
    }

    @Override
    public long getReclaimableRamBytes() {
        long preCommitGeneration = preCommitGenerationSupplier.get();
        long notFlushingBytes = 0;
        for (Entry entry : archive) {
            if (entry.generation() > preCommitGeneration) {
                notFlushingBytes += archiveEntryBytesUsed(entry.map());
            }
        }
        return notFlushingBytes;
    }

    @Override
    public long getRefreshingRamBytes() {
        long preCommitGeneration = preCommitGenerationSupplier.get();
        long flushingBytes = 0;
        for (Entry entry : archive) {
            if (entry.generation() <= preCommitGeneration) {
                flushingBytes += archiveEntryBytesUsed(entry.map());
            }
        }
        return flushingBytes;
    }
}
