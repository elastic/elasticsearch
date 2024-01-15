/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.index.engine.LiveVersionMap;
import org.elasticsearch.index.engine.LiveVersionMapArchive;
import org.elasticsearch.index.engine.VersionValue;

import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class StatelessLiveVersionMapArchive implements LiveVersionMapArchive {
    // Used to keep track of VersionValues while a refresh on unpromotable shards is pending.
    // Keeps track of the evacuated old map entries and the generation at the time of the refresh
    // to decide which evacuated maps can be removed upon a flush.
    private final NavigableMap<Long, LiveVersionMap.VersionLookup> archivePerGeneration = new TreeMap<>(Comparator.reverseOrder());
    // Provides the generation for the currently ongoing commit or the last successful commit.
    // See {@link org.elasticsearch.index.engine.InternalEngine#getPreCommitSegmentGeneration}.
    private final Supplier<Long> preCommitGenerationSupplier;
    private final Object mutex = new Object();
    private final AtomicLong minDeleteTimestamp = new AtomicLong(Long.MAX_VALUE);
    // Keeps track of an unsafe old map that has been passed to the archive and the archive has not yet received an unpromotable
    // refresh response for a generation that includes the changes that happened during the unsafe map. We need to track this, to
    // be able to correctly remember across local refreshes, that we were indexing using an unsafe map, and trigger a flush once
    // the first get request forces switching to a safe map.
    private volatile boolean isUnsafe = false;
    // Records the generation that we need to receive unpromotable refresh for, in order to consider the archive map safe.
    private volatile long minSafeGeneration = -1;

    private static final long MAP_ENTRY_KEY_BYTES = RamUsageEstimator.shallowSizeOfInstance(Long.class);
    private static final long BASE_BYTES_PER_MAP_ENTRY;

    static {
        // use the same impl as archivePerGeneration
        Map<Integer, Integer> map = new TreeMap<>();
        map.put(0, 0);
        long mapEntryShallowSize = RamUsageEstimator.shallowSizeOf(map.entrySet().iterator().next());
        // assume a load factor of 50%
        // for each entry, we need two object refs, one for the entry itself
        // and one for the free space that is due to the fact hash tables can
        // not be fully loaded
        BASE_BYTES_PER_MAP_ENTRY = mapEntryShallowSize + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }

    StatelessLiveVersionMapArchive(Supplier<Long> preCommitGenerationSupplier) {
        this.preCommitGenerationSupplier = preCommitGenerationSupplier;
    }

    @Override
    public void afterRefresh(LiveVersionMap.VersionLookup old) {
        LiveVersionMap.VersionLookup existing;
        synchronized (mutex) {
            if (old.isUnsafe()) {
                isUnsafe = true;
                // todo: this can lead to requiring two flushes/commits to clear up the `isUnsafe` flag since we conservatively
                // wait for +1 generation to ensure safety when parallel indexing and local refreshes happen between the
                // local lucene commit and the refresh that happens as part of the flush.
                minSafeGeneration = preCommitGenerationSupplier.get() + 1;
            }
            // Even if the old version lookup to archive is empty, we might need to keep track of it since it
            // might have seen a delete that we will need to calculate the archive's min delete timestamp.
            if (old.isEmpty() && old.minDeleteTimestamp() == Long.MAX_VALUE) {
                return;
            }
            minDeleteTimestamp.accumulateAndGet(old.minDeleteTimestamp(), Math::min);
            // we record the generation that these new entries would go into once a flush happens.
            long generation = preCommitGenerationSupplier.get() + 1;
            existing = archivePerGeneration.get(generation);
            if (existing == null) {
                archivePerGeneration.put(generation, old);
                return;
            }
        }
        existing.merge(old);
    }

    public void afterUnpromotablesRefreshed(long generation) {
        synchronized (mutex) {
            if (generation >= minSafeGeneration) {
                isUnsafe = false;
            }
            // go through the map and remove all entries with key <= generation
            archivePerGeneration.entrySet().removeIf(entry -> entry.getKey() <= generation);
            // update min delete timestamp
            var newMin = archivePerGeneration.values()
                .stream()
                .mapToLong(LiveVersionMap.VersionLookup::minDeleteTimestamp)
                .min()
                .orElse(Long.MAX_VALUE);
            minDeleteTimestamp.set(newMin);
        }
    }

    static long archiveEntryBytesUsed(LiveVersionMap.VersionLookup versionLookup) {
        return BASE_BYTES_PER_MAP_ENTRY + MAP_ENTRY_KEY_BYTES + versionLookup.ramBytesUsed();
    }

    @Override
    public VersionValue get(BytesRef uid) {
        synchronized (mutex) {
            // the map is sorted by descending generations
            for (var versionLookup : archivePerGeneration.values()) {
                VersionValue v = versionLookup.get(uid);
                if (v != null) {
                    return v;
                }
            }
        }
        return null;
    }

    @Override
    public long getMinDeleteTimestamp() {
        return minDeleteTimestamp.get();
    }

    // package private for testing
    Map<Long, LiveVersionMap.VersionLookup> archivePerGeneration() {
        return archivePerGeneration;
    }

    // visible for testing
    public long getMinSafeGeneration() {
        return minSafeGeneration;
    }

    @Override
    public boolean isUnsafe() {
        return isUnsafe;
    }

    @Override
    public long getMemoryBytesUsed() {
        long memBytesUsed = 0;
        for (var versionLookup : archivePerGeneration.values()) {
            memBytesUsed += archiveEntryBytesUsed(versionLookup);
        }
        return memBytesUsed;
    }

    @Override
    public long getReclaimableMemoryBytes() {
        long preCommitGeneration = preCommitGenerationSupplier.get();
        long notFlushingBytes = 0;
        for (var entry : archivePerGeneration.entrySet()) {
            if (entry.getKey() > preCommitGeneration) {
                notFlushingBytes += archiveEntryBytesUsed(entry.getValue());
            } else {
                break;
            }
        }
        return notFlushingBytes;
    }

    @Override
    public long getRefreshingMemoryBytes() {
        long preCommitGeneration = preCommitGenerationSupplier.get();
        long flushingBytes = 0;
        for (var entry : archivePerGeneration.entrySet()) {
            if (entry.getKey() <= preCommitGeneration) {
                flushingBytes += archiveEntryBytesUsed(entry.getValue());
            }
        }
        return flushingBytes;
    }
}
