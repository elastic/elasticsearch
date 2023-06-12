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
    private final Supplier<Long> generationSupplier;
    private final Object mutex = new Object();
    private final AtomicLong minDeleteTimestamp = new AtomicLong(Long.MAX_VALUE);

    StatelessLiveVersionMapArchive(Supplier<Long> generationSupplier) {
        this.generationSupplier = generationSupplier;
    }

    @Override
    public void afterRefresh(LiveVersionMap.VersionLookup old) {
        // Even if the old version lookup to archive is empty, we might need to keep track of it since it
        // might have seen a delete that we will need to calculate the archive's min delete timestamp.
        if (old.isEmpty() && old.minDeleteTimestamp() == Long.MAX_VALUE) {
            return;
        }
        LiveVersionMap.VersionLookup existing;
        synchronized (mutex) {
            minDeleteTimestamp.accumulateAndGet(old.minDeleteTimestamp(), Math::min);
            // we record the generation that these new entries would go into once a flush happens.
            long generation = generationSupplier.get() + 1;
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
}
