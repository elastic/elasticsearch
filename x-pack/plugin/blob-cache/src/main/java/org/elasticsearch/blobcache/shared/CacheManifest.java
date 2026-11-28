/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.blobcache.common.ByteRange;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A record of which byte ranges of which cache regions held data at one moment. It holds metadata only, so it stays small even for a
 * large cache.
 * <p>
 * A manifest lets a caller put the cache back into a known state: {@link SharedBlobCacheService#evictNotCoveredBy} removes whatever the
 * manifest does not describe, and the caller then fetches the ranges that are missing.
 *
 * @param <KeyType> the cache key type
 */
public final class CacheManifest<KeyType extends SharedBlobCacheService.KeyBase> {

    /**
     * What a manifest records for one region.
     *
     * @param timestampMillis the data timestamp of the region. Never {@link SharedBlobCacheService#BACKFILL_IN_PROGRESS_TIMESTAMP},
     *                        because a restored region has no backfill pass to resolve it.
     * @param ranges          the populated ranges, relative to the start of the region. The constructor sorts them and joins any that
     *                        overlap or touch, so the stored ranges are sorted, disjoint and never adjacent.
     */
    public record Region(long timestampMillis, List<ByteRange> ranges) {

        public Region {
            ranges = List.copyOf(normalize(ranges));
        }

        /**
         * @return {@code true} if every one of the given ranges lies inside one of the ranges of this region
         */
        public boolean covers(Collection<ByteRange> candidates) {
            // both lists are sorted and disjoint, so one forward pass over the ranges of this region is enough
            int next = 0;
            for (ByteRange candidate : normalize(candidates)) {
                while (next < ranges.size() && ranges.get(next).end() < candidate.end()) {
                    next++;
                }
                if (next == ranges.size() || candidate.isSubRangeOf(ranges.get(next)) == false) {
                    return false;
                }
            }
            return true;
        }

        public long totalBytes() {
            return ranges.stream().mapToLong(ByteRange::length).sum();
        }
    }

    private final Map<KeyType, SortedMap<Integer, Region>> regionsByKey;

    private CacheManifest(Map<KeyType, SortedMap<Integer, Region>> regionsByKey) {
        this.regionsByKey = regionsByKey;
    }

    public static <KeyType extends SharedBlobCacheService.KeyBase> Builder<KeyType> builder() {
        return new Builder<>();
    }

    /**
     * @return what the manifest records for the given region, or {@code null} if the region is not in the manifest
     */
    public Region get(KeyType key, int region) {
        final var regions = regionsByKey.get(key);
        return regions == null ? null : regions.get(region);
    }

    /**
     * @return the regions of every key, in region order. The map is unmodifiable.
     */
    public Map<KeyType, SortedMap<Integer, Region>> regions() {
        return regionsByKey;
    }

    public int regionCount() {
        return regionsByKey.values().stream().mapToInt(Map::size).sum();
    }

    public long totalBytes() {
        return regionsByKey.values().stream().flatMap(regions -> regions.values().stream()).mapToLong(Region::totalBytes).sum();
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof CacheManifest<?> that && regionsByKey.equals(that.regionsByKey));
    }

    @Override
    public int hashCode() {
        return Objects.hash(regionsByKey);
    }

    @Override
    public String toString() {
        return "CacheManifest{regions=" + regionCount() + ", bytes=" + totalBytes() + '}';
    }

    /**
     * Sorts the ranges and joins any that overlap or touch, so that equal data always gives equal ranges.
     */
    static List<ByteRange> normalize(Collection<ByteRange> ranges) {
        final List<ByteRange> sorted = new ArrayList<>(ranges);
        sorted.removeIf(ByteRange::isEmpty);
        Collections.sort(sorted);
        final List<ByteRange> merged = new ArrayList<>(sorted.size());
        for (ByteRange next : sorted) {
            final int last = merged.size() - 1;
            if (last >= 0 && next.start() <= merged.get(last).end()) {
                final ByteRange previous = merged.get(last);
                merged.set(last, ByteRange.of(previous.start(), Math.max(previous.end(), next.end())));
            } else {
                merged.add(next);
            }
        }
        return merged;
    }

    /**
     * The timestamp that a manifest records for a region with the given timestamp. A region that is waiting for its timestamp is recorded
     * with an unknown one, because a restored region has no backfill pass to resolve it.
     */
    static long manifestTimestamp(long regionTimestampMillis) {
        return regionTimestampMillis == SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP
            ? SharedBlobCacheService.UNKNOWN_TIMESTAMP
            : regionTimestampMillis;
    }

    public static final class Builder<KeyType extends SharedBlobCacheService.KeyBase> {

        private final Map<KeyType, SortedMap<Integer, Region>> regionsByKey = new HashMap<>();

        private Builder() {}

        /**
         * Adds a region. Regions with no ranges are ignored.
         */
        public Builder<KeyType> add(KeyType key, int region, long timestampMillis, Collection<ByteRange> populatedRanges) {
            final List<ByteRange> ranges = normalize(populatedRanges);
            if (ranges.isEmpty() == false) {
                regionsByKey.computeIfAbsent(key, k -> new TreeMap<>()).put(region, new Region(manifestTimestamp(timestampMillis), ranges));
            }
            return this;
        }

        public CacheManifest<KeyType> build() {
            final Map<KeyType, SortedMap<Integer, Region>> copy = new HashMap<>();
            regionsByKey.forEach((key, regions) -> copy.put(key, Collections.unmodifiableSortedMap(new TreeMap<>(regions))));
            return new CacheManifest<>(Collections.unmodifiableMap(copy));
        }
    }
}
