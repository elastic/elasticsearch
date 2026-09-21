/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test {@link org.elasticsearch.blobcache.shared.EvictionPolicy} that records, per {@link FileCacheKey}, the timestamp stamped on each
 * live cache region as it is cached.
 */
public final class TimestampCapturingEvictionPolicy extends DefaultEvictionPolicy<FileCacheKey> {
    // No region information is available from CacheRegion, so we capture all in a list.
    private final Map<FileCacheKey, List<Long>> capturedTimestamps = new ConcurrentHashMap<>();
    private final Map<FileCacheKey, Collection<CacheRegion<FileCacheKey>>> liveRegions = new ConcurrentHashMap<>();

    @Override
    public void onCached(CacheRegion<FileCacheKey> region) {
        super.onCached(region);
        capturedTimestamps.computeIfAbsent(region.key(), k -> new CopyOnWriteArrayList<>()).add(region.timestampMillis());
        liveRegions.computeIfAbsent(region.key(), k -> ConcurrentHashMap.newKeySet()).add(region);
    }

    @Override
    public void onEvicted(CacheRegion<FileCacheKey> region) {
        super.onEvicted(region);
        var regions = liveRegions.get(region.key());
        if (regions != null) {
            regions.remove(region);
        }
    }

    /**
     * Returns the timestamps stamped on every live region cached for {@code cacheKey} (one entry per cached region, in caching order), or
     * an empty list if no region was cached for that key.
     */
    public List<Long> capturedTimestamps(FileCacheKey cacheKey) {
        return capturedTimestamps.getOrDefault(cacheKey, List.of());
    }

    public Set<FileCacheKey> capturedKeys() {
        return Set.copyOf(capturedTimestamps.keySet());
    }

    public List<Long> liveTimestamps(FileCacheKey cacheKey) {
        var regions = liveRegions.get(cacheKey);
        if (regions == null) {
            return List.of();
        }
        return regions.stream().map(CacheRegion::timestampMillis).toList();
    }
}
