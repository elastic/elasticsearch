/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;

/**
 * Test utilities for {@link SharedBlobCacheService} that expose package-private methods to other modules.
 */
public final class SharedBlobCacheServiceTestUtils {

    private SharedBlobCacheServiceTestUtils() {}

    /**
     * A cache-region timestamp for tests that do not care about timestamp semantics: either
     * {@link SharedBlobCacheService#UNKNOWN_TIMESTAMP} or a non-negative epoch millis value.
     * Does not return {@link SharedBlobCacheService#BACKFILL_IN_PROGRESS_TIMESTAMP}.
     */
    public static long randomRegionTimestampMillis() {
        return randomBoolean() ? SharedBlobCacheService.UNKNOWN_TIMESTAMP : randomNonNegativeLong();
    }

    /**
     * Returns the number of free regions in the cache.
     */
    public static int freeRegionCount(SharedBlobCacheService<?> cacheService) {
        return cacheService.freeRegionCount();
    }

    /**
     * Ensures a cache region is present for the given key, file length, region index, and timestamp by calling
     * {@link SharedBlobCacheService#get(SharedBlobCacheService.KeyBase, long, int, long)}.
     *
     * @return the cache file region
     */
    public static <K extends SharedBlobCacheService.KeyBase> SharedBlobCacheService.CacheFileRegion<K> cacheRegion(
        SharedBlobCacheService<K> cacheService,
        K cacheKey,
        long fileLength,
        int region,
        long timestampMillis
    ) {
        return cacheService.get(cacheKey, fileLength, region, timestampMillis);
    }

    /**
     * Returns a map of access frequency to the number of cached regions matching the predicate.
     */
    public static <K extends SharedBlobCacheService.KeyBase> Map<Integer, Integer> countCachedRegionsByFreq(
        SharedBlobCacheService<K> cacheService,
        Predicate<K> predicate
    ) {
        return cacheService.countCachedRegionsByFreq(predicate);
    }

    /**
     * Returns a map of access frequency to the number of cached regions matching the predicate,
     * optionally including evicted regions.
     */
    public static <K extends SharedBlobCacheService.KeyBase> Map<Integer, Integer> countCachedRegionsByFreq(
        SharedBlobCacheService<K> cacheService,
        Predicate<K> predicate,
        boolean includeEvicted
    ) {
        return cacheService.countCachedRegionsByFreq(predicate, includeEvicted);
    }

    public static <K extends SharedBlobCacheService.KeyBase> EvictionPolicy<K> getEvictionPolicy(SharedBlobCacheService<K> cacheService) {
        return cacheService.getEvictionPolicy();
    }

    public static <K extends SharedBlobCacheService.KeyBase> boolean maybeEvictLeastUsed(
        SharedBlobCacheService<K> cacheService,
        K cacheKey,
        long length,
        int region
    ) {
        return cacheService.maybeEvictLeastUsed(cacheKey, length, region);
    }

    public static <K extends SharedBlobCacheService.KeyBase> void maybeScheduleDecayAndNewEpoch(SharedBlobCacheService<K> cacheService) {
        cacheService.maybeScheduleDecayAndNewEpoch();
    }
}
