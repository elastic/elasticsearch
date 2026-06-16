/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

/**
 * Test utilities for {@link SharedBlobCacheService} that expose package-private methods to other modules.
 */
public final class SharedBlobCacheServiceTestUtils {

    private SharedBlobCacheServiceTestUtils() {}

    /**
     * Returns the number of free regions in the cache.
     */
    public static int freeRegionCount(SharedBlobCacheService<?> cacheService) {
        return cacheService.freeRegionCount();
    }

    /**
     * Ensures a cache region is present for the given key, file length, and region index by calling
     * {@link SharedBlobCacheService#get(SharedBlobCacheService.KeyBase, long, int)}.
     */
    public static <K extends SharedBlobCacheService.KeyBase> void cacheRegion(
        SharedBlobCacheService<K> cacheService,
        K cacheKey,
        long fileLength,
        int region
    ) {
        cacheService.get(cacheKey, fileLength, region);
    }
}
