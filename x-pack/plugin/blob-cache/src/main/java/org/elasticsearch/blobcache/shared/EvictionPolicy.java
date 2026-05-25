/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService.CacheFileRegion;

/**
 * Pluggable eviction strategy for {@link SharedBlobCacheService}.
 * <p>
 * The cache service iterates entries from lowest to highest frequency and consults this policy
 * to decide whether each entry is eligible for eviction. Implementations may skip entries that
 * should be protected.
 *
 * @param <KeyType> the cache key type
 */
public interface EvictionPolicy<KeyType extends SharedBlobCacheService.KeyBase> {

    /**
     * Returns {@code true} if {@code region} can be evicted to make room for {@code incoming}.
     * <p>
     * A return value of {@code true} indicates the policy considers the region <em>eligible</em>
     * for eviction, but does not guarantee that eviction will succeed. The region may still be
     * retained if it is currently in use (e.g., held by an active writer or reader).
     * <p>
     * This method is called under the cache service's monitor lock and must not perform I/O.
     *
     * @param region   the existing cache region being considered for eviction
     * @param incoming the new cache region that needs a slot; eviction of {@code region} would free
     *                 space for this entry
     * @param degraded {@code true} when a prior pass with {@code degraded=false} found no evictable
     *                 entries but the cache still needs to free space. Implementations should relax
     *                 their protection criteria in this mode.
     */
    boolean canEvict(CacheFileRegion<KeyType> region, CacheFileRegion<KeyType> incoming, boolean degraded);

    /**
     * Called when a region is assigned a cache slot (after successful allocation or eviction+take).
     * Allows the policy to update its internal tracking if needed.
     */
    void onCached(CacheFileRegion<KeyType> region);

    /**
     * Called when a region is evicted from the cache.
     * Allows the policy to update its internal tracking if needed.
     * <p>
     * This method is called under the cache service's monitor lock and must not perform I/O, and after the region and associated key have
     * both been removed and unlinked in the cache.
     */
    void onEvicted(CacheFileRegion<KeyType> region);
}
