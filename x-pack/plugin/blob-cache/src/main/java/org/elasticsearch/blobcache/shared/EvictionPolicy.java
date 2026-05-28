/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

/**
 * Pluggable eviction strategy for {@link SharedBlobCacheService}.
 * <p>
 * The cache service iterates entries from lowest to highest frequency and consults this policy
 * to decide whether each entry is eligible for eviction. Implementations may skip entries that
 * should be protected.
 * <p>
 * Currently all methods are called under the cache service's monitor lock, so implementations
 * observe a fully serialized view of cache mutations. However, implementations should not depend
 * on this guarantee: it may be relaxed in the future and policies should be thread-safe and
 * remain correct under concurrent calls.
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
     * A return value of {@code false} does not guarantee the region will never be evicted: forced
     * eviction (e.g., shard closure or index deletion) bypasses this policy entirely.
     * <p>
     * This method must not perform I/O.
     *
     * @param region   the existing cache region being considered for eviction
     * @param incoming the new cache region that needs a slot; eviction of {@code region} would free
     *                 space for this entry
     */
    boolean canEvict(CacheRegion<KeyType> region, CacheRegion<KeyType> incoming);

    /**
     * Called when a region is assigned a cache slot (after successful allocation or eviction+take).
     * Allows the policy to update its internal tracking if needed.
     * <p>
     * This method must not perform I/O. The method is called after the region and its associated key
     * have both been added to the cache.
     */
    void onCached(CacheRegion<KeyType> region);

    /**
     * Called when a region is evicted from the cache.
     * Allows the policy to update its internal tracking if needed.
     * <p>
     * This method must not perform I/O. The method is called after the region and its associated key
     * have both been removed from the cache.
     */
    void onEvicted(CacheRegion<KeyType> region);
}
