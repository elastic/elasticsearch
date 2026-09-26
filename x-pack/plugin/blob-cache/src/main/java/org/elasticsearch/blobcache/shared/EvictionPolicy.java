/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.core.Releasable;

import java.util.function.Predicate;

/**
 * Pluggable eviction strategy for {@link SharedBlobCacheService}.
 * <p>
 * The cache service iterates entries from lowest to highest frequency and consults this policy
 * to decide whether each entry is eligible for eviction. Implementations may skip entries that
 * should be protected.
 * <p>
 * {@link #createPredicate}, {@link #onCached}, and {@link #onEvicted} are currently called under
 * the cache service's monitor lock, so those callbacks observe a fully serialized view of cache
 * mutations. {@link #isProtected} may be invoked from the periodic metrics task and is
 * <em>not</em> under the cache monitor. Implementations should not depend on the monitor-lock
 * guarantee for the other methods either: it may be relaxed in the future and policies should be
 * thread-safe and remain correct under concurrent calls.
 *
 * @param <KeyType> the cache key type
 */
public interface EvictionPolicy<KeyType extends SharedBlobCacheService.KeyBase> extends Releasable {

    /**
     * Creates a predicate that returns {@code true} if a region can be evicted to make room for {@code incoming}.
     * <p>
     * The predicate is created once per eviction scan and invoked for each candidate region. Implementations
     * can capture any required information needed for the scan when creating the predicate, rather
     * than recomputing it on every invocation.
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
     * @param incoming the new cache region that needs a slot; eviction of a cached region would free
     *                 space for this entry
     */
    Predicate<CacheRegion<KeyType>> createPredicate(CacheRegion<KeyType> incoming);

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

    /**
     * Returns {@code true} if {@code region} is currently protected from eviction by this policy based on its
     * own state (independent of any incoming region that might trigger an eviction scan).
     * <p>
     * Periodic metrics use this to count protected regions without requiring the policy to own gauge
     * instruments. The default implementation returns {@code false}. This method may be invoked outside
     * the cache monitor and must not perform I/O.
     */
    default boolean isProtected(CacheRegion<KeyType> region) {
        return false;
    }

    /**
     * Called when the policy is closed so that it has a chance to perform any cleanup if needed. This is needed
     * because policy can be dynamically configured at runtime so that the old policy must be closed.
     * This method must not perform I/O.
     */
    default void close() {}
}
