/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

/**
 * Read-only view of a cache region entry, exposed to {@link EvictionPolicy} implementations.
 * <p>
 * This interface restricts the surface available to eviction policies to the minimum needed
 * for making eviction decisions, rather than exposing the full internal {@code CacheFileRegion}.
 *
 * @param <KeyType> the cache key type
 */
public interface CacheRegion<KeyType extends SharedBlobCacheService.KeyBase> {

    /**
     * Returns the key identifying the cache region.
     */
    KeyType key();

    /**
     * Returns the representative data timestamp (epoch millis) of the content in this region, or UNKNOWN_TIMESTAMP when unknown.
     */
    long timestampMillis();
}
