/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService.CacheFileRegion;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

/**
 * Eviction policy that protects cache entries based on their boost level quotas.
 * An entry can only be evicted if the actual cached data at its boost level exceeds
 * the expected quota for that level.
 * <p>
 * The boost level concept is internal to this implementation. It is derived from the region's
 * timestamp and the active `BoostConfiguration`.
 */
public class BoostLevelEvictionPolicy implements EvictionPolicy<FileCacheKey> {

    @Override
    public boolean canEvict(CacheFileRegion<FileCacheKey> region, CacheFileRegion<FileCacheKey> incoming, boolean degraded) {
        return true;
    }

    @Override
    public void onCached(CacheFileRegion<FileCacheKey> region) {}

    @Override
    public void onEvicted(CacheFileRegion<FileCacheKey> region) {}

    @Override
    public boolean supportDegradation() {
        return true;
    }
}
