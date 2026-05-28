/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

/**
 * Default eviction policy where all entries are evictable.
 */
public class DefaultEvictionPolicy<KeyType extends SharedBlobCacheService.KeyBase> implements EvictionPolicy<KeyType> {

    @Override
    public boolean canEvict(CacheRegion<KeyType> region, CacheRegion<KeyType> incoming) {
        return true;
    }

    @Override
    public void onCached(CacheRegion<KeyType> region) {}

    @Override
    public void onEvicted(CacheRegion<KeyType> region) {}
}
