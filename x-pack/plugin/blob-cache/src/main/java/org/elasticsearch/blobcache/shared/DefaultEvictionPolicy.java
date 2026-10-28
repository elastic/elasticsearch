/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.core.Predicates;

import java.util.function.Predicate;

/**
 * Default eviction policy where all entries are evictable.
 */
public class DefaultEvictionPolicy<KeyType extends SharedBlobCacheService.KeyBase> implements EvictionPolicy<KeyType> {

    @Override
    public Predicate<CacheRegion<KeyType>> createPredicate(CacheRegion<KeyType> incoming) {
        return Predicates.always();
    }

    @Override
    public void onCached(CacheRegion<KeyType> region) {}

    @Override
    public void onEvicted(CacheRegion<KeyType> region) {}
}
