/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * A registry that provides common cache invalidation services for caches that relies on the security index.
 */
public class CacheInvalidatorRegistry {

    private final Map<String, CacheInvalidator> cacheInvalidators = new ConcurrentHashMap<>();

    public CacheInvalidatorRegistry() {
    }

    public void registerCacheInvalidator(String name, CacheInvalidator cacheInvalidator) {
        if (cacheInvalidators.containsKey(name)) {
            throw new IllegalArgumentException("Cache invalidator registry already has an entry with name: [" + name + "]");
        }
        cacheInvalidators.put(name, cacheInvalidator);
    }

    public void onSecurityIndexStageChange(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (isMoveFromRedToNonRed(previousState, currentState)
            || isIndexDeleted(previousState, currentState)
            || Objects.equals(previousState.indexUUID, currentState.indexUUID) == false
            || previousState.isIndexUpToDate != currentState.isIndexUpToDate) {
            cacheInvalidators.values().forEach(CacheInvalidator::invalidateAll);
        }
    }

    public void invalidateByKey(String cacheName, Collection<String> keys) {
        final CacheInvalidator cacheInvalidator = cacheInvalidators.get(cacheName);
        if (cacheInvalidator != null) {
            cacheInvalidator.invalidate(keys);
        } else {
            throw new IllegalArgumentException("No cache named [" + cacheName + "] is found");
        }
    }

    public void invalidateCache(String cacheName) {
        final CacheInvalidator cacheInvalidator = cacheInvalidators.get(cacheName);
        if (cacheInvalidator != null) {
            cacheInvalidator.invalidateAll();
        } else {
            throw new IllegalArgumentException("No cache named [" + cacheName + "] is found");
        }
    }

    public interface CacheInvalidator {
        void invalidate(Collection<String> keys);

        void invalidateAll();
    }
}
