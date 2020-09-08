/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.support;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * A registry that provides common cache invalidation services for caches that relies on the security index.
 */
public class CacheInvalidatorRegistry {

    private static final Map<String, CacheInvalidator> cacheInvalidators = new ConcurrentHashMap<>();

    public static void registerCacheInvalidator(String name, CacheInvalidator cacheInvalidator) {
        // TODO: check no overriding entries
        cacheInvalidators.put(name, cacheInvalidator);
    }

    public static void onSecurityIndexStageChange(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (isMoveFromRedToNonRed(previousState, currentState)
            || isIndexDeleted(previousState, currentState)
            || previousState.isIndexUpToDate != currentState.isIndexUpToDate) {
            cacheInvalidators.values().forEach(CacheInvalidator::invalidateAll);
        }
    }

    public static void invalidateByKey(String cacheName, Collection<String> keys) {
        final CacheInvalidator cacheInvalidator = cacheInvalidators.get(cacheName);
        if (cacheInvalidator != null) {
            cacheInvalidator.invalidate(keys);
        }
    }

    public static void invalidateCache(String cacheName) {
        final CacheInvalidator cacheInvalidator = cacheInvalidators.get(cacheName);
        if (cacheInvalidator != null) {
            cacheInvalidator.invalidateAll();
        }
    }

    public interface CacheInvalidator {
        void invalidate(Collection<String> keys);

        void invalidateAll();
    }
}
