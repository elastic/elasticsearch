/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * A registry that provides common cache invalidation services for caches that relies on the security index.
 */
public class CacheInvalidatorRegistry {

    private final Map<String, CacheInvalidator> cacheInvalidators = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> cacheAliases = new ConcurrentHashMap<>();

    public CacheInvalidatorRegistry() {
    }

    public void registerCacheInvalidator(String name, CacheInvalidator cacheInvalidator) {
        if (cacheInvalidators.containsKey(name)) {
            throw new IllegalArgumentException("Cache invalidator registry already has an entry with name: [" + name + "]");
        }
        cacheInvalidators.put(name, cacheInvalidator);
    }

    public void registerAlias(String alias, Set<String> names) {
        Objects.requireNonNull(alias, "cache alias cannot be null");
        if (names.isEmpty()) {
            throw new IllegalArgumentException("cache names cannot be empty for aliasing");
        }
        if (cacheAliases.containsKey(alias)) {
            throw new IllegalArgumentException("cache alias already exists: [" + alias + "]");
        }
        cacheAliases.put(alias, names);
    }

    public void validate() {
        for (String alias : cacheAliases.keySet()) {
            if (cacheInvalidators.containsKey(alias)) {
                throw new IllegalStateException("cache alias cannot clash with cache name: [" + alias + "]");
            }
            final Set<String> names = cacheAliases.get(alias);
            if (false == cacheInvalidators.keySet().containsAll(names)) {
                throw new IllegalStateException("cache names not found: ["
                    + Strings.collectionToCommaDelimitedString(Sets.difference(names, cacheInvalidators.keySet())) + "]");
            }
        }
    }

    public void onSecurityIndexStateChange(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (isMoveFromRedToNonRed(previousState, currentState)
            || isIndexDeleted(previousState, currentState)
            || Objects.equals(previousState.indexUUID, currentState.indexUUID) == false
            || previousState.isIndexUpToDate != currentState.isIndexUpToDate) {
            cacheInvalidators.values().stream()
                .filter(CacheInvalidator::shouldClearOnSecurityIndexStateChange).forEach(CacheInvalidator::invalidateAll);
        }
    }

    public void invalidateByKey(String cacheName, Collection<String> keys) {
        if (cacheAliases.containsKey(cacheName)) {
            cacheAliases.get(cacheName).forEach(name -> doInvalidateByKey(name, keys));
        } else {
            doInvalidateByKey(cacheName, keys);
        }
    }

    public void invalidateCache(String cacheName) {
        if (cacheAliases.containsKey(cacheName)) {
            cacheAliases.get(cacheName).forEach(this::doInvalidateCache);
        } else {
            doInvalidateCache(cacheName);
        }
    }

    private void doInvalidateByKey(String cacheName, Collection<String> keys) {
        final CacheInvalidator cacheInvalidator = cacheInvalidators.get(cacheName);
        if (cacheInvalidator != null) {
            cacheInvalidator.invalidate(keys);
        } else {
            throw new IllegalArgumentException("No cache named [" + cacheName + "] is found");
        }
    }

    private void doInvalidateCache(String cacheName) {
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

        default boolean shouldClearOnSecurityIndexStateChange() {
            return true;
        }
    }
}
