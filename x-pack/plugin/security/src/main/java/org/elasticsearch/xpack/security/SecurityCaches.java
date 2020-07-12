/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

public class SecurityCaches {

    private static final Map<String, Tuple<Consumer<Collection<String>>, Runnable>> cacheInvalidationCallbacks = new ConcurrentHashMap<>();

    public static <V> SecurityCache<V> newSecurityCache(String name, Cache<String, V> delegate) {
        return newSecurityCache(name, delegate, null);
    }

    public static <V> SecurityCache<V> newSecurityCache(
        String name,
        Cache<String, V> delegate,
        CacheInvalidator extraCacheInvalidator) {
        if (cacheInvalidationCallbacks.containsKey(name)) {
            // throw new IllegalArgumentException("Security cache of name [" + name + "] already exists");
        }
        final SecurityCache<V> securityCache = new SecurityCache<>(name, delegate, extraCacheInvalidator);
        cacheInvalidationCallbacks.put(name, new Tuple<>(securityCache::invalidate, securityCache::invalidateAll));
        return securityCache;
    }

    public static void onSecurityIndexStageChange(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (isMoveFromRedToNonRed(previousState, currentState) || isIndexDeleted(previousState,
            currentState) || previousState.isIndexUpToDate != currentState.isIndexUpToDate) {
            cacheInvalidationCallbacks.values().stream().map(Tuple::v2).forEach(Runnable::run);
        }
    }

    // TODO: The static invalidation methods can be used to implement APIs for clearing named security caches:
    //       e.g. /_security/_cache/{name}/_clear_cache
    public static void invalidate(String name, Collection<String> keys) {
        final Consumer<Collection<String>> consumer = cacheInvalidationCallbacks.get(name).v1();
        if (consumer != null) {
            consumer.accept(keys);
        }
    }

    public static void invalidateAll(String name) {
        final Runnable callback = cacheInvalidationCallbacks.get(name).v2();
        if (callback != null) {
            callback.run();
        }
    }

    public static class SecurityCache<V> {

        private static final Logger logger = LogManager.getLogger(SecurityCache.class);

        private final String name;
        private final Cache<String, V> delegate;
        private final CacheInvalidator extraCacheInvalidator;
        private final AtomicLong numInvalidation = new AtomicLong();
        private final ReadWriteLock invalidationLock = new ReentrantReadWriteLock();
        private final ReleasableLock invalidationReadLock = new ReleasableLock(invalidationLock.readLock());
        private final ReleasableLock invalidationWriteLock = new ReleasableLock(invalidationLock.writeLock());

        private SecurityCache(String name, Cache<String, V> delegate, CacheInvalidator extraCacheInvalidator) {
            this.name = name;
            this.delegate = delegate;
            this.extraCacheInvalidator = extraCacheInvalidator;
        }

        public CacheItemsConsumer<V> preparePut() {
            final long invalidationCounter = numInvalidation.get();
            return (key, value, extraCachingRunnable) -> {
                try (ReleasableLock ignored = invalidationReadLock.acquire()) {
                    if (invalidationCounter == numInvalidation.get()) {
                        logger.debug("Cache: [{}] - caching key [{}]", name, key);
                        delegate.put(key, value);
                        if (extraCachingRunnable != null) {
                            try {
                                extraCachingRunnable.run();
                            } catch (Exception e) {
                                logger.error("Failed to cache extra item for cache [" + name + "]", e);
                            }
                        }
                    }
                }
            };
        }

        public V get(String key) {
            return delegate.get(key);
        }

        public void invalidate(Collection<String> keys) {
            try (ReleasableLock ignored = invalidationWriteLock.acquire()) {
                numInvalidation.incrementAndGet();
            }
            logger.debug("Cache: [{}] - invalidating [{}]", name, keys);
            keys.forEach(delegate::invalidate);
            if (extraCacheInvalidator != null) {
                extraCacheInvalidator.invalidate(keys);
            }
        }

        public void invalidateAll() {
            try (ReleasableLock ignored = invalidationWriteLock.acquire()) {
                numInvalidation.incrementAndGet();
            }
            logger.debug("Cache: [{}] - invalidating all", name);
            delegate.invalidateAll();
            if (extraCacheInvalidator != null) {
                extraCacheInvalidator.invalidate(null);
            }
        }
    }

    public interface CacheItemsConsumer<V> {
        void consume(String key, V value, CheckedRunnable<Exception> extraCachingRunnable);

        default void consume(String key, V value) {
            consume(key, value, null);
        }
    }

    public interface CacheInvalidator {
        void invalidate(Collection<String> keys);
    }
}
