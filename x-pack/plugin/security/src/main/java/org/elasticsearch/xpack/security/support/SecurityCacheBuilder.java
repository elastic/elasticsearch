/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToLongBiFunction;

public class SecurityCacheBuilder<K1, V1, R> {

    private final CacheBuilder<K1, V1> delegateCacheBuilder;
    private Function<R, V1> valueFunc;

    private SecurityCacheBuilder() {
        delegateCacheBuilder = CacheBuilder.builder();
    }

    public static <K1, V1, R> SecurityCacheBuilder<K1, V1, R> builder() {
        return new SecurityCacheBuilder<>();
    }

    public SecurityCacheBuilder<K1, V1, R> setExpireAfterAccess(TimeValue expireAfterAccess) {
        delegateCacheBuilder.setExpireAfterAccess(expireAfterAccess);
        return this;
    }

    public SecurityCacheBuilder<K1, V1, R> setExpireAfterWrite(TimeValue expireAfterWrite) {
        delegateCacheBuilder.setExpireAfterWrite(expireAfterWrite);
        return this;
    }

    public SecurityCacheBuilder<K1, V1, R> setMaximumWeight(long maximumWeight) {
        delegateCacheBuilder.setMaximumWeight(maximumWeight);
        return this;
    }

    public SecurityCacheBuilder<K1, V1, R> weigher(ToLongBiFunction<K1, V1> weigher) {
        delegateCacheBuilder.weigher(weigher);
        return this;
    }

    public SecurityCacheBuilder<K1, V1, R> removalListener(RemovalListener<K1, V1> removalListener) {
        delegateCacheBuilder.removalListener(removalListener);
        return this;
    }

    public SecurityCacheBuilder<K1, V1, R> valueFunc(Function<R, V1> primaryValueFunc) {
        this.valueFunc = primaryValueFunc;
        return this;
    }

    public SecurityCache<K1, V1, R> build() {
        final Cache<K1, V1> delegate = delegateCacheBuilder.build();
        return new SecurityCache<>(delegate, Objects.requireNonNull(valueFunc));
    }

    public static class SecurityCache<K1, V1, R> {

        private static final Logger logger = LogManager.getLogger(SecurityCache.class);

        private final Cache<K1, V1> delegate;
        private final Function<R, V1> valueFunc;
        private final AtomicLong numInvalidation = new AtomicLong();
        private final ReadWriteLock invalidationLock = new ReentrantReadWriteLock();
        private final ReleasableLock invalidationReadLock = new ReleasableLock(invalidationLock.readLock());
        private final ReleasableLock invalidationWriteLock = new ReleasableLock(invalidationLock.writeLock());

        private SecurityCache(Cache<K1, V1> delegate, Function<R, V1> valueFunc) {
            this.delegate = delegate;
            this.valueFunc = valueFunc;
        }

        public BiConsumer<K1, R> prepareCacheItemConsumer(BiConsumer<K1, R> secondaryConsumer) {
            final long invalidationCounter = numInvalidation.get();
            return (key, response) -> {
                try (ReleasableLock ignored = invalidationReadLock.acquire()) {
                    if (invalidationCounter == numInvalidation.get()) {
                        logger.debug("Caching for key [{}]", key);
                        final V1 value = valueFunc.apply(response);
                        delegate.put(key, value);
                        secondaryConsumer.accept(key, response);
                    }
                }
            };
        }

        public V1 get(K1 key) {
            return delegate.get(key);
        }

        public void invalidate(Collection<K1> keys) {
            try (ReleasableLock ignored = invalidationWriteLock.acquire()) {
                numInvalidation.incrementAndGet();
            }
            logger.debug("Invalidating for keys [{}]", keys);
            keys.forEach(delegate::invalidate);
            // Secondary cache is invalidated via removalListener
        }

        public void invalidateAll() {
            try (ReleasableLock ignored = invalidationWriteLock.acquire()) {
                numInvalidation.incrementAndGet();
            }
            logger.debug("Invalidating all cache entries");
            delegate.invalidateAll();
        }

    }

}
