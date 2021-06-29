/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A wrapper of {@link Cache} that keeps a counter for invalidation calls in order to
 * minimizes the possibility of caching stale results.
 */
public class InvalidationCountingCacheWrapper<K, V> {

    private static final Logger logger = LogManager.getLogger(InvalidationCountingCacheWrapper.class);

    private final Cache<K, V> delegate;
    private final AtomicLong numInvalidation = new AtomicLong();
    private final ReadWriteLock invalidationLock = new ReentrantReadWriteLock();
    private final ReleasableLock invalidationReadLock = new ReleasableLock(invalidationLock.readLock());
    private final ReleasableLock invalidationWriteLock = new ReleasableLock(invalidationLock.writeLock());

    public InvalidationCountingCacheWrapper(Cache<K, V> delegate) {
        this.delegate = delegate;
    }

    public long getInvalidationCount() {
        return numInvalidation.get();
    }

    public boolean putIfNoInvalidationSince(K key, V value, long invalidationCount) {
        assert invalidationCount >= 0 : "Invalidation count must be non-negative";
        try (ReleasableLock ignored = invalidationReadLock.acquire()) {
            if (invalidationCount == numInvalidation.get()) {
                logger.debug("Caching for key [{}], value [{}]", key, value);
                delegate.put(key, value);
                return true;
            }
        }
        return false;
    }

    public V get(K key) {
        return delegate.get(key);
    }

    public void invalidate(Collection<K> keys) {
        try (ReleasableLock ignored = invalidationWriteLock.acquire()) {
            numInvalidation.incrementAndGet();
        }
        logger.debug("Invalidating for keys [{}]", keys);
        keys.forEach(delegate::invalidate);
    }

    public void invalidateAll() {
        try (ReleasableLock ignored = invalidationWriteLock.acquire()) {
            numInvalidation.incrementAndGet();
        }
        logger.debug("Invalidating all cache entries");
        delegate.invalidateAll();
    }

    public int count() {
        return delegate.count();
    }
}
