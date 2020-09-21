/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
 * A wrapper of {@link Cache} that minimizes the possibility of caching stale results.
 */
public class ConsistentCache<K, V> {

    private static final Logger logger = LogManager.getLogger(ConsistentCache.class);

    private final Cache<K, V> delegate;
    private final AtomicLong numInvalidation = new AtomicLong();
    private final ReadWriteLock invalidationLock = new ReentrantReadWriteLock();
    private final ReleasableLock invalidationReadLock = new ReleasableLock(invalidationLock.readLock());
    private final ReleasableLock invalidationWriteLock = new ReleasableLock(invalidationLock.writeLock());

    public ConsistentCache(Cache<K, V> delegate) {
        this.delegate = delegate;
    }

    public Checkpoint<K, V> checkpoint() {
        final long invalidationCounter = numInvalidation.get();
        return (key, value) -> {
            try (ReleasableLock ignored = invalidationReadLock.acquire()) {
                if (invalidationCounter == numInvalidation.get()) {
                    logger.debug("Caching for key [{}], value [{}]", key, value);
                    delegate.put(key, value);
                    return true;
                }
            }
            return false;
        };
    }

    public V get(K key) {
        return delegate.get(key);
    }

    // If there are secondary caches that must be invalidated when the main
    // entry is invalidated, they can be registered via the removalListener
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

    public interface Checkpoint<K, V> {
        boolean put(K key, V value);
    }

}
