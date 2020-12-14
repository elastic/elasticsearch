/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.cache.Cache;

import java.util.Collection;

/**
 * A wrapper of {@link Cache} that keeps a counter for invalidation calls in order to
 * minimizes the possibility of caching stale results.
 */
public class InvalidationCountingCacheWrapper<K, V> {

    private static final Logger logger = LogManager.getLogger(InvalidationCountingCacheWrapper.class);

    private final CountingRunner countingRunner;
    private final Cache<K, V> delegate;

    public InvalidationCountingCacheWrapper(Cache<K, V> delegate) {
        this.delegate = delegate;
        this.countingRunner = new CountingRunner();
    }

    public long getInvalidationCount() {
        return countingRunner.getCount();
    }

    public boolean putIfNoInvalidationSince(K key, V value, long invalidationCount) throws Exception {
        return countingRunner.runIfCountMatches(() -> {
            logger.debug("Caching for key [{}], value [{}]", key, value);
            delegate.put(key, value);
        }, invalidationCount);
    }

    public V get(K key) {
        return delegate.get(key);
    }

    public void invalidate(Collection<K> keys) throws Exception {
        countingRunner.incrementAndRun(() -> {
            logger.debug("Invalidating for keys [{}]", keys);
            keys.forEach(delegate::invalidate);
        });

    }

    public void invalidateAll() throws Exception {
        countingRunner.incrementAndRun(() -> {
            logger.debug("Invalidating all cache entries");
            delegate.invalidateAll();
        });
    }

    public int count() {
        return delegate.count();
    }
}
