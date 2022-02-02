/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.util.Iterator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

/**
 * A utility class to facilitate iterating over (and modifying) a {@link org.elasticsearch.common.cache.Cache}.
 * The semantics of the cache are such that when iterating (with the potential to call {@link Iterator#remove()}), we must prevent any
 * other modifications.
 * This class provides the necessary methods to support this constraint in a clear manner.
 */
public class CacheIteratorHelper<K, V> {
    private final Cache<K, V> cache;
    private final ReleasableLock updateLock;
    private final ReleasableLock iteratorLock;

    public CacheIteratorHelper(Cache<K, V> cache) {
        this.cache = cache;
        final ReadWriteLock lock = new ReentrantReadWriteLock();
        // the lock is used in an odd manner; when iterating over the cache we cannot have modifiers other than deletes using the
        // iterator but when not iterating we can modify the cache without external locking. When making normal modifications to the cache
        // the read lock is obtained so that we can allow concurrent modifications; however when we need to iterate over the keys or values
        // of the cache the write lock must obtained to prevent any modifications.
        updateLock = new ReleasableLock(lock.readLock());
        iteratorLock = new ReleasableLock(lock.writeLock());
    }

    public ReleasableLock acquireUpdateLock() {
        return updateLock.acquire();
    }

    private ReleasableLock acquireForIterator() {
        return iteratorLock.acquire();
    }

    public void removeKeysIf(Predicate<K> removeIf) {
        // the cache cannot be modified while doing this operation per the terms of the cache iterator
        try (ReleasableLock ignored = this.acquireForIterator()) {
            Iterator<K> iterator = cache.keys().iterator();
            while (iterator.hasNext()) {
                K key = iterator.next();
                if (removeIf.test(key)) {
                    iterator.remove();
                }
            }
        }
    }
}
