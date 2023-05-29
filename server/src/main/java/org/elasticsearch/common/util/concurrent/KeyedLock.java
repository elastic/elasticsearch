/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

/**
 * This class manages locks. Locks can be accessed with an identifier and are
 * created the first time they are acquired and removed if no thread hold the
 * lock. The latter is important to assure that the list of locks does not grow
 * infinitely.
 * Note: this lock is reentrant
 *
 * */
public final class KeyedLock<T> {

    private final ConcurrentMap<T, KeyLock> map = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    /**
     * Acquires a lock for the given key. The key is compared by it's equals method not by object identity. The lock can be acquired
     * by the same thread multiple times. The lock is released by closing the returned {@link Releasable}.
     */
    public Releasable acquire(T key) {
        KeyLock perNodeLock = map.compute(key, computeLock());
        perNodeLock.lock();
        return releasableLock(key, perNodeLock);
    }

    /**
     * Tries to acquire the lock for the given key and returns it. If the lock can't be acquired null is returned.
     */
    public Releasable tryAcquire(T key) {
        KeyLock perNodeLock = map.compute(key, computeLock());
        if (perNodeLock.tryLock()) {
            return releasableLock(key, perNodeLock);
        }
        // failed to get the lock, but we incremented its count when acquiring it above, so we decrement the count and potentially remove
        // it from the map
        if (perNodeLock.count.decrementAndGet() == 0) {
            map.remove(key, perNodeLock);
        }
        return null;
    }

    private static <S> BiFunction<S, KeyLock, KeyLock> computeLock() {
        // duplicate lambdas a little to save capturing lambda instantiation from capturing the 'fair' flag
        return (k, existing) -> existing != null && existing.count.updateAndGet(i -> i == 0 ? 0 : i + 1) > 0 ? existing : new KeyLock();
    }

    /**
     * Returns <code>true</code> iff the caller thread holds the lock for the given key
     */
    public boolean isHeldByCurrentThread(T key) {
        KeyLock lock = map.get(key);
        if (lock == null) {
            return false;
        }
        return lock.isHeldByCurrentThread();
    }

    private void release(T key, KeyLock lock) {
        assert lock == map.get(key);
        final int decrementAndGet = lock.count.decrementAndGet();
        lock.unlock();
        if (decrementAndGet == 0) {
            map.remove(key, lock);
        }
        assert decrementAndGet >= 0 : decrementAndGet + " must be >= 0 but wasn't";
    }

    private Releasable releasableLock(T key, KeyLock lock) {
        return Releasables.releaseOnce(() -> release(key, lock));
    }

    @SuppressWarnings("serial")
    private static final class KeyLock extends ReentrantLock {
        KeyLock() {
            super();
        }

        private final AtomicInteger count = new AtomicInteger(1);
    }

    /**
     * Returns <code>true</code> if this lock has at least one locked key.
     */
    public boolean hasLockedKeys() {
        return map.isEmpty() == false;
    }

}
