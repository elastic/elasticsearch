/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.core.Releasable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

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
        while (true) {
            KeyLock perNodeLock = map.get(key);
            if (perNodeLock == null) {
                ReleasableLock newLock = tryCreateNewLock(key);
                if (newLock != null) {
                    return newLock;
                }
            } else {
                int i = perNodeLock.count;
                if (i > 0 && perNodeLock.tryIncCount(i)) {
                    perNodeLock.lock();
                    return new ReleasableLock(key, perNodeLock);
                }
            }
        }
    }

    /**
     * Tries to acquire the lock for the given key and returns it. If the lock can't be acquired null is returned.
     */
    public Releasable tryAcquire(T key) {
        final KeyLock perNodeLock = map.get(key);
        if (perNodeLock == null) {
            return tryCreateNewLock(key);
        }
        if (perNodeLock.tryLock()) { // ok we got it - make sure we increment it accordingly otherwise release it again
            int i;
            while ((i = perNodeLock.count) > 0) {
                // we have to do this in a loop here since even if the count is > 0
                // there could be a concurrent blocking acquire that changes the count and then this CAS fails. Since we already got
                // the lock we should retry and see if we can still get it or if the count is 0. If that is the case and we give up.
                if (perNodeLock.tryIncCount(i)) {
                    return new ReleasableLock(key, perNodeLock);
                }
            }
            perNodeLock.unlock(); // make sure we unlock and don't leave the lock in a locked state
        }
        return null;
    }

    private ReleasableLock tryCreateNewLock(T key) {
        KeyLock newLock = new KeyLock();
        newLock.lock();
        KeyLock keyLock = map.putIfAbsent(key, newLock);
        if (keyLock == null) {
            return new ReleasableLock(key, newLock);
        }
        return null;
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
        final int decrementAndGet = lock.decCountAndGet();
        lock.unlock();
        if (decrementAndGet == 0) {
            map.remove(key, lock);
        }
        assert decrementAndGet >= 0 : decrementAndGet + " must be >= 0 but wasn't";
    }

    private final class ReleasableLock extends AtomicReference<T> implements Releasable {
        final KeyLock lock;

        private ReleasableLock(T key, KeyLock lock) {
            super(key);
            this.lock = lock;
        }

        @Override
        public void close() {
            T k = getAndSet(null);
            if (k != null) {
                release(k, lock);
            }
        }
    }

    private static final class KeyLock extends ReentrantLock {
        private static final VarHandle VH_COUNT_FIELD;

        static {
            try {
                VH_COUNT_FIELD = MethodHandles.lookup().in(KeyLock.class).findVarHandle(KeyLock.class, "count", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("FieldMayBeFinal") // updated via VH_COUNT_FIELD (and _only_ via VH_COUNT_FIELD)
        private volatile int count = 1;

        KeyLock() {
            super();
        }

        int decCountAndGet() {
            do {
                int i = count;
                int newCount = i - 1;
                if (VH_COUNT_FIELD.weakCompareAndSet(this, i, newCount)) {
                    return newCount;
                }
            } while (true);
        }

        boolean tryIncCount(int expectedCount) {
            return VH_COUNT_FIELD.compareAndSet(this, expectedCount, expectedCount + 1);
        }
    }

    /**
     * Returns <code>true</code> if this lock has at least one locked key.
     */
    public boolean hasLockedKeys() {
        return map.isEmpty() == false;
    }

}
