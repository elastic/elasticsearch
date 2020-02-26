/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;


import org.elasticsearch.common.lease.Releasable;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages locks. Locks can be accessed with an identifier and are
 * created the first time they are acquired and removed if no thread hold the
 * lock. The latter is important to assure that the list of locks does not grow
 * infinitely.
 * Note: this lock is reentrant
 *
 * */
public final class KeyedSemaphore<T> {

    private final ConcurrentMap<T, KeySemaphore> map = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();
    private final boolean fair;

    /**
     * Creates a new lock
     * @param fair Use fair locking, ie threads get the lock in the order they requested it
     */
    public KeyedSemaphore(boolean fair) {
        this.fair = fair;
    }

    /**
     * Creates a non-fair lock
     */
    public KeyedSemaphore() {
        this(false);
    }

    /**
     * Acquires a lock for the given key. The key is compared by it's equals method not by object identity. The lock can be acquired
     * by the same thread multiple times. The lock is released by closing the returned {@link Releasable}.
     */
    public Releasable acquire(T key) {
        while (true) {
            KeySemaphore perNodeLock = map.get(key);
            if (perNodeLock == null) {
                ReleasableLock newLock = tryCreateNewLock(key);
                if (newLock != null) {
                    return newLock;
                }
            } else {
                assert perNodeLock != null;
                int i = perNodeLock.count.get();
                if (i > 0 && perNodeLock.count.compareAndSet(i, i + 1)) {
                    perNodeLock.acquireUninterruptibly();
                    return new ReleasableLock(key, perNodeLock);
                }
            }
        }
    }

    /**
     * Tries to acquire the lock for the given key and returns it. If the lock can't be acquired null is returned.
     */
    public Releasable tryAcquire(T key) {
        final KeySemaphore perNodeLock = map.get(key);
        if (perNodeLock == null) {
            return tryCreateNewLock(key);
        }
        if (perNodeLock.tryAcquire()) { // ok we got it - make sure we increment it accordingly otherwise release it again
            int i;
            while ((i = perNodeLock.count.get()) > 0) {
                // we have to do this in a loop here since even if the count is > 0
                // there could be a concurrent blocking acquire that changes the count and then this CAS fails. Since we already got
                // the lock we should retry and see if we can still get it or if the count is 0. If that is the case and we give up.
                if (perNodeLock.count.compareAndSet(i, i + 1)) {
                    return new ReleasableLock(key, perNodeLock);
                }
            }
            perNodeLock.release(); // make sure we unlock and don't leave the lock in a locked state
        }
        return null;
    }

    private ReleasableLock tryCreateNewLock(T key) {
        KeySemaphore newLock = new KeySemaphore(fair);
        newLock.acquireUninterruptibly();
        KeySemaphore keySemaphore = map.putIfAbsent(key, newLock);
        if (keySemaphore == null) {
            return new ReleasableLock(key, newLock);
        }
        return null;
    }

    private void release(T key, KeySemaphore lock) {
        assert lock == map.get(key);
        final int decrementAndGet = lock.count.decrementAndGet();
        lock.release();
        if (decrementAndGet == 0) {
            map.remove(key, lock);
        }
        assert decrementAndGet >= 0 : decrementAndGet + " must be >= 0 but wasn't";
    }


    private final class ReleasableLock implements Releasable {
        final T key;
        final KeySemaphore semaphore;
        final AtomicBoolean closed = new AtomicBoolean();

        private ReleasableLock(T key, KeySemaphore semaphore) {
            this.key = key;
            this.semaphore = semaphore;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                release(key, semaphore);
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class KeySemaphore extends Semaphore {
        KeySemaphore(boolean fair) {
            super(1, fair);
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
