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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class manages locks. Locks can be accessed with an identifier and are
 * created the first time they are acquired and removed if no thread hold the
 * lock. The latter is important to assure that the list of locks does not grow
 * infinitely.
 *
 *
 * */
public class KeyedLock<T> {

    private final boolean fair;

    /**
     * @param fair Use fair locking, ie threads get the lock in the order they requested it
     */
    public KeyedLock(boolean fair) {
        this.fair = fair;
    }

    public KeyedLock() {
        this(false);
    }

    private final ConcurrentMap<T, KeyLock> map = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    public Releasable acquire(T key) {
        assert isHeldByCurrentThread(key) == false : "lock for " + key + " is already heald by this thread";
        while (true) {
            KeyLock perNodeLock = map.get(key);
            if (perNodeLock == null) {
                KeyLock newLock = new KeyLock(fair);
                perNodeLock = map.putIfAbsent(key, newLock);
                if (perNodeLock == null) {
                    newLock.lock();
                    return new ReleasableLock(key, newLock);
                }
            }
            assert perNodeLock != null;
            int i = perNodeLock.count.get();
            if (i > 0 && perNodeLock.count.compareAndSet(i, i + 1)) {
                perNodeLock.lock();
                return new ReleasableLock(key, perNodeLock);
            }
        }
    }

    public boolean isHeldByCurrentThread(T key) {
        KeyLock lock = map.get(key);
        if (lock == null) {
            return false;
        }
        return lock.isHeldByCurrentThread();
    }

    void release(T key, KeyLock lock) {
        assert lock == map.get(key);
        lock.unlock();
        int decrementAndGet = lock.count.decrementAndGet();
        if (decrementAndGet == 0) {
            map.remove(key, lock);
        }
    }


    private final class ReleasableLock implements Releasable {
        final T key;
        final KeyLock lock;
        final AtomicBoolean closed = new AtomicBoolean();

        private ReleasableLock(T key, KeyLock lock) {
            this.key = key;
            this.lock = lock;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                release(key, lock);
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class KeyLock extends ReentrantLock {
        KeyLock(boolean fair) {
            super(fair);
        }

        private final AtomicInteger count = new AtomicInteger(1);
    }

    public boolean hasLockedKeys() {
        return !map.isEmpty();
    }

}
