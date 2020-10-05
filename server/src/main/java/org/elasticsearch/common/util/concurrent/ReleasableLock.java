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

import org.elasticsearch.Assertions;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.EngineException;

import java.util.concurrent.locks.Lock;

/**
 * Releasable lock used inside of Engine implementations
 */
public class ReleasableLock implements Releasable {
    private final Lock lock;


    // a per-thread count indicating how many times the thread has entered the lock; only works if assertions are enabled
    private final ThreadLocal<Integer> holdingThreads;

    public ReleasableLock(Lock lock) {
        this.lock = lock;
        if (Assertions.ENABLED) {
            holdingThreads = new ThreadLocal<>();
        } else {
            holdingThreads = null;
        }
    }

    @Override
    public void close() {
        lock.unlock();
        assert removeCurrentThread();
    }


    public ReleasableLock acquire() throws EngineException {
        lock.lock();
        assert addCurrentThread();
        return this;
    }

    /**
     * Try acquiring lock, returning null if unable to acquire lock within timeout.
     */
    public ReleasableLock tryAcquire(TimeValue timeout) throws InterruptedException {
        boolean locked = lock.tryLock(timeout.duration(), timeout.timeUnit());
        if (locked) {
            assert addCurrentThread();
            return this;
        } else {
            return null;
        }
    }

    private boolean addCurrentThread() {
        final Integer current = holdingThreads.get();
        holdingThreads.set(current == null ? 1 : current + 1);
        return true;
    }

    private boolean removeCurrentThread() {
        final Integer count = holdingThreads.get();
        assert count != null && count > 0;
        if (count == 1) {
            holdingThreads.remove();
        } else {
            holdingThreads.set(count - 1);
        }
        return true;
    }

    public boolean isHeldByCurrentThread() {
        if (holdingThreads == null) {
            throw new UnsupportedOperationException("asserts must be enabled");
        }
        final Integer count = holdingThreads.get();
        return count != null && count > 0;
    }
}
