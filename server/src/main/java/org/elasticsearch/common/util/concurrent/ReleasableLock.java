/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.Assertions;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
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
     * Try acquiring lock, returning null if unable.
     */
    public ReleasableLock tryAcquire() {
        boolean locked = lock.tryLock();
        if (locked) {
            assert addCurrentThread();
            return this;
        } else {
            return null;
        }
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
