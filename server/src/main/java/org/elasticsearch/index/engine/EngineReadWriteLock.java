/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Reentrant read/write lock used to guard engine changes in a shard.
 *
 * Implemented as a simple wrapper around a {@link ReentrantReadWriteLock} to make it easier to add/override methods in the future.
 */
public final class EngineReadWriteLock implements ReadWriteLock {

    private final ReentrantReadWriteLock lock;

    public EngineReadWriteLock() {
        this.lock = new ReentrantReadWriteLock();
    }

    @Override
    public Lock writeLock() {
        return lock.writeLock();
    }

    @Override
    public Lock readLock() {
        return lock.readLock();
    }

    /**
     * See {@link ReentrantReadWriteLock#isWriteLocked()}
     */
    public boolean isWriteLocked() {
        return lock.isWriteLocked();
    }

    /**
     * See {@link ReentrantReadWriteLock#isWriteLockedByCurrentThread()}
     */
    public boolean isWriteLockedByCurrentThread() {
        return lock.isWriteLockedByCurrentThread();
    }

    /**
     * Returns {@code true} if the number of read locks held by any thread is greater than zero.
     * This method is designed for use in monitoring system state, not for synchronization control.
     *
     * @return {@code true} if any thread holds a read lock and {@code false} otherwise
     */
    public boolean isReadLocked() {
        return lock.getReadLockCount() > 0;
    }

    /**
     * Returns {@code true} if the number of holds on the read lock by the current thread is greater than zero.
     * This method is designed for use in monitoring system state, not for synchronization control.
     *
     * @return {@code true} if the number of holds on the read lock by the current thread is greater than zero, {@code false} otherwise
     */
    public boolean isReadLockedByCurrentThread() {
        return lock.getReadHoldCount() > 0;
    }
}
