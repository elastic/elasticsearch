/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An utility class that keeps an internal counter to ensure given runnable is only executed
 * when the counter matches the expected value. It also ensures that the counter value
 * do not change while the runnable is executing. This is done by applying a RW lock when
 * reading and writing (increment) to the counter.
 */
public class LockingAtomicCounter {

    private static final Logger logger = LogManager.getLogger(LockingAtomicCounter.class);

    private final AtomicLong counter = new AtomicLong();
    private final ReadWriteLock countingLock = new ReentrantReadWriteLock();
    private final ReleasableLock countingReadLock = new ReleasableLock(countingLock.readLock());
    private final ReleasableLock countingWriteLock = new ReleasableLock(countingLock.writeLock());

    public long get() {
        return counter.get();
    }

    /**
     * Execute the given runnable if the internal counter matches the given count.
     * The counter check is performed inside a read-locking block to prevent the counter
     * value from changing (i.e. block the call to {@link LockingAtomicCounter#increment()}.
     * This method is in concept similar to {@link AtomicLong#compareAndSet}. Instead of
     * a single "setValue" operation, this method takes any Runnable and ensure that
     * the counter value do not change while the runnable is executing.
     *
     * Because it uses a readLock, it does *not* block other invocations of
     * {@link LockingAtomicCounter#compareAndRun}.
     *
     * @return true if the runnable is executed, other false.
     */
    public boolean compareAndRun(long count, Runnable runnable) {
        assert count >= 0 : "Count must be non-negative";
        try (ReleasableLock ignored = countingReadLock.acquire()) {
            if (count == counter.get()) {
                logger.debug("Count matches [{}], executing runnable", count);
                runnable.run();
                return true;
            }
        }
        return false;
    }

    /**
     * Increment the internal counter in the writeLock so it will be blocked if any invocation
     * of {@link LockingAtomicCounter#compareAndRun} is already underway.
     */
    public void increment() {
        try (ReleasableLock ignored = countingWriteLock.acquire()) {
            counter.incrementAndGet();
        }
    }
}
