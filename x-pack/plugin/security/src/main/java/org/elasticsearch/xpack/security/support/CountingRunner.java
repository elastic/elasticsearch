/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An utility class that keeps a counter when executing runnables.
 * It is designed to help minimizing the possibility of caching stable results in a {@link Cache}.
 */
public class CountingRunner {

    private static final Logger logger = LogManager.getLogger(CountingRunner.class);

    private final AtomicLong counter = new AtomicLong();
    private final ReadWriteLock countingLock = new ReentrantReadWriteLock();
    private final ReleasableLock countingReadLock = new ReleasableLock(countingLock.readLock());
    private final ReleasableLock countingWriteLock = new ReleasableLock(countingLock.writeLock());

    public long getCount() {
        return counter.get();
    }

    /**
     * Execute the given runnable if the internal counter matches the given count.
     * The counter check is performed inside a read-locking block to prevent incrementing of
     * the counter, i.e. call to {@link CountingRunner#incrementAndRun} will be blocked.
     * But it does *not* block other invocations of {@link CountingRunner#runIfCountMatches}.
     *
     * @return true if the runnable is executed, other false.
     */
    public boolean runIfCountMatches(Runnable runnable, long count) {
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
     * Increment the internal counter before executing the given runnable.
     * The counter is incremented in a write-locking block so that no other runnable
     * can be executed by methods of the same manager when the counter is being incremented.
     * @param runnable
     */
    public void incrementAndRun(Runnable runnable) {
        try (ReleasableLock ignored = countingWriteLock.acquire()) {
            counter.incrementAndGet();
        }
        runnable.run();
    }
}
