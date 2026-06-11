/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-query concurrency budget that limits the number of concurrent in-flight storage API requests
 * for a single query. Budgets are dynamically resizable: the {@link ConcurrencyBudgetAllocator}
 * adjusts each budget's max permits as queries start and finish to maintain fair-share allocation.
 */
class QueryConcurrencyBudget implements Closeable {

    private static final Logger logger = LogManager.getLogger(QueryConcurrencyBudget.class);

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition permitAvailable = lock.newCondition();
    private int inFlight;
    private volatile int maxPermits;
    private final long acquireTimeoutMs;
    private final ConcurrencyBudgetAllocator allocator;
    private volatile boolean closed;

    /**
     * Timestamp of the last permit release (or budget increase), used by {@link #acquire()} to tell
     * a healthy-but-slowly-draining pool apart from a genuinely stalled one. A waiter whose personal
     * timeout expires keeps waiting as long as the pool released a permit within the last timeout
     * window; the timeout only fires when no permit moved for a full window (a leak or a stall).
     */
    private volatile long lastReleaseNanos;

    private final AtomicLong lastWarnLogTime = new AtomicLong(0);
    private static final long WARN_LOG_INTERVAL_MS = 30_000;
    private static final long WARN_WAIT_THRESHOLD_MS = 5_000;

    // Shared singleton for the disabled/unlimited case. Because acquire() short-circuits on
    // maxPermits <= 0, none of the mutable state (lock, condition, closed) is ever exercised.
    // Closing this instance is harmless (and must remain so).
    static final QueryConcurrencyBudget UNLIMITED = new QueryConcurrencyBudget(0, 60_000L, null);

    QueryConcurrencyBudget(int maxPermits, long acquireTimeoutMs, ConcurrencyBudgetAllocator allocator) {
        this.maxPermits = maxPermits;
        this.acquireTimeoutMs = acquireTimeoutMs;
        this.allocator = allocator;
        this.lastReleaseNanos = System.nanoTime();
    }

    /**
     * Acquires a permit, blocking if the query is at its budget limit. Throws immediately if the
     * budget has been closed.
     * <p>
     * The acquire timeout is progress-aware rather than a hard per-waiter deadline: a fixed personal
     * timeout turns a structurally over-subscribed but healthy pool (more demand than permits, with
     * permits held across long read+parse dwells) into a failure cliff once FIFO queue depth times
     * average dwell exceeds the timeout. So when a waiter's personal deadline expires but the pool
     * released a permit within the last timeout window, the deadline is extended to one window past
     * that release and the waiter keeps queueing. The timeout only fires when no permit was released
     * for a full timeout window — preserving detection of genuine leaks and stalls.
     */
    void acquire() throws TimeoutException, InterruptedException {
        if (maxPermits <= 0) {
            return;
        }
        if (closed) {
            throw new TimeoutException("Budget is closed");
        }
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMs);
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + timeoutNanos;
        lock.lock();
        try {
            while (inFlight >= maxPermits) {
                if (closed) {
                    throw new TimeoutException("Budget was closed while waiting for permit");
                }
                long waitNanos = deadlineNanos - System.nanoTime();
                if (waitNanos <= 0) {
                    long lastRelease = lastReleaseNanos;
                    long sinceLastReleaseNanos = System.nanoTime() - lastRelease;
                    if (sinceLastReleaseNanos < timeoutNanos) {
                        // The pool made progress within one timeout window: keep waiting, with the
                        // deadline pushed to one window past the last observed release.
                        deadlineNanos = lastRelease + timeoutNanos;
                        continue;
                    }
                    throw new TimeoutException(
                        "Timed out waiting for query concurrency budget permit after ["
                            + acquireTimeoutMs
                            + "]ms (max permits ["
                            + maxPermits
                            + "]), no permit released in the last ["
                            + TimeUnit.NANOSECONDS.toMillis(sinceLastReleaseNanos)
                            + "]ms"
                    );
                }
                permitAvailable.awaitNanos(waitNanos);
            }
            if (closed) {
                throw new TimeoutException("Budget was closed while waiting for permit");
            }
            inFlight++;
        } finally {
            lock.unlock();
        }
        long waitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (waitMs > WARN_WAIT_THRESHOLD_MS) {
            long lastWarn = lastWarnLogTime.get();
            long now = System.currentTimeMillis();
            if (now - lastWarn > WARN_LOG_INTERVAL_MS && lastWarnLogTime.compareAndSet(lastWarn, now)) {
                logger.warn(
                    "per-query storage API request waited [{}]ms for concurrency budget permit (max permits [{}])",
                    waitMs,
                    maxPermits
                );
            }
        }
    }

    /**
     * Releases a permit, waking one blocked acquirer. Must be paired with a preceding
     * successful {@link #acquire()}.
     */
    void release() {
        if (maxPermits <= 0) {
            return;
        }
        lock.lock();
        try {
            assert inFlight > 0 : "release() called without a matching acquire(), inFlight=" + inFlight;
            if (inFlight > 0) {
                inFlight--;
            }
            lastReleaseNanos = System.nanoTime();
            permitAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Dynamically adjusts the maximum permits. Only signals blocked acquirers when the budget
     * increases to avoid thundering herd on shrink.
     */
    void updateMaxPermits(int newMax) {
        int old = maxPermits;
        maxPermits = newMax;
        if (newMax > old) {
            lock.lock();
            try {
                // A budget increase is progress for waiters, same as a release.
                lastReleaseNanos = System.nanoTime();
                permitAvailable.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    int inFlight() {
        lock.lock();
        try {
            return inFlight;
        } finally {
            lock.unlock();
        }
    }

    int maxPermits() {
        return maxPermits;
    }

    boolean isClosed() {
        return closed;
    }

    boolean isEnabled() {
        return maxPermits > 0;
    }

    /**
     * Closes the budget, unblocking any waiting acquirers and deregistering from the allocator.
     */
    @Override
    public void close() {
        closed = true;
        lock.lock();
        try {
            permitAvailable.signalAll();
        } finally {
            lock.unlock();
        }
        if (allocator != null) {
            allocator.deregister(this);
        }
    }
}
