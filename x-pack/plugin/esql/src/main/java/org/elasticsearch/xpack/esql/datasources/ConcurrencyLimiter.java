/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Limits the number of concurrent in-flight cloud storage API requests per node.
 * Uses a fair {@link Semaphore} to prevent starvation under sustained load.
 * <p>
 * Thread-safe and designed to be shared across all queries targeting the same storage scheme.
 * A permits value of 0 disables limiting entirely (all operations pass through).
 */
class ConcurrencyLimiter {

    private static final Logger logger = LogManager.getLogger(ConcurrencyLimiter.class);

    static final ConcurrencyLimiter UNLIMITED = new ConcurrencyLimiter(0, 60_000L);

    private final Semaphore semaphore;
    private final int maxPermits;
    private final long acquireTimeoutMs;
    private final AtomicLong lastWarnLogTime = new AtomicLong(0);

    /**
     * Timestamp of the last permit release, used by {@link #acquire()} to tell a healthy-but-slowly-
     * draining pool apart from a genuinely stalled one (see {@link QueryConcurrencyBudget#acquire()}
     * for the rationale — this is the node-global layer of the same progress-aware timeout).
     */
    private volatile long lastReleaseNanos;

    private static final long WARN_LOG_INTERVAL_MS = 30_000;
    private static final long WARN_WAIT_THRESHOLD_MS = 5_000;

    ConcurrencyLimiter(int maxPermits, long acquireTimeoutMs) {
        this.maxPermits = maxPermits;
        this.acquireTimeoutMs = acquireTimeoutMs;
        this.semaphore = maxPermits > 0 ? new Semaphore(maxPermits, true) : null;
        this.lastReleaseNanos = System.nanoTime();
    }

    ConcurrencyLimiter(int maxPermits) {
        this(maxPermits, 60_000L);
    }

    void acquire() throws TimeoutException, InterruptedException {
        if (semaphore == null) {
            return;
        }
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMs);
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + timeoutNanos;
        while (true) {
            long waitNanos = deadlineNanos - System.nanoTime();
            if (waitNanos > 0 && semaphore.tryAcquire(waitNanos, TimeUnit.NANOSECONDS)) {
                break;
            }
            // Personal deadline expired without a permit. If the pool released a permit within the
            // last timeout window it is draining, just slower than one personal timeout: keep
            // waiting, with the deadline pushed to one window past the last observed release. Only
            // a pool with no release for a full window is genuinely stalled.
            long lastRelease = lastReleaseNanos;
            long sinceLastReleaseNanos = System.nanoTime() - lastRelease;
            if (sinceLastReleaseNanos < timeoutNanos) {
                deadlineNanos = lastRelease + timeoutNanos;
                continue;
            }
            throw new TimeoutException(
                "Timed out waiting for cloud API permit after ["
                    + acquireTimeoutMs
                    + "]ms (max permits ["
                    + maxPermits
                    + "]), no permit released in the last ["
                    + TimeUnit.NANOSECONDS.toMillis(sinceLastReleaseNanos)
                    + "]ms"
            );
        }
        long waitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (waitMs > WARN_WAIT_THRESHOLD_MS) {
            long lastWarn = lastWarnLogTime.get();
            long now = System.currentTimeMillis();
            if (now - lastWarn > WARN_LOG_INTERVAL_MS && lastWarnLogTime.compareAndSet(lastWarn, now)) {
                logger.warn("cloud API request waited [{}]ms for concurrency permit (max permits [{}])", waitMs, maxPermits);
            }
        }
    }

    void release() {
        if (semaphore != null) {
            lastReleaseNanos = System.nanoTime();
            semaphore.release();
        }
    }

    boolean isEnabled() {
        return semaphore != null;
    }

    int maxPermits() {
        return maxPermits;
    }

    int availablePermits() {
        return semaphore != null ? semaphore.availablePermits() : Integer.MAX_VALUE;
    }
}
