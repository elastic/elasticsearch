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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounds the number of concurrent in-flight storage reads per node, per scheme, as a deliberate
 * local-resource guardrail — not a throttle defense (the object store signals overload via 503 and
 * we react with backoff; this protects our own resources and backends that cannot signal backpressure,
 * such as the local filesystem). Uses a fair {@link Semaphore} so waiters are served in order.
 * <p>
 * {@link #acquire()} <em>blocks</em> until a permit frees rather than failing on a deadline: a
 * self-imposed limit must make work wait its turn, never kill a query for being at the limit. It stays
 * interruptible so query cancellation still unblocks a waiter. Permits are per-call, each acquirer holds
 * exactly one and releases it before taking the next, so blocking cannot deadlock as long as the limit is
 * positive. A permits value of 0 disables limiting entirely (all operations pass through).
 * <p>
 * Thread-safe and designed to be shared across all queries targeting the same storage scheme.
 */
class ConcurrencyLimiter {

    private static final Logger logger = LogManager.getLogger(ConcurrencyLimiter.class);

    static final ConcurrencyLimiter UNLIMITED = new ConcurrencyLimiter(0);

    private final Semaphore semaphore;
    private final int maxPermits;
    private final AtomicLong lastWarnLogTime = new AtomicLong(0);

    private static final long WARN_LOG_INTERVAL_MS = 30_000;
    private static final long WARN_WAIT_THRESHOLD_MS = 5_000;

    ConcurrencyLimiter(int maxPermits) {
        this.maxPermits = maxPermits;
        this.semaphore = maxPermits > 0 ? new Semaphore(maxPermits, true) : null;
    }

    void acquire() throws InterruptedException {
        if (semaphore == null) {
            return;
        }
        long startNanos = System.nanoTime();
        // Block until a permit frees. We never fail the read for waiting at our own self-imposed limit;
        // it queues. Interruptible, so query cancellation unblocks the waiter.
        semaphore.acquire();
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
