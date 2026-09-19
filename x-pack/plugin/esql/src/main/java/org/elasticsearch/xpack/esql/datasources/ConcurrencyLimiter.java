/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Objects;
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
    private final String scheme;
    private final ExternalSourceSettings.BlobStoreConcurrency concurrency;
    private final long acquireTimeoutMs;
    private final AtomicLong lastWarnLogTime = new AtomicLong(0);

    private static final long WARN_LOG_INTERVAL_MS = 30_000;
    private static final long WARN_WAIT_THRESHOLD_MS = 5_000;

    private ConcurrencyLimiter(int maxPermits, long acquireTimeoutMs) {
        if (maxPermits != 0) {
            throw new IllegalArgumentException("maxPermits must be 0");
        }
        this.scheme = null;
        this.concurrency = null;
        this.acquireTimeoutMs = acquireTimeoutMs;
        this.semaphore = null;
    }

    ConcurrencyLimiter(String scheme, ExternalSourceSettings.BlobStoreConcurrency concurrency) {
        this(scheme, concurrency, 60_000L);
    }

    ConcurrencyLimiter(String scheme, ExternalSourceSettings.BlobStoreConcurrency concurrency, long acquireTimeoutMs) {
        if (Strings.isNullOrEmpty(scheme)) {
            throw new IllegalArgumentException("Scheme cannot be null or empty");
        }
        Objects.requireNonNull(concurrency, "concurrency");
        if (concurrency.permits() <= 0) {
            throw new IllegalArgumentException("permits must be positive");
        }
        this.scheme = scheme;
        this.concurrency = concurrency;
        this.acquireTimeoutMs = acquireTimeoutMs;
        this.semaphore = new Semaphore(concurrency.permits(), true);
    }

    void acquire() throws TimeoutException, InterruptedException {
        if (semaphore == null) {
            return;
        }
        long startNanos = System.nanoTime();
        boolean acquired = semaphore.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS);
        if (acquired == false) {
            throw new TimeoutException(timeoutMessage());
        }
        long waitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (waitMs > WARN_WAIT_THRESHOLD_MS) {
            long lastWarn = lastWarnLogTime.get();
            long now = System.currentTimeMillis();
            if (now - lastWarn > WARN_LOG_INTERVAL_MS && lastWarnLogTime.compareAndSet(lastWarn, now)) {
                logger.warn("[{}] request waited [{}]ms for a concurrency permit (max permits [{}])", scheme, waitMs, maxPermits());
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
        return concurrency == null ? 0 : concurrency.permits();
    }

    String scheme() {
        return scheme;
    }

    boolean settingCanRaiseLimit() {
        return concurrency != null && concurrency.settingCanRaiseLimit();
    }

    int availablePermits() {
        return semaphore != null ? semaphore.availablePermits() : Integer.MAX_VALUE;
    }

    private String timeoutMessage() {
        String key = ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.getKey();
        if (concurrency.settingCanRaiseLimit()) {
            return Strings.format(
                "Timed out waiting for a concurrency permit for [%s] after [%s]ms (max permits [%s]). "
                    + "Raise [%s] in the node's configuration and restart the node to increase the limit.",
                scheme,
                acquireTimeoutMs,
                maxPermits(),
                key
            );
        }
        return Strings.format(
            "Timed out waiting for a concurrency permit for [%s] after [%s]ms (max permits [%s]). "
                + "[%s] cannot raise this node's limit.",
            scheme,
            acquireTimeoutMs,
            maxPermits(),
            key
        );
    }
}
