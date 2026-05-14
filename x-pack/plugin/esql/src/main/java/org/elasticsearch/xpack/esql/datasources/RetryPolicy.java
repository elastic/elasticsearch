/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * Retry policy with exponential backoff and jitter for transient storage failures.
 * Supports separate retry budgets for throttling errors (429/503/SlowDown) versus
 * other transient errors (connection reset, socket timeout).
 * <p>
 * Throttling errors are expected under high parallelism and are always transient,
 * so they get a higher retry budget (default 10) with longer backoff delays.
 * Non-throttle transient errors use the standard budget (default 3).
 * <p>
 * Optionally integrates with {@link AdaptiveBackoff} to scale throttle retry delays
 * based on the global throttling pressure observed across all requests on the same provider.
 * <p>
 * A total duration budget can also be applied to cap the cumulative time spent retrying,
 * regardless of remaining attempt count.
 */
class RetryPolicy {

    private static final Logger logger = LogManager.getLogger(RetryPolicy.class);

    static final int DEFAULT_MAX_RETRIES = 3;
    static final long DEFAULT_INITIAL_DELAY_MS = 200;
    static final long DEFAULT_MAX_DELAY_MS = 5000;

    static final int DEFAULT_THROTTLE_MAX_RETRIES = 10;
    static final long DEFAULT_THROTTLE_INITIAL_DELAY_MS = 500;
    static final long DEFAULT_THROTTLE_MAX_DELAY_MS = 30_000;

    /** No total duration budget — retries are bounded only by attempt count. */
    static final long NO_BUDGET = 0;

    static final RetryPolicy NONE = new RetryPolicy(0, 0, 0, 0, 0, 0, NO_BUDGET, null);
    static final RetryPolicy DEFAULT = new RetryPolicy(
        DEFAULT_MAX_RETRIES,
        DEFAULT_INITIAL_DELAY_MS,
        DEFAULT_MAX_DELAY_MS,
        DEFAULT_THROTTLE_MAX_RETRIES,
        DEFAULT_THROTTLE_INITIAL_DELAY_MS,
        DEFAULT_THROTTLE_MAX_DELAY_MS,
        NO_BUDGET,
        null
    );

    private final int maxRetries;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final int throttleMaxRetries;
    private final long throttleInitialDelayMs;
    private final long throttleMaxDelayMs;
    private final long maxTotalDurationMs;
    private final AdaptiveBackoff adaptiveBackoff;

    RetryPolicy(
        int maxRetries,
        long initialDelayMs,
        long maxDelayMs,
        int throttleMaxRetries,
        long throttleInitialDelayMs,
        long throttleMaxDelayMs,
        long maxTotalDurationMs,
        AdaptiveBackoff adaptiveBackoff
    ) {
        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.throttleMaxRetries = throttleMaxRetries;
        this.throttleInitialDelayMs = throttleInitialDelayMs;
        this.throttleMaxDelayMs = throttleMaxDelayMs;
        this.maxTotalDurationMs = maxTotalDurationMs;
        this.adaptiveBackoff = adaptiveBackoff;
    }

    RetryPolicy(int maxRetries, long initialDelayMs, long maxDelayMs) {
        this(maxRetries, initialDelayMs, maxDelayMs, maxRetries, initialDelayMs, maxDelayMs, NO_BUDGET, null);
    }

    RetryPolicy(int maxRetries, long initialDelayMs, long maxDelayMs, long maxTotalDurationMs) {
        this(maxRetries, initialDelayMs, maxDelayMs, maxRetries, initialDelayMs, maxDelayMs, maxTotalDurationMs, null);
    }

    /**
     * Returns a new policy with the same retry parameters but constrained by a total duration budget.
     * When the elapsed time plus the next delay would exceed the budget, the operation fails immediately
     * rather than sleeping and retrying.
     */
    RetryPolicy withTotalDurationBudget(long budgetMs) {
        return new RetryPolicy(
            maxRetries,
            initialDelayMs,
            maxDelayMs,
            throttleMaxRetries,
            throttleInitialDelayMs,
            throttleMaxDelayMs,
            budgetMs,
            adaptiveBackoff
        );
    }

    RetryPolicy withAdaptiveBackoff(AdaptiveBackoff backoff) {
        return new RetryPolicy(
            maxRetries,
            initialDelayMs,
            maxDelayMs,
            throttleMaxRetries,
            throttleInitialDelayMs,
            throttleMaxDelayMs,
            maxTotalDurationMs,
            backoff
        );
    }

    RetryPolicy withThrottleConfig(int throttleRetries, long throttleInitialMs, long throttleMaxMs) {
        return new RetryPolicy(
            maxRetries,
            initialDelayMs,
            maxDelayMs,
            throttleRetries,
            throttleInitialMs,
            throttleMaxMs,
            maxTotalDurationMs,
            adaptiveBackoff
        );
    }

    int maxRetries() {
        return maxRetries;
    }

    int throttleMaxRetries() {
        return throttleMaxRetries;
    }

    long delayMillis(int attempt) {
        return delayMillis(attempt, false);
    }

    long delayMillis(int attempt, boolean isThrottle) {
        if (maxRetries == 0 && throttleMaxRetries == 0) {
            return 0;
        }
        long effectiveInitial = isThrottle ? throttleInitialDelayMs : initialDelayMs;
        long effectiveMax = isThrottle ? throttleMaxDelayMs : maxDelayMs;

        long baseDelay = effectiveInitial * (1L << attempt);
        long capped = Math.min(baseDelay, effectiveMax);
        long jitter = Randomness.get().nextLong(capped / 4 + 1);
        long delay = Math.min(effectiveMax, capped + jitter);

        if (isThrottle && adaptiveBackoff != null && adaptiveBackoff.isEnabled()) {
            delay *= adaptiveBackoff.currentMultiplier();
            delay = Math.min(delay, effectiveMax);
        }
        return delay;
    }

    boolean isRetryable(Throwable t) {
        if (maxRetries == 0 && throttleMaxRetries == 0) {
            return false;
        }
        return isTransientStorageError(t);
    }

    long maxTotalDurationMs() {
        return maxTotalDurationMs;
    }

    <T> T execute(IOSupplier<T> operation, String operationName, StoragePath path) throws IOException {
        if (maxRetries == 0 && throttleMaxRetries == 0) {
            return operation.get();
        }
        long startNanos = System.nanoTime();
        int maxAttempts = Math.max(maxRetries, throttleMaxRetries);
        for (int attempt = 0; attempt <= maxAttempts; attempt++) {
            try {
                T result = operation.get();
                if (adaptiveBackoff != null) {
                    adaptiveBackoff.onSuccess();
                }
                return result;
            } catch (IOException e) {
                boolean isThrottle = isThrottlingError(e);
                boolean isTransient = isThrottle || isTransientStorageError(e);
                int effectiveMaxRetries = isThrottle ? throttleMaxRetries : maxRetries;

                if (isTransient == false || attempt >= effectiveMaxRetries) {
                    throw e;
                }

                if (isThrottle && adaptiveBackoff != null) {
                    adaptiveBackoff.onThrottled();
                }

                long delay = delayMillis(attempt, isThrottle);
                if (maxTotalDurationMs > 0) {
                    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
                    if (elapsedMs + delay > maxTotalDurationMs) {
                        logger.debug(
                            "aborting retry for [{}] on [{}]: elapsed [{}]ms + delay [{}]ms exceeds budget [{}]ms",
                            operationName,
                            path,
                            elapsedMs,
                            delay,
                            maxTotalDurationMs
                        );
                        throw e;
                    }
                }
                logger.debug(
                    "retrying [{}] for [{}] after {} failure (attempt [{}]/[{}], delay [{}]ms): [{}]",
                    operationName,
                    path,
                    isThrottle ? "throttle" : "transient",
                    attempt + 1,
                    effectiveMaxRetries,
                    delay,
                    e.getMessage()
                );
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
        throw new AssertionError("retry loop exited unexpectedly");
    }

    void notifySuccess() {
        if (adaptiveBackoff != null) {
            adaptiveBackoff.onSuccess();
        }
    }

    void notifyThrottled() {
        if (adaptiveBackoff != null) {
            adaptiveBackoff.onThrottled();
        }
    }

    static boolean isThrottlingError(Throwable t) {
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (isThrottlingSingleCause(current)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isThrottlingSingleCause(Throwable t) {
        String message = t.getMessage();
        if (message == null) {
            return false;
        }
        if (message.contains("429") || message.contains("Too Many Requests")) {
            return true;
        }
        if (message.contains("503") || message.contains("Service Unavailable")) {
            return true;
        }
        if (message.contains("SlowDown") || message.contains("Reduce your request rate")) {
            return true;
        }
        return false;
    }

    private static boolean isTransientStorageError(Throwable t) {
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (isTransientSingleCause(current)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isTransientSingleCause(Throwable t) {
        if (t instanceof SocketTimeoutException) {
            return true;
        }
        if (t instanceof ConnectException) {
            for (Throwable cause = t.getCause(); cause != null; cause = cause.getCause()) {
                if (cause instanceof UnknownHostException) {
                    return false;
                }
            }
            return true;
        }
        if (t instanceof SocketException) {
            String msg = t.getMessage();
            return msg != null && msg.contains("Connection reset");
        }
        String message = t.getMessage();
        if (message == null) {
            return false;
        }
        if (message.contains("500") || message.contains("Internal Server Error") || message.contains("InternalError")) {
            return true;
        }
        return isThrottlingSingleCause(t);
    }

    @FunctionalInterface
    interface IOSupplier<T> {
        T get() throws IOException;
    }
}
