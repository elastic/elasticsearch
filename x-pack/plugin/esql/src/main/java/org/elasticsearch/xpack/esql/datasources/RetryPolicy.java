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
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InterruptedIOException;
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

    /** Whether a fault warrants a retry, and the backoff to wait first. */
    record RetryDecision(boolean retry, long delayMillis) {
        static final RetryDecision GIVE_UP = new RetryDecision(false, 0L);
    }

    /**
     * Best-effort lifecycle callbacks for a retry driver, letting the caller surface terminal give-ups and the
     * cumulative backoff stall to node telemetry. {@link RetryPolicy} stays decision-only: it computes decisions
     * and reports the lifecycle here, holding no metric state itself. All methods default to no-ops
     * ({@link #NONE}), so a driver that does not care about telemetry is unaffected.
     */
    interface RetryTelemetry {
        RetryTelemetry NONE = new RetryTelemetry() {};

        /** The operation gave up on {@code failure} after a cumulative {@code totalBackoffMillis} spent in backoff. */
        default void onGiveUp(Throwable failure, long totalBackoffMillis) {}

        /** The operation completed after a cumulative {@code totalBackoffMillis} spent in backoff (0 if it never retried). */
        default void onComplete(long totalBackoffMillis) {}
    }

    /**
     * The shared retry decision used by every retry driver — sync {@link #execute}, async reads, and the
     * mid-read resume. Classifies the fault, applies the throttle-vs-normal budget against {@code attempt}
     * (retries already made), feeds the adaptive backoff on a throttle, and checks the total-time budget against
     * {@code startNanos}. Returns the backoff to wait before retrying, or {@link RetryDecision#GIVE_UP}. Having
     * one decision point keeps every driver's classification/budget/backoff identical (the throttle signal that
     * used to drift between hand-rolled loops cannot diverge here).
     */
    RetryDecision decide(Throwable e, int attempt, long startNanos) {
        boolean isThrottle = isThrottlingError(e);
        boolean isTransient = isThrottle || isTransientStorageError(e);
        int effectiveMaxRetries = isThrottle ? throttleMaxRetries : maxRetries;
        if (isTransient == false || attempt >= effectiveMaxRetries) {
            return RetryDecision.GIVE_UP;
        }
        long delay = delayMillis(attempt, isThrottle);
        if (maxTotalDurationMs > 0 && (System.nanoTime() - startNanos) / 1_000_000 + delay > maxTotalDurationMs) {
            return RetryDecision.GIVE_UP;
        }
        // Feed the cross-request adaptive backoff only once we've committed to retrying — no point ramping it
        // when we're about to give up on the budget or the time limit.
        if (isThrottle && adaptiveBackoff != null) {
            adaptiveBackoff.onThrottled();
        }
        return new RetryDecision(true, delay);
    }

    <T> T execute(IOSupplier<T> operation, String operationName, StoragePath path) throws IOException {
        return execute(operation, operationName, path, () -> {});
    }

    /**
     * As {@link #execute(IOSupplier, String, StoragePath)}, plus a hook fired exactly once per
     * scheduled retry — used by {@code RetryableStorageObject} to bump
     * {@code StorageObjectMetricsCounters.retryCount} so the observed retry count surfaces in the
     * query profile. The hook fires when a transient/throttle failure has been classified as
     * retryable and the policy has decided to sleep + try again; it does NOT fire on the initial
     * attempt or on a final terminal failure.
     */
    <T> T execute(IOSupplier<T> operation, String operationName, StoragePath path, Runnable onRetry) throws IOException {
        return execute(operation, operationName, path, onRetry, RetryTelemetry.NONE);
    }

    /**
     * As {@link #execute(IOSupplier, String, StoragePath, Runnable)}, plus a best-effort {@link RetryTelemetry}
     * whose {@link RetryTelemetry#onComplete}/{@link RetryTelemetry#onGiveUp} fire once when the operation ends,
     * carrying the cumulative backoff time so the caller can publish read-stall / terminal-error metrics. The
     * policy remains decision-only; it merely reports the lifecycle it already computes.
     */
    <T> T execute(IOSupplier<T> operation, String operationName, StoragePath path, Runnable onRetry, RetryTelemetry telemetry)
        throws IOException {
        if (maxRetries == 0 && throttleMaxRetries == 0) {
            // Retries disabled: run the operation once, but still fire the lifecycle so a terminal failure is
            // surfaced to telemetry (there was no backoff, so the cumulative stall is 0). Mirror the retry loop's
            // catch — a plain RuntimeException propagates without a give-up, exactly as it does with retries on.
            try {
                T result = operation.get();
                telemetry.onComplete(0L);
                return result;
            } catch (IOException | ExternalUnavailableException e) {
                telemetry.onGiveUp(e, 0L);
                throw e;
            }
        }
        long startNanos = System.nanoTime();
        long totalBackoffMillis = 0;
        int maxAttempts = Math.max(maxRetries, throttleMaxRetries);
        for (int attempt = 0; attempt <= maxAttempts; attempt++) {
            try {
                T result = operation.get();
                if (adaptiveBackoff != null) {
                    adaptiveBackoff.onSuccess();
                }
                telemetry.onComplete(totalBackoffMillis);
                return result;
            } catch (IOException | ExternalUnavailableException e) {
                // ExternalUnavailableException is an unchecked QlException (it maps to a 503), so it is caught
                // explicitly alongside the checked transport IOExceptions; both flow through the one decision point.
                RetryDecision decision = decide(e, attempt, startNanos);
                if (decision.retry() == false) {
                    telemetry.onGiveUp(e, totalBackoffMillis);
                    throw e;
                }
                totalBackoffMillis += decision.delayMillis();
                logger.debug(
                    "retrying [{}] for [{}] after transient failure (attempt [{}], delay [{}]ms): [{}]",
                    operationName,
                    path,
                    attempt + 1,
                    decision.delayMillis(),
                    e.getMessage()
                );
                // Abort promptly if the originating query was already cancelled (skips the retry-count bump).
                if (StorageRetryCancellation.isCancelled()) {
                    throw new TaskCancelledException(StorageRetryCancellation.CANCELLED_MESSAGE);
                }
                onRetry.run();
                try {
                    // Cancellation-aware sleep: polls during the delay so a cancel that flips mid-sleep aborts
                    // within ~one poll interval rather than waiting out the full (up to 30s throttle) backoff.
                    StorageRetryCancellation.sleepWithCancellationChecks(decision.delayMillis());
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

    static boolean isThrottlingError(Throwable t) {
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (isThrottlingSingleCause(current)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isThrottlingSingleCause(Throwable t) {
        // Throttling (HTTP 429 / 503 / SlowDown) is classified by the provider from the status code and flagged
        // on the typed exception; it is no longer inferred from message text.
        return t instanceof ExternalUnavailableException eue && eue.throttling();
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
        // Typed signal from a storage provider: it classified this fault by type/status (no message sniffing).
        // Every retryable remote-store status (5xx/429/timeout) is mapped to ExternalUnavailableException at the
        // provider boundary, so the retry layer keys on the type, not the HTTP status or the message.
        if (t instanceof ExternalUnavailableException) {
            return true;
        }
        // ConnectException is a SocketException subtype, so it must be checked FIRST: a failure to (re)connect
        // is transient EXCEPT when caused by an unresolvable host (a config / DNS error, not worth retrying).
        if (t instanceof ConnectException) {
            for (Throwable cause = t.getCause(); cause != null; cause = cause.getCause()) {
                if (cause instanceof UnknownHostException) {
                    return false;
                }
            }
            return true;
        }
        // Other JDK transport types are transient by type. A SocketException covers connection reset / reset
        // by peer / broken pipe; on a read these are all transient, since the byte range can be re-opened.
        if (t instanceof SocketTimeoutException || t instanceof SocketException || t instanceof InterruptedIOException) {
            return true;
        }
        // HTTP-status transients (500 / 503 / 429) reach here only as a typed ExternalUnavailableException raised
        // by the provider (the layer that has the status code), which is already handled above; a bare throwable
        // (no transient type, no JDK transport type) is treated as a real error.
        return false;
    }

    @FunctionalInterface
    interface IOSupplier<T> {
        T get() throws IOException;
    }
}
