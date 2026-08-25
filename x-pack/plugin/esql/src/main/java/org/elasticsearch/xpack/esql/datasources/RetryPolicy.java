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
import java.util.function.LongSupplier;

/**
 * Retry policy with exponential backoff and jitter for transient storage failures.
 * Supports separate retry semantics for throttling errors (429/503/SlowDown) versus
 * other transient errors (connection reset, socket timeout).
 * <p>
 * <b>Throttle arm:</b> governed by a wall-clock time budget
 * ({@code esql.external.throttle_max_retry_duration}, default 30 s). Within the budget the
 * delay is either the server-supplied {@code Retry-After} hint (when the exception carries one) or the
 * computed exponential backoff, truncated to the remaining budget so the sleep never overshoots.
 * {@link #THROTTLE_RETRIES_SANITY_CAP} is a safety backstop; under typical cloud-provider
 * behaviour the time budget is the effective bound.
 * <p>
 * <b>Non-throttle transient arm:</b> bounded by attempt count (default {@link #DEFAULT_MAX_RETRIES})
 * with an optional secondary time budget check — semantics unchanged from the original design.
 * <p>
 * Optionally integrates with {@link AdaptiveBackoff} to scale throttle retry delays based on
 * the global throttling pressure observed across all requests on the same provider.
 */
class RetryPolicy {

    private static final Logger logger = LogManager.getLogger(RetryPolicy.class);

    static final int DEFAULT_MAX_RETRIES = 3;
    static final long DEFAULT_INITIAL_DELAY_MS = 200;
    static final long DEFAULT_MAX_DELAY_MS = 5000;

    /**
     * Sanity backstop on the number of throttle retries — guards against an infinite loop when the clock
     * is broken or Retry-After hints are unexpectedly small. Under typical cloud-provider behaviour
     * (Retry-After ≥ 1 s or exponential back-off from the 500 ms default), the time budget is reached
     * well before this cap.
     */
    static final int THROTTLE_RETRIES_SANITY_CAP = 500;
    static final long DEFAULT_THROTTLE_INITIAL_DELAY_MS = 500;
    static final long DEFAULT_THROTTLE_MAX_DELAY_MS = 30_000;

    /** No total duration budget — retries are bounded only by attempt count. */
    static final long NO_BUDGET = 0;

    static final RetryPolicy NONE = new RetryPolicy(0, 0, 0, 0, 0, 0, NO_BUDGET, null);
    static final RetryPolicy DEFAULT = new RetryPolicy(
        DEFAULT_MAX_RETRIES,
        DEFAULT_INITIAL_DELAY_MS,
        DEFAULT_MAX_DELAY_MS,
        THROTTLE_RETRIES_SANITY_CAP,
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
    private final LongSupplier clockNanos;

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
        this(
            maxRetries,
            initialDelayMs,
            maxDelayMs,
            throttleMaxRetries,
            throttleInitialDelayMs,
            throttleMaxDelayMs,
            maxTotalDurationMs,
            adaptiveBackoff,
            null
        );
    }

    private RetryPolicy(
        int maxRetries,
        long initialDelayMs,
        long maxDelayMs,
        int throttleMaxRetries,
        long throttleInitialDelayMs,
        long throttleMaxDelayMs,
        long maxTotalDurationMs,
        AdaptiveBackoff adaptiveBackoff,
        LongSupplier clockNanos
    ) {
        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.throttleMaxRetries = throttleMaxRetries;
        this.throttleInitialDelayMs = throttleInitialDelayMs;
        this.throttleMaxDelayMs = throttleMaxDelayMs;
        this.maxTotalDurationMs = maxTotalDurationMs;
        this.adaptiveBackoff = adaptiveBackoff;
        this.clockNanos = clockNanos != null ? clockNanos : System::nanoTime;
    }

    RetryPolicy(int maxRetries, long initialDelayMs, long maxDelayMs) {
        this(maxRetries, initialDelayMs, maxDelayMs, maxRetries, initialDelayMs, maxDelayMs, NO_BUDGET, null);
    }

    RetryPolicy(int maxRetries, long initialDelayMs, long maxDelayMs, long maxTotalDurationMs) {
        this(maxRetries, initialDelayMs, maxDelayMs, maxRetries, initialDelayMs, maxDelayMs, maxTotalDurationMs, null);
    }

    /**
     * Returns a new policy with the same retry parameters but constrained by a total duration budget.
     * For the throttle arm the budget is the primary bound: the delay is truncated to the remaining budget
     * rather than causing a refusal, so the budget is genuinely spent before giving up.
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
            adaptiveBackoff,
            clockNanos
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
            backoff,
            clockNanos
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
            adaptiveBackoff,
            clockNanos
        );
    }

    /** Returns a new policy that uses the given clock supplier instead of {@code System::nanoTime}. For testing only. */
    RetryPolicy withClock(LongSupplier clock) {
        return new RetryPolicy(
            maxRetries,
            initialDelayMs,
            maxDelayMs,
            throttleMaxRetries,
            throttleInitialDelayMs,
            throttleMaxDelayMs,
            maxTotalDurationMs,
            adaptiveBackoff,
            clock
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
     * mid-read resume. Classifies the fault, applies the appropriate bound against {@code attempt}
     * (retries already made), feeds the adaptive backoff on a throttle, and checks the time budget against
     * {@code startNanos}. Returns the backoff to wait before retrying, or {@link RetryDecision#GIVE_UP}.
     * Having one decision point keeps every driver's classification/budget/backoff identical.
     * <p>
     * <b>Throttle arm:</b> the time budget is the primary bound. The delay is truncated to the remaining
     * budget rather than causing a refusal, so the budget is genuinely spent before giving up. When the
     * exception carries a server-supplied {@code Retry-After} hint, that hint is used as the delay; if it
     * exceeds the remaining budget the operation gives up immediately (retrying before the store's stated
     * wait is spam, not resilience). {@link #throttleMaxRetries} acts as a sanity cap only.
     * <p>
     * <b>Non-throttle arm:</b> attempt count is the primary bound; the time budget is a secondary check.
     */
    RetryDecision decide(Throwable e, int attempt, long startNanos) {
        boolean isThrottle = isThrottlingError(e);
        boolean isTransient = isThrottle || isTransientStorageError(e);
        if (isTransient == false) {
            return RetryDecision.GIVE_UP;
        }

        if (isThrottle) {
            // Throttle arm: budget-governed; attempt count is a sanity backstop only.
            if (attempt >= throttleMaxRetries) {
                return RetryDecision.GIVE_UP;
            }
            long retryAfterMs = retryAfterMsFromChain(e);
            long elapsedMs = (clockNanos.getAsLong() - startNanos) / 1_000_000;

            long delay;
            if (retryAfterMs > 0) {
                // Honor the server's hint, capped at our maximum configured delay so a broken server cannot
                // cause an unbounded sleep. If the capped hint still exceeds the remaining budget, give up:
                // retrying before the store's stated wait is spam, not resilience.
                long cappedHint = Math.min(retryAfterMs, throttleMaxDelayMs);
                if (maxTotalDurationMs > 0 && elapsedMs + cappedHint > maxTotalDurationMs) {
                    return RetryDecision.GIVE_UP;
                }
                delay = cappedHint;
            } else {
                long computed = delayMillis(attempt, true);
                if (maxTotalDurationMs > 0) {
                    long remainingMs = maxTotalDurationMs - elapsedMs;
                    if (remainingMs < throttleInitialDelayMs) {
                        // Remaining budget is less than the minimum meaningful retry sleep. Starting
                        // another attempt only to truncate the delay to near-zero is wasteful; treat the
                        // budget as spent. The hint path uses a tighter test (hint > remaining) because
                        // the server told us exactly how long to wait — a partial hint is useless.
                        return RetryDecision.GIVE_UP;
                    }
                    // Truncate to remaining budget so the sleep never overshoots.
                    delay = Math.min(computed, remainingMs);
                } else {
                    delay = computed;
                }
            }

            // Feed the cross-request adaptive backoff only once we've committed to retrying.
            if (adaptiveBackoff != null) {
                adaptiveBackoff.onThrottled();
            }
            return new RetryDecision(true, delay);
        } else {
            // Non-throttle transient arm: attempt count is the effective bound.
            if (attempt >= maxRetries) {
                return RetryDecision.GIVE_UP;
            }
            long delay = delayMillis(attempt, false);
            if (maxTotalDurationMs > 0 && (clockNanos.getAsLong() - startNanos) / 1_000_000 + delay > maxTotalDurationMs) {
                return RetryDecision.GIVE_UP;
            }
            return new RetryDecision(true, delay);
        }
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
        long startNanos = clockNanos.getAsLong();
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

    /** Walks the cause chain to extract a server-supplied {@code Retry-After} hint, mirroring {@link #isThrottlingError}. */
    private static long retryAfterMsFromChain(Throwable e) {
        for (Throwable current = e; current != null; current = current.getCause()) {
            if (current instanceof ExternalUnavailableException eue && eue.retryAfterMs() > 0) {
                return eue.retryAfterMs();
            }
        }
        return 0L;
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
        // Typed signal: a fault was classified by type/status (no message sniffing). Every retryable remote-store
        // status (5xx/429/timeout) is mapped to ExternalUnavailableException at the provider boundary, and node-local
        // admission back-pressure (permit exhaustion) is mapped to it at the concurrency-limiter boundary, so the retry
        // layer keys on the type, not the HTTP status or the message.
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
