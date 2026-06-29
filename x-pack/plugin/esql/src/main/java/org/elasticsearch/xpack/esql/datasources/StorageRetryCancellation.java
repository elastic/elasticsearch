/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.function.BooleanSupplier;

/**
 * Ambient, thread-local cancellation signal consulted by the synchronous storage-retry backoff sites
 * ({@link RetryPolicy#execute} and {@code RetryableStorageObject.ResumingInputStream.reopenOrThrow})
 * so that a single in-flight footer read parked in retry/throttle backoff aborts promptly when the
 * originating query is cancelled.
 *
 * <p>Providers and their {@link RetryPolicy} are cached per scheme in {@code StorageProviderRegistry},
 * so the per-query cancellation signal cannot be bound at provider construction (it would leak across
 * queries). Instead, the synchronous footer reads run on the same worker thread that runs the split
 * discovery / resolution per-file lambda, so the signal is carried as thread-local state established
 * around that read via {@link #runWithCancellation}. Nested scopes restore the enclosing supplier on
 * exit; when no scope is active {@link #isCancelled()} returns {@code false}.
 *
 * <p><b>Scope.</b> Scopes are installed only around coordinator-side split-discovery and source-resolution
 * footer reads. Data-node runtime scan reads hit the same backoff with no scope active and are
 * intentionally not covered here: the driver already checks task cancellation between pages, so only the
 * in-read backoff is uncovered on that path. Extending cancellation to the runtime read path is left as a
 * separate, follow-up change.
 *
 * <p><b>Thread affinity.</b> The signal only reaches a backoff sleep that runs on the same thread the scope
 * was installed on. This holds for every synchronous {@code StorageObject} footer read driven from the
 * discovery / resolution worker. It does NOT reach reads outside that synchronous Java retry layer: the
 * bzip2 parallel block-boundary scan runs chunk reads on a separate executor for large files (the
 * thread-local does not propagate to those threads), and the parquet-rs reader performs footer I/O in a
 * native runtime that bypasses the Java retry layer entirely. Both fall back to their own, non-cancellable
 * backoff.
 */
final class StorageRetryCancellation {

    private static final ThreadLocal<BooleanSupplier> CURRENT = new ThreadLocal<>();

    /** Message used for the {@link TaskCancelledException} thrown when a backoff sleep is cut short by cancellation. */
    static final String CANCELLED_MESSAGE = "ES|QL storage retry cancelled";

    /**
     * Granularity of the cancellation-aware sleep: the requested delay is consumed in chunks no larger
     * than this so a cancel that arrives <em>after</em> the sleep has started is observed within roughly
     * this bound instead of after the full (up to 30s throttle) delay.
     */
    static final long POLL_INTERVAL_MS = 50;

    private StorageRetryCancellation() {}

    /**
     * Runs {@code body} with {@code isCancelled} installed as the ambient cancellation signal for the
     * current thread, restoring any previously installed supplier on exit (so nested scopes compose).
     */
    static <E extends Exception> void runWithCancellation(BooleanSupplier isCancelled, CheckedRunnable<E> body) throws E {
        BooleanSupplier previous = CURRENT.get();
        CURRENT.set(isCancelled);
        try {
            body.run();
        } finally {
            restore(previous);
        }
    }

    /**
     * As {@link #runWithCancellation(BooleanSupplier, CheckedRunnable)} but returns the value produced
     * by {@code body}.
     */
    static <T, E extends Exception> T callWithCancellation(BooleanSupplier isCancelled, CheckedSupplier<T, E> body) throws E {
        BooleanSupplier previous = CURRENT.get();
        CURRENT.set(isCancelled);
        try {
            return body.get();
        } finally {
            restore(previous);
        }
    }

    private static void restore(BooleanSupplier previous) {
        if (previous == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(previous);
        }
    }

    /** Returns {@code true} when an ambient cancellation signal is installed and reports cancelled. */
    static boolean isCancelled() {
        BooleanSupplier current = CURRENT.get();
        return current != null && current.getAsBoolean();
    }

    /**
     * Sleeps for {@code millis} while remaining responsive to the ambient cancellation signal. Rather than
     * one blocking {@link Thread#sleep}, the delay is consumed in {@link #POLL_INTERVAL_MS} chunks; the
     * cancellation signal is polled before each chunk and once more after the final wake, so a cancel that
     * arrives <em>after</em> the sleep has begun (flipped from another thread) aborts within roughly one poll
     * interval instead of after the whole delay. A non-positive {@code millis} still performs a single
     * cancellation check and returns.
     *
     * @throws TaskCancelledException if the ambient signal reports cancelled before or during the sleep
     * @throws InterruptedException   if the thread is interrupted while sleeping (caller decides how to map it)
     */
    static void sleepWithCancellationChecks(long millis) throws InterruptedException {
        if (isCancelled()) {
            throw new TaskCancelledException(CANCELLED_MESSAGE);
        }
        if (millis <= 0) {
            return;
        }
        long deadlineNanos = System.nanoTime() + millis * 1_000_000L;
        while (true) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                break;
            }
            long remainingMs = (remainingNanos + 999_999L) / 1_000_000L;
            Thread.sleep(Math.min(POLL_INTERVAL_MS, remainingMs));
            if (isCancelled()) {
                throw new TaskCancelledException(CANCELLED_MESSAGE);
            }
        }
    }
}
