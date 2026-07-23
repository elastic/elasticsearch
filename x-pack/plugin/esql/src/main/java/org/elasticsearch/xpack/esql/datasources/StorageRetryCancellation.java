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
 * <p><b>Scope.</b> Scopes are installed around coordinator-side split-discovery and source-resolution footer
 * reads (see {@code ExternalSourceResolver}) and around the data-node runtime producer read (see
 * {@code AsyncExternalSourceOperatorFactory} and {@code ExternalSourceDrainUtils}). The runtime scope is keyed
 * to {@code AsyncExternalSourceBuffer.readCancelled()} — a hard cut of a still-running producer (task cancel /
 * async DELETE / LIMIT teardown), but <em>not</em> async STOP, which keeps buffered pages for a partial
 * response and must therefore let an in-flight read complete cooperatively rather than abort it mid-backoff.
 * This means a STOP arriving while a read is parked in backoff can lag by up to that read's remaining retry
 * budget before the producer observes {@code noMoreInputs} between pages and exits — sub-second for a transient
 * backoff, up to the per-attempt throttle ceiling (tens of seconds) for a throttled read. That bounded lag is
 * an accepted cost of the STOP contract: arming {@code readCancelled} on STOP would abort the read and degrade
 * a graceful partial-result stop into a hard failure, so STOP deliberately waits the backoff out.
 * Between pages the runtime producer already exits on {@code noMoreInputs}; the runtime scope adds the missing
 * in-read coverage so a page pull parked in retry/throttle backoff aborts promptly on a hard cancel instead of
 * sleeping out its budget (the cause of long-lived post-cancel reads on slow/throttling object stores).
 *
 * <p><b>Thread affinity.</b> The signal only reaches a backoff sleep that runs on the same thread the scope
 * was installed on. This holds for every synchronous {@code StorageObject} footer read driven from the
 * discovery / resolution worker, and for the runtime open / drain steps that pull pages on the same thread the
 * scope wraps. It does NOT reach reads outside that synchronous Java retry layer: the bzip2 parallel
 * block-boundary scan and other parallel-parse workers run chunk reads on separate executor threads (the
 * thread-local does not propagate to those threads), and the parquet-rs reader performs footer I/O in a
 * native runtime that bypasses the Java retry layer entirely. Both fall back to their own, non-cancellable
 * backoff.
 *
 * <p><b>Degenerate-query cancellation (what the scope does NOT fix).</b> The scope only shortcuts the Java
 * retry/throttle <em>backoff sleep</em> — it never interrupts an in-progress socket read or a native read, and
 * it never reaches a read running on a thread it does not wrap (a parallel-parse worker, a native reader). For a
 * degenerate query blocked in one of those — e.g. a single stalled object-store connection with no data flowing,
 * or a native row-group read — a hard cancel behaves as follows: the driver observes the cancel between loop
 * iterations and throws {@link TaskCancelledException}, and the task is marked cancelled at the task layer, but
 * the driver's <em>completion</em> (via {@code DriverContext.waitForAsyncActions}) — and therefore the final
 * response delivery and full resource release (the held storage connection, buffered blocks) — still waits for
 * that background read to unwind on its own: its socket/read timeout, a native stall timeout, or natural EOF.
 * In other words, arming {@code readCancelled} converts the common multi-hour case (a producer sleeping out an
 * object-store retry/throttle backoff) into a prompt abort, but it does not bound a read wedged in a genuinely
 * uncancellable operation off the scoped thread; that residual case is bounded only by the underlying layer's
 * own timeout, not by cancellation. Closing that gap needs interrupt-/stream-abort-based cancellation and is a
 * separate change.
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
