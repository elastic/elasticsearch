/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionGate;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException.Condition;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.QueryAdmission;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * Limits the number of concurrent in-flight cloud storage API requests per node.
 * Uses a fair {@link Semaphore} to prevent starvation under sustained load.
 * <p>
 * Thread-safe and designed to be shared across all queries targeting the same storage scheme.
 * A permits value of 0 disables limiting entirely (all operations pass through).
 */
class ConcurrencyLimiter implements AdmissionGate {

    private static final Logger logger = LogManager.getLogger(ConcurrencyLimiter.class);

    static final ConcurrencyLimiter UNLIMITED = new ConcurrencyLimiter(QueryAdmission.DEFAULT_ACQUIRE_TIMEOUT_MS);

    private static TaskCancelledException cancelled() {
        return new TaskCancelledException("Cancelled while waiting for a concurrency permit");
    }

    private final Semaphore semaphore;
    private final String scheme;
    private final ExternalSourceSettings.BlobStoreConcurrency concurrency;
    private final long acquireTimeoutMs;
    private final AdmissionTracker tracker;
    private final AtomicLong lastWarnLogTime = new AtomicLong(0);
    private final ReentrantLock asyncLock = new ReentrantLock();
    private final ArrayDeque<AsyncWaiter> asyncWaiters = new ArrayDeque<>();
    private final ArrayList<Runnable> pendingCompletions = new ArrayList<>();

    private static final long WARN_LOG_INTERVAL_MS = 30_000;
    private static final long WARN_WAIT_THRESHOLD_MS = 5_000;

    private ConcurrencyLimiter(long acquireTimeoutMs) {
        this.scheme = null;
        this.concurrency = null;
        this.acquireTimeoutMs = acquireTimeoutMs;
        this.semaphore = null;
        this.tracker = AdmissionTracker.NOOP;
    }

    ConcurrencyLimiter(String scheme, ExternalSourceSettings.BlobStoreConcurrency concurrency) {
        this(scheme, concurrency, QueryAdmission.DEFAULT_ACQUIRE_TIMEOUT_MS);
    }

    ConcurrencyLimiter(String scheme, ExternalSourceSettings.BlobStoreConcurrency concurrency, long acquireTimeoutMs) {
        this(scheme, concurrency, acquireTimeoutMs, AdmissionTracker.NOOP);
    }

    ConcurrencyLimiter(
        String scheme,
        ExternalSourceSettings.BlobStoreConcurrency concurrency,
        long acquireTimeoutMs,
        AdmissionTracker tracker
    ) {
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
        this.tracker = tracker == null ? AdmissionTracker.NOOP : tracker;
        this.tracker.register(this);
    }

    /**
     * Acquires a permit, mapping limiter failures onto the exception types the storage retry
     * layer understands. Timeout is node-local admission back-pressure, raised as a retryable
     * {@link ExternalUnavailableException} ({@code RetryPolicy.execute} retries that type) with the
     * {@link Condition#LOCAL_CAPACITY} condition, so telemetry reports it as a local resource limit rather than as a store outage.
     * {@code throttling=false}: this is a local semaphore, not a remote-store 429/503, so it
     * must not feed the per-bucket adaptive backoff or the throttle budget. Interrupt is a
     * shutdown/cancellation signal, not back-pressure: throw non-retryable so the retry layer
     * does not loop on an interrupt flag that will fire again immediately. The interrupt is
     * preserved as the cause ({@link EsRejectedExecutionException} has no cause constructor).
     */
    void acquireChecked() {
        try {
            acquire();
        } catch (TimeoutException e) {
            ExternalUnavailableException ex = new ExternalUnavailableException(
                Condition.LOCAL_CAPACITY,
                StoragePath.NONE,
                "",
                "",
                false,
                0L,
                e
            );
            ex.setDetail(e.getMessage());
            throw ex;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            EsRejectedExecutionException rejected = new EsRejectedExecutionException("Interrupted while acquiring a concurrency permit");
            rejected.initCause(e);
            throw rejected;
        }
    }

    /**
     * {@link #release()} prefers queued tickets, so leftover sync {@code readBytes}
     * is not FIFO with {@link #acquireAsync}. Spare permits sitting on the semaphore
     * are drained to tickets first so sync cannot barge those.
     */
    void acquire() throws TimeoutException, InterruptedException {
        if (semaphore == null) {
            return;
        }
        // Fair zero-timeout acquire: fails when waiters exist, so lastGrant only moves on a real park.
        if (semaphore.tryAcquire(0, TimeUnit.NANOSECONDS)) {
            return;
        }
        long startNanos = System.nanoTime();
        AdmissionTracker.Wait wait = tracker.waitStarted(name(), Thread.currentThread().getName());
        boolean acquired;
        try {
            drainSparesToTickets();
            acquired = semaphore.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            wait.finished();
            throw e;
        }
        if (acquired == false) {
            wait.finished();
            throw new TimeoutException(timeoutMessage());
        }
        wait.granted();
        long waitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (waitMs > WARN_WAIT_THRESHOLD_MS) {
            long lastWarn = lastWarnLogTime.get();
            long now = System.currentTimeMillis();
            if (now - lastWarn > WARN_LOG_INTERVAL_MS && lastWarnLogTime.compareAndSet(lastWarn, now)) {
                logger.warn("[{}] request waited [{}]ms for a concurrency permit (max permits [{}])", scheme, waitMs, maxPermits());
            }
        }
    }

    /**
     * Untimed barge: returns immediately. Fair waiters may be skipped. Used by async retries so a
     * continuation never parks (fail, reschedule with jitter). First-attempt async still uses
     * {@link #acquireChecked()}.
     */
    boolean tryAcquire() {
        if (semaphore == null) {
            return true;
        }
        return semaphore.tryAcquire();
    }

    /**
     * {@link #tryAcquire()} mapped onto {@link PermitMissException} so the retry layer can wait on
     * the admission clock without consuming a storage attempt. throttling=false: local semaphore.
     */
    void acquireBargeChecked() {
        if (tryAcquire() == false) {
            throw new PermitMissException(scheme, maxPermits());
        }
    }

    long acquireTimeoutMs() {
        return acquireTimeoutMs;
    }

    /**
     * Untimed barge missed the node semaphore. Not a store fault: {@link RetryableStorageObject}
     * reschedules on {@link #acquireTimeoutMs()} and does not burn a storage retry or record
     * retry/error metrics. Terminal admission timeout is converted to the same
     * {@link ExternalUnavailableException} as {@link #acquireChecked()}.
     */
    static final class PermitMissException extends RuntimeException {
        PermitMissException(String scheme, int maxPermits) {
            super("No concurrency permit available for [" + scheme + "] (max permits [" + maxPermits + "])");
        }

        ExternalUnavailableException toUnavailable() {
            ExternalUnavailableException ex = new ExternalUnavailableException(
                Condition.STORE_UNAVAILABLE,
                StoragePath.NONE,
                "",
                "",
                false,
                0L
            );
            ex.setDetail(getMessage());
            return ex;
        }
    }

    void release() {
        if (semaphore == null) {
            return;
        }
        List<Runnable> completions;
        asyncLock.lock();
        try {
            failCancelledLocked();
            AsyncWaiter head = asyncWaiters.peekFirst();
            if (head != null) {
                asyncWaiters.removeFirst();
                head.completeGrant();
            } else {
                semaphore.release();
            }
            completions = takePendingCompletions();
        } finally {
            asyncLock.unlock();
        }
        runCompletions(completions);
    }

    /**
     * Async permit ticket. Completes on grant; fails on cancel. Uncontended grants complete
     * on the caller; contended grants are forked onto {@code executor}. Fair FIFO among
     * ticket waiters. Leftover sync {@link #acquire} can time out while tickets are queued.
     */
    SubscribableListener<Void> acquireAsync(BooleanSupplier cancelSignal, Executor executor) {
        SubscribableListener<Void> listener = new SubscribableListener<>();
        if (semaphore == null) {
            listener.onResponse(null);
            return listener;
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor is required");
        }
        BooleanSupplier cancel = cancelSignal == null ? () -> false : cancelSignal;
        if (cancel.getAsBoolean()) {
            listener.onFailure(cancelled());
            return listener;
        }
        List<Runnable> completions = List.of();
        Exception failNow = null;
        asyncLock.lock();
        try {
            if (cancel.getAsBoolean()) {
                failNow = cancelled();
            } else if (asyncWaiters.isEmpty() && semaphore.tryAcquire()) {
                AsyncWaiter waiter = new AsyncWaiter(listener, executor, cancel);
                waiter.completeGrantInline();
                completions = takePendingCompletions();
            } else {
                AsyncWaiter waiter = new AsyncWaiter(listener, executor, cancel);
                waiter.tracked = tracker.waitStarted(name(), Thread.currentThread().getName());
                asyncWaiters.addLast(waiter);
                grantSparesLocked();
                completions = takePendingCompletions();
            }
        } finally {
            asyncLock.unlock();
        }
        if (failNow != null) {
            listener.onFailure(failNow);
            return listener;
        }
        runCompletions(completions);
        return listener;
    }

    int asyncWaiterCount() {
        asyncLock.lock();
        try {
            return asyncWaiters.size();
        } finally {
            asyncLock.unlock();
        }
    }

    void wakeAsyncWaiters() {
        List<Runnable> completions;
        asyncLock.lock();
        try {
            failCancelledLocked();
            completions = takePendingCompletions();
        } finally {
            asyncLock.unlock();
        }
        runCompletions(completions);
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

    @Override
    public String name() {
        return scheme == null ? AdmissionTracker.permits("none") : AdmissionTracker.permits(scheme);
    }

    @Override
    public int holders() {
        if (semaphore == null) {
            return 0;
        }
        return maxPermits() - availablePermits();
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
        if (concurrency.parseFloorBinds()) {
            return Strings.format(
                "Timed out waiting for a concurrency permit for [%s] after [%s]ms (max permits [%s]). "
                    + "[%s] cannot raise this node's limit: the parse-floor of [%s] is the binding constraint.",
                scheme,
                acquireTimeoutMs,
                maxPermits(),
                key,
                ExternalSourceSettings.BLOB_STORE_CONCURRENCY_FLOOR
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

    private void failCancelledLocked() {
        Iterator<AsyncWaiter> it = asyncWaiters.iterator();
        while (it.hasNext()) {
            AsyncWaiter waiter = it.next();
            if (waiter.cancel.getAsBoolean()) {
                it.remove();
                waiter.fail(cancelled());
            }
        }
    }

    /**
     * Gives leftover semaphore permits to the FIFO ticket queue. Called after enqueue
     * (waiters skip {@code tryAcquire} when the queue is already non-empty) and before
     * leftover sync {@link #acquire} waits on the semaphore.
     */
    private void grantSparesLocked() {
        failCancelledLocked();
        while (asyncWaiters.isEmpty() == false && semaphore.tryAcquire()) {
            asyncWaiters.removeFirst().completeGrant();
        }
    }

    private void drainSparesToTickets() {
        List<Runnable> completions;
        asyncLock.lock();
        try {
            grantSparesLocked();
            completions = takePendingCompletions();
        } finally {
            asyncLock.unlock();
        }
        runCompletions(completions);
    }

    private List<Runnable> takePendingCompletions() {
        if (pendingCompletions.isEmpty()) {
            return List.of();
        }
        List<Runnable> batch = new ArrayList<>(pendingCompletions);
        pendingCompletions.clear();
        return batch;
    }

    private static void runCompletions(List<Runnable> completions) {
        for (Runnable completion : completions) {
            completion.run();
        }
    }

    private final class AsyncWaiter {
        private final SubscribableListener<Void> listener;
        private final Executor executor;
        private final BooleanSupplier cancel;
        private final AtomicBoolean completed = new AtomicBoolean();
        private AdmissionTracker.Wait tracked = AdmissionTracker.NOOP_WAIT;

        private AsyncWaiter(SubscribableListener<Void> listener, Executor executor, BooleanSupplier cancel) {
            this.listener = listener;
            this.executor = executor;
            this.cancel = cancel;
        }

        private void completeGrant() {
            pendingCompletions.add(() -> forkGrant(this::deliverGrant));
        }

        private void completeGrantInline() {
            pendingCompletions.add(this::deliverGrant);
        }

        private void deliverGrant() {
            if (completed.compareAndSet(false, true) == false) {
                return;
            }
            if (cancel.getAsBoolean()) {
                tracked.finished();
                release();
                listener.onFailure(cancelled());
                return;
            }
            tracked.granted();
            listener.onResponse(null);
        }

        private void fail(Exception e) {
            pendingCompletions.add(() -> fork(() -> {
                if (completed.compareAndSet(false, true)) {
                    tracked.finished();
                    listener.onFailure(e);
                }
            }));
        }

        private void forkGrant(Runnable task) {
            try {
                executor.execute(task);
            } catch (Exception e) {
                if (completed.compareAndSet(false, true)) {
                    tracked.finished();
                    release();
                    listener.onFailure(e);
                }
            }
        }

        private void fork(Runnable task) {
            try {
                executor.execute(task);
            } catch (Exception e) {
                if (completed.compareAndSet(false, true)) {
                    tracked.finished();
                    listener.onFailure(e);
                }
            }
        }
    }
}
