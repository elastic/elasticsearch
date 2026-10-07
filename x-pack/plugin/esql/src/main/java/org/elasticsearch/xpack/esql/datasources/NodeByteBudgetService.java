/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;
import org.elasticsearch.xpack.esql.datasources.spi.NodeByteBudget;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * Node-scoped {@link NodeByteBudget}. Look-ahead {@link #tryAdmit} refuses rather than wait.
 * {@link #admitAsync} is FIFO; one overshoot slot is granted only to a runnable lease. A unit
 * larger than the cap goes through that slot only. Legacy {@link #admitWaitUntil} keeps the
 * charge-on-expiry path for leftover parquet column iterator tests until the hard cap lands.
 */
public final class NodeByteBudgetService implements NodeByteBudget {

    private static final Logger logger = LogManager.getLogger(NodeByteBudgetService.class);

    public static final int HEAP_DIVISOR = 8;

    public static final long DEFAULT_ADMIT_WAIT_MS = 1_000L;

    public static final int FORCE_ADMIT_LIMIT_MULTIPLIER = 2;

    private static final long WARN_LOG_INTERVAL_MS = 30_000L;

    private final long limit;
    private final long admitWaitMs;
    private final AtomicLong used = new AtomicLong();
    private final AtomicLong peakUsed = new AtomicLong();
    private final AtomicLong forcedAdmits = new AtomicLong();
    private final AtomicLong waitNanos = new AtomicLong();
    private final AtomicLong lastWarnLogTime = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final ArrayDeque<TicketWaiter> waiters = new ArrayDeque<>();
    private final ArrayList<Runnable> pendingCompletions = new ArrayList<>();
    private RowGroupIo overshootOwner;
    private volatile AdmissionTracker tracker = AdmissionTracker.NOOP;

    public static NodeByteBudgetService forHeap() {
        long heapBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        return new NodeByteBudgetService(Math.max(1L, heapBytes / HEAP_DIVISOR));
    }

    public NodeByteBudgetService(long limit) {
        this(limit, DEFAULT_ADMIT_WAIT_MS);
    }

    public NodeByteBudgetService(long limit, long admitWaitMs) {
        if (limit < 1L) {
            throw new IllegalArgumentException("limit must be at least 1, got: " + limit);
        }
        if (admitWaitMs < 0L) {
            throw new IllegalArgumentException("admitWaitMs must be non-negative, got: " + admitWaitMs);
        }
        this.limit = limit;
        this.admitWaitMs = admitWaitMs;
    }

    public void bindTracker(AdmissionTracker tracker) {
        this.tracker = tracker == null ? AdmissionTracker.NOOP : tracker;
    }

    public long admitWaitMs() {
        return admitWaitMs;
    }

    public long forcedAdmits() {
        return forcedAdmits.get();
    }

    public long waitNanos() {
        return waitNanos.get();
    }

    public long forceAdmitLimit() {
        if (limit > Long.MAX_VALUE / FORCE_ADMIT_LIMIT_MULTIPLIER) {
            return Long.MAX_VALUE;
        }
        return limit * (long) FORCE_ADMIT_LIMIT_MULTIPLIER;
    }

    @Override
    public Hold tryAdmit(long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return new HoldImpl(this, 0L, null, false);
        }
        lock.lock();
        try {
            if (waiters.isEmpty() == false) {
                return null;
            }
            return tryChargeLocked(bytes, null, false);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public SubscribableListener<Hold> admitAsync(long bytes, RowGroupIo lease, BooleanSupplier cancelSignal, Executor executor) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (executor == null) {
            throw new IllegalArgumentException("executor is required");
        }
        BooleanSupplier cancel = cancelSignal == null ? () -> false : cancelSignal;
        SubscribableListener<Hold> listener = new SubscribableListener<>();
        if (bytes == 0L) {
            listener.onResponse(new HoldImpl(this, 0L, lease, false));
            return listener;
        }
        if (cancel.getAsBoolean() || (lease != null && lease.isCancelled())) {
            listener.onFailure(cancelled());
            return listener;
        }
        List<Runnable> completions = List.of();
        Exception failNow = null;
        lock.lock();
        try {
            if (cancel.getAsBoolean() || (lease != null && lease.isCancelled())) {
                failNow = cancelled();
            } else if (lease == null && bytes > limit) {
                failNow = new EsRejectedExecutionException("unit exceeds the node byte cap without a row-group lease");
            } else {
                HoldImpl immediate = waiters.isEmpty() ? tryChargeLocked(bytes, lease, true) : null;
                TicketWaiter waiter = new TicketWaiter(bytes, lease, cancel, executor, listener);
                if (immediate != null) {
                    waiter.completeInline(immediate);
                } else {
                    waiter.tracked = tracker.waitStarted(
                        AdmissionTracker.GATE_BYTES,
                        lease == null ? Thread.currentThread().getName() : "lease#" + lease.startSeq()
                    );
                    waiters.addLast(waiter);
                    if (lease != null) {
                        lease.setWake(this, this::wakeWaiters);
                    }
                    grantTicketWaitersLocked();
                }
                completions = takePendingCompletions();
            }
        } finally {
            lock.unlock();
        }
        if (failNow != null) {
            listener.onFailure(failNow);
            return listener;
        }
        runCompletions(completions);
        return listener;
    }

    @Override
    public void add(long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return;
        }
        lock.lock();
        try {
            long next = used.get() + bytes;
            if (next < 0L) {
                next = Long.MAX_VALUE;
            }
            setUsed(next);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void release(long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return;
        }
        List<Runnable> completions;
        lock.lock();
        try {
            used.updateAndGet(current -> {
                long next = current - bytes;
                return next < 0L ? 0L : next;
            });
            grantTicketWaitersLocked();
            notFull.signalAll();
            completions = takePendingCompletions();
        } finally {
            lock.unlock();
        }
        runCompletions(completions);
    }

    @Override
    public void clearOwner(RowGroupIo lease) {
        if (lease == null) {
            return;
        }
        List<Runnable> completions;
        lock.lock();
        try {
            if (overshootOwner != lease) {
                return;
            }
            overshootOwner = null;
            grantTicketWaitersLocked();
            notFull.signalAll();
            completions = takePendingCompletions();
        } finally {
            lock.unlock();
        }
        runCompletions(completions);
    }

    @Override
    @Nullable
    public RowGroupIo overshootOwner() {
        lock.lock();
        try {
            return overshootOwner;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long used() {
        return used.get();
    }

    @Override
    public long limit() {
        return limit;
    }

    public long peakUsed() {
        return peakUsed.get();
    }

    public int waiterCount() {
        lock.lock();
        try {
            return waiters.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void wakeWaiters() {
        List<Runnable> completions;
        lock.lock();
        try {
            failCancelledWaitersLocked();
            grantTicketWaitersLocked();
            notFull.signalAll();
            completions = takePendingCompletions();
        } finally {
            lock.unlock();
        }
        runCompletions(completions);
    }

    /**
     * Blocking wait with charge-on-expiry for leftover parquet column iterator tests.
     * Production coalesced reads no longer call this.
     * Lock order: this lock, then the budget lock inside {@link RowGroupIo#tryPinOvershoot()}.
     */
    public Hold admitWaitUntil(long bytes, RowGroupIo lease, long deadlineNanos) {
        if (lease == null) {
            throw new IllegalArgumentException("lease is required");
        }
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return new HoldImpl(this, 0L, lease, false);
        }
        boolean enteredWait = false;
        boolean forced = false;
        long waitStartedNanos = 0L;
        boolean ambientCancelled = StorageRetryCancellation.isCancelled();
        Hold hold;
        lock.lock();
        try {
            while (true) {
                if (lease.isCancelled()) {
                    throw cancelled();
                }
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0L && ambientCancelled) {
                    throw cancelled();
                }
                HoldImpl granted = tryChargeLocked(bytes, lease, true);
                if (granted != null) {
                    hold = granted;
                    break;
                }
                lease.setWake(this, this::wakeWaiters);
                if (lease.isCancelled()) {
                    throw cancelled();
                }
                if (remainingNanos <= 0L) {
                    if (ambientCancelled) {
                        throw cancelled();
                    }
                    long current = used.get();
                    long next = current + bytes;
                    if (next < 0L) {
                        throw new EsRejectedExecutionException("parquet I/O byte reservation overflow");
                    }
                    if (next > forceAdmitLimit()) {
                        throw overForceLimit(bytes, next);
                    }
                    setUsed(next);
                    forcedAdmits.incrementAndGet();
                    forced = true;
                    hold = new HoldImpl(this, bytes, lease, false);
                    break;
                }
                if (enteredWait == false) {
                    enteredWait = true;
                    waitStartedNanos = System.nanoTime();
                }
                try {
                    notFull.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new EsRejectedExecutionException("Interrupted while waiting for parquet I/O bytes: " + e);
                }
                lock.unlock();
                try {
                    ambientCancelled = StorageRetryCancellation.isCancelled();
                } finally {
                    lock.lock();
                }
            }
        } finally {
            if (enteredWait) {
                waitNanos.addAndGet(System.nanoTime() - waitStartedNanos);
            }
            lock.unlock();
        }
        if (forced) {
            maybeLogForcedAdmit(bytes);
        }
        return hold;
    }

    /**
     * Caller holds the lock. Does not inspect the waiter queue: the caller decides whether a
     * queued waiter may charge (grant path) or must refuse (look-ahead {@link #tryAdmit}).
     * {@code allowOvershoot} is true for tickets and the parking path; {@link #tryAdmit} never
     * takes the slot.
     */
    private HoldImpl tryChargeLocked(long bytes, RowGroupIo lease, boolean allowOvershoot) {
        long current = used.get();
        long next = current + bytes;
        if (next < 0L) {
            throw new EsRejectedExecutionException("parquet I/O byte reservation overflow");
        }
        if (next <= limit) {
            setUsed(next);
            return new HoldImpl(this, bytes, lease, false);
        }
        if (allowOvershoot == false || lease == null) {
            return null;
        }
        if (overshootOwner == lease) {
            setUsed(next);
            return new HoldImpl(this, bytes, lease, true);
        }
        if (overshootOwner == null && tryBecomeOwner(lease, next)) {
            return new HoldImpl(this, bytes, lease, true);
        }
        return null;
    }

    /**
     * Caller holds the lock. Budget lock is taken inside {@code tryPinOvershoot} / {@code unpin}
     * and released before this returns. A null scheduler (file:// and
     * {@code max_concurrent_requests=0}) assigns the owner without pinning.
     */
    private boolean tryBecomeOwner(RowGroupIo lease, long nextUsed) {
        if (lease.scheduler() != null) {
            if (lease.tryPinOvershoot()) {
                overshootOwner = lease;
                lease.setWake(this, this::wakeWaiters);
                setUsed(nextUsed);
                return true;
            }
            lease.unpin();
            return false;
        }
        overshootOwner = lease;
        setUsed(nextUsed);
        return true;
    }

    private void grantTicketWaitersLocked() {
        failCancelledWaitersLocked();
        while (waiters.isEmpty() == false) {
            TicketWaiter head = waiters.peekFirst();
            if (head.cancel.getAsBoolean() || (head.lease != null && head.lease.isCancelled())) {
                waiters.removeFirst();
                head.fail(cancelled());
                continue;
            }
            HoldImpl granted = tryChargeLocked(head.bytes, head.lease, true);
            if (granted == null) {
                return;
            }
            waiters.removeFirst();
            head.complete(granted);
        }
    }

    private void failCancelledWaitersLocked() {
        Iterator<TicketWaiter> it = waiters.iterator();
        while (it.hasNext()) {
            TicketWaiter waiter = it.next();
            if (waiter.cancel.getAsBoolean() || (waiter.lease != null && waiter.lease.isCancelled())) {
                it.remove();
                waiter.fail(cancelled());
            }
        }
    }

    private void setUsed(long next) {
        used.set(next);
        peakUsed.updateAndGet(peak -> Math.max(peak, next));
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

    private void maybeLogForcedAdmit(long bytes) {
        long last = lastWarnLogTime.get();
        long now = System.currentTimeMillis();
        if (now - last > WARN_LOG_INTERVAL_MS && lastWarnLogTime.compareAndSet(last, now)) {
            logger.warn(
                "parquet I/O byte wait expired; charged [{}] over cap [{}] (forced admits so far [{}], total wait [{}]ms)",
                ByteSizeValue.ofBytes(bytes),
                ByteSizeValue.ofBytes(limit),
                forcedAdmits.get(),
                TimeUnit.NANOSECONDS.toMillis(waitNanos.get())
            );
        }
    }

    private EsRejectedExecutionException overForceLimit(long bytes, long next) {
        return new EsRejectedExecutionException(
            "parquet I/O byte wait expired; charging ["
                + bytes
                + "] bytes would exceed twice the node cap ["
                + limit
                + "] (used ["
                + used.get()
                + "], next ["
                + next
                + "])"
        );
    }

    public static EsRejectedExecutionException cancelled() {
        return NodeByteBudget.cancelled();
    }

    private final class TicketWaiter {
        private final long bytes;
        private final RowGroupIo lease;
        private final BooleanSupplier cancel;
        private final Executor executor;
        private final SubscribableListener<Hold> listener;
        private final AtomicBoolean completed = new AtomicBoolean();
        private AdmissionTracker.Wait tracked = AdmissionTracker.NOOP_WAIT;

        private TicketWaiter(long bytes, RowGroupIo lease, BooleanSupplier cancel, Executor executor, SubscribableListener<Hold> listener) {
            this.bytes = bytes;
            this.lease = lease;
            this.cancel = cancel;
            this.executor = executor;
            this.listener = listener;
        }

        private void complete(HoldImpl hold) {
            pendingCompletions.add(() -> fork(() -> deliver(hold), hold));
        }

        private void completeInline(HoldImpl hold) {
            pendingCompletions.add(() -> deliver(hold));
        }

        private void deliver(HoldImpl hold) {
            if (completed.compareAndSet(false, true) == false) {
                hold.close();
                return;
            }
            if (cancel.getAsBoolean() || (lease != null && lease.isCancelled())) {
                tracked.finished();
                hold.close();
                listener.onFailure(cancelled());
                return;
            }
            tracked.granted();
            listener.onResponse(hold);
        }

        private void fail(Exception e) {
            pendingCompletions.add(() -> fork(() -> {
                if (completed.compareAndSet(false, true)) {
                    tracked.finished();
                    listener.onFailure(e);
                }
            }, null));
        }

        private void fork(Runnable task, @Nullable HoldImpl holdOnReject) {
            try {
                executor.execute(task);
            } catch (Exception e) {
                if (holdOnReject != null) {
                    holdOnReject.close();
                }
                if (completed.compareAndSet(false, true)) {
                    tracked.finished();
                    listener.onFailure(e);
                }
            }
        }
    }

    static final class HoldImpl implements Hold {
        private final NodeByteBudgetService budget;
        private final long bytes;
        private final RowGroupIo lease;
        private final boolean overshoot;
        private final AtomicLong remaining;
        private final AtomicBoolean closed = new AtomicBoolean();

        private HoldImpl(NodeByteBudgetService budget, long bytes, RowGroupIo lease, boolean overshoot) {
            this.budget = budget;
            this.bytes = bytes;
            this.lease = lease;
            this.overshoot = overshoot;
            this.remaining = new AtomicLong(Math.max(0L, bytes));
        }

        @Override
        public long bytes() {
            return bytes;
        }

        @Override
        public long remaining() {
            return remaining.get();
        }

        @Override
        public RowGroupIo lease() {
            return lease;
        }

        @Override
        public boolean isOvershoot() {
            return overshoot;
        }

        @Override
        public void drop(long dropBytes) {
            if (dropBytes <= 0L) {
                return;
            }
            while (true) {
                long current = remaining.get();
                if (current <= 0L) {
                    return;
                }
                long release = Math.min(current, dropBytes);
                if (remaining.compareAndSet(current, current - release)) {
                    budget.release(release);
                    return;
                }
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true) == false) {
                return;
            }
            // Charge only. Overshoot owner stays until clearOwner(lease): force-added
            // buffers can still sit in used, and a second over-cap unit must queue.
            drop(Long.MAX_VALUE);
        }
    }
}
