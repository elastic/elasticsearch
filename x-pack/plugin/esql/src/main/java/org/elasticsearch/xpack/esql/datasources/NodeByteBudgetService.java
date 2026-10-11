/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionGate;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;
import org.elasticsearch.xpack.esql.datasources.spi.NodeByteBudget;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * Node-scoped {@link NodeByteBudget}. Look-ahead {@link #tryAdmit} refuses rather than wait.
 * {@link #admitAsync} is FIFO; one overshoot slot is granted only to a runnable lease. A unit
 * larger than the cap goes through that slot only. There is no blocking wait and no
 * charge-on-expiry: waiters park on a ticket until a grant or cancel. Live-cluster
 * {@code hot_threads} proof that {@code esql_worker} is absent from gate frames is deferred.
 */
public final class NodeByteBudgetService implements NodeByteBudget {

    public static final int HEAP_DIVISOR = 8;

    private final long limit;
    private final AtomicLong used = new AtomicLong();
    private final AtomicLong peakUsed = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();
    private final ArrayDeque<TicketWaiter> waiters = new ArrayDeque<>();
    private final ArrayList<Runnable> pendingCompletions = new ArrayList<>();
    private RowGroupIo overshootOwner;
    /** Live OVER_CAP rescue holds. Guarded by {@link #lock}. */
    private int rescueHolds;
    private volatile AdmissionTracker tracker = AdmissionTracker.NOOP;

    public static NodeByteBudgetService forHeap() {
        long heapBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        return new NodeByteBudgetService(Math.max(1L, heapBytes / HEAP_DIVISOR));
    }

    public NodeByteBudgetService(long limit) {
        if (limit < 1L) {
            throw new IllegalArgumentException("limit must be at least 1, got: " + limit);
        }
        this.limit = limit;
    }

    public void bindTracker(AdmissionTracker tracker) {
        this.tracker = tracker == null ? AdmissionTracker.NOOP : tracker;
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
        Exception enqueueFail = terminalFailure(lease, cancel);
        if (enqueueFail != null) {
            listener.onFailure(enqueueFail);
            return listener;
        }
        List<Runnable> completions = List.of();
        Exception failNow = null;
        lock.lock();
        try {
            failNow = terminalFailure(lease, cancel);
            if (failNow == null && lease == null && bytes > limit) {
                failNow = new EsRejectedExecutionException("unit exceeds the node byte cap without a row-group lease");
            } else if (failNow == null) {
                HoldImpl immediate = waiters.isEmpty() ? tryChargeLocked(bytes, lease, true) : null;
                TicketWaiter waiter = new TicketWaiter(bytes, lease, cancel, executor, listener);
                if (immediate != null) {
                    waiter.completeInline(immediate);
                } else {
                    waiter.tracked = tracker.waitStarted(
                        AdmissionTracker.GATE_BYTES,
                        lease == null
                            ? Thread.currentThread().getName() + ":bytes=" + bytes
                            : "lease#" + lease.startSeq() + ":bytes=" + bytes
                    );
                    waiters.addLast(waiter);
                    if (lease != null) {
                        lease.setWake(this, () -> onLeaseTerminal(lease));
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
            if (overshootOwner == lease) {
                overshootOwner = null;
            }
            // Unpin before the grant loop so a FIFO head can take the slot this pin was
            // blocking. Same lock order as tryBecomeOwner (budget then scheduler).
            lease.unpin();
            grantTicketWaitersLocked();
            completions = takePendingCompletions();
        } finally {
            lock.unlock();
        }
        runCompletions(completions);
    }

    /**
     * Lease-cancel wake: always re-grant (and unpin) so a cancelled owner does not keep the
     * slot until iterator close. {@link RowGroupIo#finish()} does not invoke this. The
     * cancelled owner's bytes stay in {@code used} until its iterator closes, so a new owner
     * can make the node transiently {@code cap + 2} units. The only production
     * {@link RowGroupIo#cancel()} is {@code QueryConcurrencyBudget.close} at query end, so
     * that window is short.
     */
    private void onLeaseTerminal(RowGroupIo lease) {
        clearOwner(lease);
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
            failTerminalWaitersLocked();
            grantTicketWaitersLocked();
            completions = takePendingCompletions();
        } finally {
            lock.unlock();
        }
        runCompletions(completions);
    }

    /**
     * Unsticks the FIFO head, then re-runs the normal grant loop. Skips cancelled heads.
     * {@link AdmissionGate.RescueResult#OVER_CAP} is a plain hold ({@code owner=false}), counted
     * in {@link #used()}. At most one rescued hold is live: a second over-cap grant waits until
     * that hold releases. OVER_CAP is skipped when {@code used} has dropped since the head
     * parked (holders draining, not a wedge). {@link AdmissionGate.RescueResult#REGRANT} is a
     * within-cap grant that should have happened on an earlier release (lost wakeup).
     * {@code delivery} {@code null} keeps each waiter's executor.
     */
    public AdmissionGate.RescueResult rescueHeadOverCap() {
        return rescueHeadOverCap(null);
    }

    /**
     * Fail cancelled or finished waiters, then grant the next runnable head. Same lock work as
     * {@link #wakeWaiters()}. Completions run on each waiter's executor after the lock is
     * dropped. Used every watchdog tick so a cancelled FIFO head cannot sit in front of a
     * waiter that now fits, which would otherwise become a rescue {@code REGRANT}.
     */
    public void failCancelledWaiters() {
        wakeWaiters();
    }

    public AdmissionGate.RescueResult rescueHeadOverCap(@Nullable Executor delivery) {
        List<Runnable> completions;
        AdmissionGate.RescueResult result;
        lock.lock();
        try {
            result = rescueHeadLocked(delivery);
            grantTicketWaitersLocked(delivery);
            completions = takePendingCompletions();
        } finally {
            lock.unlock();
        }
        runCompletions(completions);
        return result;
    }

    /**
     * Caller holds the lock. Does not inspect the waiter queue: the caller decides whether a
     * queued waiter may charge (grant path) or must refuse (look-ahead {@link #tryAdmit}).
     * {@code allowOvershoot} is true for tickets; {@link #tryAdmit} never takes the slot.
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
                lease.setWake(this, () -> onLeaseTerminal(lease));
                setUsed(nextUsed);
                return true;
            }
            lease.unpin();
            return false;
        }
        overshootOwner = lease;
        lease.setWake(this, () -> onLeaseTerminal(lease));
        setUsed(nextUsed);
        return true;
    }

    private void grantTicketWaitersLocked() {
        grantTicketWaitersLocked(null);
    }

    private void grantTicketWaitersLocked(@Nullable Executor delivery) {
        failTerminalWaitersLocked();
        while (waiters.isEmpty() == false) {
            TicketWaiter head = waiters.peekFirst();
            Exception terminal = terminalFailure(head.lease, head.cancel);
            if (terminal != null) {
                waiters.removeFirst();
                head.fail(terminal);
                continue;
            }
            HoldImpl granted = tryChargeLocked(head.bytes, head.lease, true);
            if (granted == null) {
                return;
            }
            waiters.removeFirst();
            head.complete(granted, deliveryFor(head, delivery));
        }
    }

    /**
     * Caller holds the lock. Cancelled heads are dropped until a live head remains. A head that
     * {@link #tryChargeLocked} can admit is a lost-wakeup {@link AdmissionGate.RescueResult#REGRANT}.
     * An over-cap grant is a {@link HoldImpl} with {@code owner=false}; it does not
     * {@link #tryBecomeOwner} or pin the overshoot slot. Skipped when {@code used} has dropped
     * since the head parked, or when another rescued hold is still live.
     */
    private AdmissionGate.RescueResult rescueHeadLocked(@Nullable Executor delivery) {
        failTerminalWaitersLocked();
        while (waiters.isEmpty() == false) {
            TicketWaiter head = waiters.peekFirst();
            Exception terminal = terminalFailure(head.lease, head.cancel);
            if (terminal != null) {
                waiters.removeFirst();
                head.fail(terminal);
                continue;
            }
            HoldImpl charged = tryChargeLocked(head.bytes, head.lease, true);
            if (charged != null) {
                waiters.removeFirst();
                head.complete(charged, deliveryFor(head, delivery));
                return AdmissionGate.RescueResult.REGRANT;
            }
            if (used.get() < head.usedAtPark) {
                head.usedAtPark = used.get();
                return AdmissionGate.RescueResult.NONE;
            }
            if (rescueHolds > 0) {
                return AdmissionGate.RescueResult.NONE;
            }
            long next = used.get() + head.bytes;
            if (next < 0L) {
                throw new EsRejectedExecutionException("parquet I/O byte reservation overflow");
            }
            setUsed(next);
            rescueHolds++;
            waiters.removeFirst();
            head.complete(new HoldImpl(this, head.bytes, head.lease, false, true), deliveryFor(head, delivery));
            return AdmissionGate.RescueResult.OVER_CAP;
        }
        return AdmissionGate.RescueResult.NONE;
    }

    private void releaseRescueHold() {
        lock.lock();
        try {
            if (rescueHolds > 0) {
                rescueHolds--;
            }
        } finally {
            lock.unlock();
        }
    }

    private static Executor deliveryFor(TicketWaiter head, @Nullable Executor delivery) {
        return delivery != null ? delivery : head.executor;
    }

    private void failTerminalWaitersLocked() {
        Iterator<TicketWaiter> it = waiters.iterator();
        while (it.hasNext()) {
            TicketWaiter waiter = it.next();
            Exception terminal = terminalFailure(waiter.lease, waiter.cancel);
            if (terminal != null) {
                it.remove();
                waiter.fail(terminal);
            }
        }
    }

    @Nullable
    private static Exception terminalFailure(RowGroupIo lease, BooleanSupplier cancel) {
        if (lease != null && lease.isFinished()) {
            return finished();
        }
        if (cancel.getAsBoolean() || (lease != null && lease.isCancelled())) {
            return cancelled();
        }
        return null;
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

    public static EsRejectedExecutionException cancelled() {
        return NodeByteBudget.cancelled();
    }

    public static EsRejectedExecutionException finished() {
        return NodeByteBudget.finished();
    }

    private final class TicketWaiter {
        private final long bytes;
        private final RowGroupIo lease;
        private final BooleanSupplier cancel;
        private final Executor executor;
        private final SubscribableListener<Hold> listener;
        private long usedAtPark;
        private final AtomicBoolean completed = new AtomicBoolean();
        private AdmissionTracker.Wait tracked = AdmissionTracker.NOOP_WAIT;

        private TicketWaiter(long bytes, RowGroupIo lease, BooleanSupplier cancel, Executor executor, SubscribableListener<Hold> listener) {
            this.bytes = bytes;
            this.lease = lease;
            this.cancel = cancel;
            this.executor = executor;
            this.listener = listener;
            this.usedAtPark = used.get();
        }

        private void complete(HoldImpl hold, Executor exec) {
            // Stamp grant time at the decision, before the delivery fork. An undelivered grant
            // sitting on a saturated pool must not look like "no grant" to the watchdog.
            tracked.granted();
            pendingCompletions.add(() -> fork(exec, () -> deliver(hold), hold));
        }

        private void completeInline(HoldImpl hold) {
            tracked.granted();
            pendingCompletions.add(() -> deliver(hold));
        }

        private void deliver(HoldImpl hold) {
            if (completed.compareAndSet(false, true) == false) {
                discardUndelivered(hold);
                return;
            }
            Exception terminal = terminalFailure(lease, cancel);
            if (terminal != null) {
                tracked.finished();
                discardUndelivered(hold);
                listener.onFailure(terminal);
                return;
            }
            listener.onResponse(hold);
        }

        private void fail(Exception e) {
            pendingCompletions.add(() -> fork(executor, () -> {
                if (completed.compareAndSet(false, true)) {
                    tracked.finished();
                    listener.onFailure(e);
                }
            }, null));
        }

        private void fork(Executor exec, Runnable task, @Nullable HoldImpl holdOnReject) {
            try {
                exec.execute(task);
            } catch (Exception e) {
                if (holdOnReject != null) {
                    discardUndelivered(holdOnReject);
                }
                if (completed.compareAndSet(false, true)) {
                    tracked.finished();
                    listener.onFailure(e);
                }
            }
        }
    }

    /**
     * A hold that never reached the caller. Drop bytes and, if this lease took the overshoot
     * slot, clear it so a cancelled grant cannot pin the node until iterator close.
     */
    private void discardUndelivered(HoldImpl hold) {
        hold.close();
        if (hold.isOvershoot() && hold.lease() != null) {
            clearOwner(hold.lease());
        }
    }

    static final class HoldImpl implements Hold {
        private final NodeByteBudgetService budget;
        private final long bytes;
        private final RowGroupIo lease;
        private final boolean overshoot;
        private final boolean rescued;
        private final AtomicLong remaining;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean rescueReleased = new AtomicBoolean();

        private HoldImpl(NodeByteBudgetService budget, long bytes, RowGroupIo lease, boolean overshoot) {
            this(budget, bytes, lease, overshoot, false);
        }

        private HoldImpl(NodeByteBudgetService budget, long bytes, RowGroupIo lease, boolean overshoot, boolean rescued) {
            this.budget = budget;
            this.bytes = bytes;
            this.lease = lease;
            this.overshoot = overshoot;
            this.rescued = rescued;
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
        public void grow(long growBytes) {
            if (growBytes <= 0L || closed.get()) {
                return;
            }
            remaining.addAndGet(growBytes);
            budget.add(growBytes);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true) == false) {
                return;
            }
            // Charge only. Overshoot owner stays until clearOwner(lease): force-added
            // buffers can still sit in used, and a second over-cap unit must queue.
            drop(Long.MAX_VALUE);
            if (rescued && rescueReleased.compareAndSet(false, true)) {
                budget.releaseRescueHold();
            }
        }
    }
}
