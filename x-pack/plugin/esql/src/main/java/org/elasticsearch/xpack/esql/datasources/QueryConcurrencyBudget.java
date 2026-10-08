/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;
import org.elasticsearch.xpack.esql.datasources.spi.QueryAdmission;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupScheduler;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * Per-query concurrency budget that limits the number of concurrent in-flight storage API requests
 * for a single query. Budgets are dynamically resizable: the {@link ConcurrencyBudgetAllocator}
 * adjusts each budget's max permits as queries start and finish to maintain fair-share allocation.
 * <p>
 * When waiters are queued, {@link #choose} grants the next permit to the row-group lease closest
 * to done (fewest remaining GETs) once it leads by {@link #PREEMPT_GAP}, otherwise to the oldest
 * bound lease. Null leases stay FIFO among themselves and never become favoured, except a null
 * waiter that has sat for {@link #NULL_LEASE_MAX_WAIT_MS} takes the next grant.
 */
class QueryConcurrencyBudget implements Closeable, RowGroupScheduler {

    private static final Logger logger = LogManager.getLogger(QueryConcurrencyBudget.class);

    /** Outstanding-GET lead required to unseat the incumbent or skip the oldest startSeq. */
    static final int PREEMPT_GAP = 2;

    /**
     * How long a null-lease waiter may sit beside real-lease waiters before taking the next grant.
     * Code constant, not a cluster {@code Setting}.
     */
    static final long NULL_LEASE_MAX_WAIT_MS = 5_000L;

    private final ReentrantLock lock = new ReentrantLock(true);
    private int inFlight;
    private volatile int maxPermits;
    private final long acquireTimeoutMs;
    private final ConcurrencyBudgetAllocator allocator;
    private final AdmissionTracker tracker;
    private volatile boolean closed;

    private final AtomicLong lastWarnLogTime = new AtomicLong(0);
    private static final long WARN_LOG_INTERVAL_MS = 30_000;
    private static final long WARN_WAIT_THRESHOLD_MS = 5_000;

    private final Set<RowGroupIo> registry = new LinkedHashSet<>();
    private final Set<Waiter> waiters = new LinkedHashSet<>();
    private final ArrayList<Runnable> pendingCompletions = new ArrayList<>();
    private long nextStartSeq;
    private RowGroupIo favoured;
    private RowGroupIo pinnedLease;

    // Shared singleton for the disabled/unlimited case. Because acquire() short-circuits on
    // maxPermits <= 0, none of the mutable state (lock, condition, closed) is ever exercised.
    // Closing this instance is harmless (and must remain so).
    static final QueryConcurrencyBudget UNLIMITED = new QueryConcurrencyBudget(0, QueryAdmission.DEFAULT_ACQUIRE_TIMEOUT_MS, null);

    private static TaskCancelledException cancelled() {
        return new TaskCancelledException("Cancelled while waiting for permit");
    }

    QueryConcurrencyBudget(int maxPermits, long acquireTimeoutMs, ConcurrencyBudgetAllocator allocator) {
        this(maxPermits, acquireTimeoutMs, allocator, AdmissionTracker.NOOP);
    }

    QueryConcurrencyBudget(int maxPermits, long acquireTimeoutMs, ConcurrencyBudgetAllocator allocator, AdmissionTracker tracker) {
        this.maxPermits = maxPermits;
        this.acquireTimeoutMs = acquireTimeoutMs;
        this.allocator = allocator;
        this.tracker = tracker == null ? AdmissionTracker.NOOP : tracker;
    }

    long acquireTimeoutMs() {
        return acquireTimeoutMs;
    }

    private String budgetGate() {
        return allocator == null ? AdmissionTracker.GATE_BUDGET : allocator.name();
    }

    /**
     * Registers {@code io} so later grants and overshoot pins can rank it. Assigns {@code startSeq}
     * once. A second bind of the same instance is a no-op besides re-attaching this scheduler.
     */
    void bind(RowGroupIo io) {
        if (io == null || maxPermits <= 0) {
            return;
        }
        lock.lock();
        try {
            if (closed) {
                return;
            }
            io.attachScheduler(this);
            if (registry.contains(io) == false) {
                io.setStartSeq(nextStartSeq++);
                registry.add(io);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Acquires a permit, blocking if the query is at its budget limit. Throws immediately if the
     * budget has been closed. Null-lease form used by streams and callers with no row-group scope.
     */
    void acquire() throws TimeoutException, InterruptedException {
        acquire(null, false);
    }

    void acquire(RowGroupIo lease) throws TimeoutException, InterruptedException {
        acquire(lease, false);
    }

    void acquire(RowGroupIo lease, boolean countGets) throws TimeoutException, InterruptedException {
        if (maxPermits <= 0) {
            return;
        }
        if (closed) {
            throw new TimeoutException("Budget is closed");
        }
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMs);
        lock.lock();
        try {
            if (closed) {
                throw new TimeoutException("Budget was closed while waiting for permit");
            }
            if (lease != null && lease.isFinished()) {
                throw new TimeoutException("Row group lease was finished while waiting for permit");
            }
            if (waiters.isEmpty() && inFlight < maxPermits) {
                takePermit(lease, countGets);
                return;
            }
            Waiter waiter = new Waiter(lease, countGets);
            waiters.add(waiter);
            AdmissionTracker.Wait tracked = tracker.waitStarted(budgetGate(), budgetWaiterLabel(lease));
            try {
                while (waiter.granted == false) {
                    if (closed) {
                        waiters.remove(waiter);
                        tracked.finished();
                        throw new TimeoutException("Budget was closed while waiting for permit");
                    }
                    if (lease != null && lease.isFinished()) {
                        waiters.remove(waiter);
                        tracked.finished();
                        throw new TimeoutException("Row group lease was finished while waiting for permit");
                    }
                    long waitNanos = deadlineNanos - System.nanoTime();
                    if (waitNanos <= 0) {
                        waiters.remove(waiter);
                        tracked.finished();
                        throw new TimeoutException(
                            "Timed out waiting for query concurrency budget permit after ["
                                + acquireTimeoutMs
                                + "]ms (max permits ["
                                + maxPermits
                                + "])"
                        );
                    }
                    waiter.condition.awaitNanos(waitNanos);
                }
                tracked.granted();
            } catch (InterruptedException e) {
                if (waiter.granted == false) {
                    waiters.remove(waiter);
                    tracked.finished();
                    throw e;
                }
                tracked.granted();
                Thread.currentThread().interrupt();
            }
        } finally {
            lock.unlock();
        }
        long waitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        if (waitMs > WARN_WAIT_THRESHOLD_MS) {
            long lastWarn = lastWarnLogTime.get();
            long now = System.currentTimeMillis();
            if (now - lastWarn > WARN_LOG_INTERVAL_MS && lastWarnLogTime.compareAndSet(lastWarn, now)) {
                logger.warn(
                    "per-query storage API request waited [{}]ms for concurrency budget permit (max permits [{}])",
                    waitMs,
                    maxPermits
                );
            }
        }
    }

    /**
     * Async permit ticket. Completes on grant; fails on close, lease finish, or cancel.
     * Contended grants are forked onto {@code executor}. Uncontended grants complete on the
     * caller. A wait ends on grant, cancel, or query close.
     */
    SubscribableListener<Void> acquireAsync(RowGroupIo lease, boolean countGets, BooleanSupplier cancelSignal, Executor executor) {
        SubscribableListener<Void> listener = new SubscribableListener<>();
        if (maxPermits <= 0) {
            listener.onResponse(null);
            return listener;
        }
        BooleanSupplier cancel = cancelSignal == null ? () -> false : cancelSignal;
        if (executor == null) {
            throw new IllegalArgumentException("executor is required");
        }
        if (closed) {
            listener.onFailure(new TimeoutException("Budget is closed"));
            return listener;
        }
        if (cancel.getAsBoolean() || (lease != null && (lease.isCancelled() || lease.isFinished()))) {
            listener.onFailure(cancelled());
            return listener;
        }
        List<Runnable> completions = List.of();
        Exception failNow = null;
        lock.lock();
        try {
            if (closed) {
                failNow = new TimeoutException("Budget was closed while waiting for permit");
            } else if (lease != null && lease.isFinished()) {
                failNow = cancelled();
            } else if (cancel.getAsBoolean() || (lease != null && lease.isCancelled())) {
                failNow = cancelled();
            } else if (waiters.isEmpty() && inFlight < maxPermits) {
                takePermit(lease, countGets);
                Waiter waiter = new Waiter(lease, countGets, listener, executor, cancel);
                waiter.completeGrantInline();
                completions = takePendingCompletions();
            } else {
                Waiter waiter = new Waiter(lease, countGets, listener, executor, cancel);
                waiter.tracked = tracker.waitStarted(budgetGate(), budgetWaiterLabel(lease));
                waiters.add(waiter);
                if (lease != null) {
                    lease.setWake(this, this::wakeAsyncWaiters);
                }
                grantIfSpare();
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

    void wakeAsyncWaiters() {
        List<Runnable> completions;
        lock.lock();
        try {
            failCancelledWaitersLocked();
            grantIfSpare();
            completions = takePendingCompletions();
        } finally {
            lock.unlock();
        }
        runCompletions(completions);
    }

    /**
     * Releases a permit, waking the waiter chosen by {@link #choose} when any are queued. Must be
     * paired with a preceding successful {@link #acquire()}.
     */
    void release() {
        release(null, false);
    }

    void release(RowGroupIo lease, boolean countGets) {
        if (maxPermits <= 0) {
            return;
        }
        List<Runnable> completions;
        lock.lock();
        try {
            assert inFlight > 0 : "release() called without a matching acquire(), inFlight=" + inFlight;
            if (inFlight > 0) {
                inFlight--;
            }
            if (countGets && lease != null) {
                lease.onGetComplete();
            }
            failCancelledWaitersLocked();
            grantIfSpare();
            completions = takePendingCompletions();
        } finally {
            lock.unlock();
        }
        runCompletions(completions);
    }

    /**
     * Dynamically adjusts the maximum permits. Only grants blocked waiters when the budget
     * increases to avoid thundering herd on shrink.
     */
    void updateMaxPermits(int newMax) {
        int old = maxPermits;
        maxPermits = newMax;
        if (newMax > old) {
            List<Runnable> completions;
            lock.lock();
            try {
                grantIfSpare();
                completions = takePendingCompletions();
            } finally {
                lock.unlock();
            }
            runCompletions(completions);
        }
    }

    int inFlight() {
        lock.lock();
        try {
            return inFlight;
        } finally {
            lock.unlock();
        }
    }

    int maxPermits() {
        return maxPermits;
    }

    boolean isClosed() {
        return closed;
    }

    boolean isEnabled() {
        return maxPermits > 0;
    }

    /**
     * Closes the budget, unblocking any waiting acquirers and deregistering from the allocator.
     * Copies the registry under the budget lock, unlocks, then {@link RowGroupIo#cancel()}s each
     * lease so wake runnables can take the watermark lock.
     */
    @Override
    public void close() {
        if (maxPermits <= 0) {
            return;
        }
        List<RowGroupIo> toCancel;
        List<Runnable> completions;
        lock.lock();
        try {
            closed = true;
            toCancel = new ArrayList<>(registry);
            Iterator<Waiter> it = waiters.iterator();
            while (it.hasNext()) {
                Waiter waiter = it.next();
                if (waiter.async != null) {
                    it.remove();
                    waiter.fail(new TimeoutException("Budget was closed while waiting for permit"));
                } else {
                    waiter.condition.signal();
                }
            }
            completions = takePendingCompletions();
        } finally {
            lock.unlock();
        }
        runCompletions(completions);
        for (RowGroupIo io : toCancel) {
            io.cancel();
        }
        if (allocator != null) {
            allocator.deregister(this);
        }
    }

    // ── RowGroupScheduler ──────────────────────────────────────────────────────

    @Override
    public boolean tryPinOvershoot(RowGroupIo io) {
        if (io == null || maxPermits <= 0) {
            return false;
        }
        lock.lock();
        try {
            if (io.isFinished() || closed) {
                return false;
            }
            if (pinnedLease != null && pinnedLease != io && pinnedLease.isFinished() == false) {
                return false;
            }
            RowGroupIo winner;
            if (favoured != null && favoured.isFinished() == false) {
                winner = favoured;
            } else {
                winner = winnerAmongRegistered();
            }
            if (winner != io) {
                return false;
            }
            favoured = io;
            pinnedLease = io;
            io.setPinned(true);
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unpin(RowGroupIo io) {
        if (io == null || maxPermits <= 0) {
            return;
        }
        lock.lock();
        try {
            if (pinnedLease == io) {
                pinnedLease = null;
                io.setPinned(false);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void finish(RowGroupIo io) {
        if (io == null) {
            return;
        }
        if (maxPermits <= 0) {
            io.markFinished();
            return;
        }
        List<Runnable> completions;
        lock.lock();
        try {
            if (favoured == io) {
                favoured = null;
            }
            if (pinnedLease == io) {
                pinnedLease = null;
            }
            registry.remove(io);
            io.markFinished();
            Iterator<Waiter> it = waiters.iterator();
            while (it.hasNext()) {
                Waiter waiter = it.next();
                if (waiter.lease == io) {
                    it.remove();
                    if (waiter.async != null) {
                        waiter.fail(cancelled());
                    } else {
                        waiter.condition.signal();
                    }
                }
            }
            completions = takePendingCompletions();
        } finally {
            lock.unlock();
        }
        runCompletions(completions);
    }

    private static String budgetWaiterLabel(RowGroupIo lease) {
        if (lease == null) {
            return Thread.currentThread().getName();
        }
        return "lease#" + lease.startSeq();
    }

    // ── grant policy ───────────────────────────────────────────────────────────

    private void takePermit(RowGroupIo lease, boolean countGets) {
        inFlight++;
        if (countGets && lease != null) {
            lease.onGetStart();
        }
    }

    private void grant(Waiter waiter) {
        waiters.remove(waiter);
        takePermit(waiter.lease, waiter.countGets);
        waiter.granted = true;
        if (waiter.async != null) {
            waiter.completeGrant();
        } else {
            waiter.condition.signal();
        }
    }

    private void grantIfSpare() {
        failCancelledWaitersLocked();
        while (closed == false && waiters.isEmpty() == false && inFlight < maxPermits) {
            Waiter chosen = choose(waiters);
            if (chosen == null) {
                return;
            }
            grant(chosen);
        }
    }

    private void failCancelledWaitersLocked() {
        Iterator<Waiter> it = waiters.iterator();
        while (it.hasNext()) {
            Waiter waiter = it.next();
            if (waiter.async == null) {
                continue;
            }
            if (waiter.cancel.getAsBoolean() || (waiter.lease != null && waiter.lease.isCancelled())) {
                it.remove();
                waiter.fail(cancelled());
            }
        }
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

    /**
     * Picks the next waiter. Outstanding is read at grant time, not copied onto the waiter.
     * {@code acquire(null)} is FIFO among null leases and never becomes {@code favoured}, except a
     * null waiter that has waited {@link #NULL_LEASE_MAX_WAIT_MS} takes one grant.
     * A live favoured lease (unfinished, and either waiting, still holding outstanding GETs, or
     * pinned) is not replaced just because it is mid-GET and absent from {@code waiting}.
     */
    Waiter choose(Collection<Waiter> waiting) {
        Waiter nullDue = oldestNullLeaseWaitingAtLeast(waiting, NULL_LEASE_MAX_WAIT_MS);
        if (nullDue != null) {
            return nullDue;
        }

        Waiter incumbent = waiterFor(waiting, favoured);
        boolean favouredLive = favoured != null
            && favoured.isFinished() == false
            && (incumbent != null || favoured.outstanding() > 0 || favoured.isPinned());
        if (favouredLive) {
            if (favoured.isPinned() == false) {
                Waiter challenger = closestOther(waiting, favoured);
                if (challenger != null && favoured.outstanding() - challenger.outstanding() >= PREEMPT_GAP) {
                    favoured = challenger.lease;
                    return challenger;
                }
            }
            if (incumbent != null) {
                return incumbent;
            }
            return grantWithoutUnseating(waiting);
        }
        Waiter oldest = smallestStartSeq(waiting);
        Waiter closest = smallestOutstanding(waiting);
        if (oldest == null) {
            return oldestNull(waiting);
        }
        if (oldest != closest && closest != null && oldest.outstanding() - closest.outstanding() >= PREEMPT_GAP) {
            favoured = closest.lease;
            return closest;
        }
        favoured = oldest.lease;
        return oldest;
    }

    /** Grants a waiter without changing {@link #favoured}. */
    private Waiter grantWithoutUnseating(Collection<Waiter> waiting) {
        Waiter oldest = smallestStartSeq(waiting);
        if (oldest == null) {
            return oldestNull(waiting);
        }
        Waiter closest = smallestOutstanding(waiting);
        if (oldest != closest && closest != null && oldest.outstanding() - closest.outstanding() >= PREEMPT_GAP) {
            return closest;
        }
        return oldest;
    }

    private static Waiter waiterFor(Collection<Waiter> waiting, RowGroupIo lease) {
        if (lease == null) {
            return null;
        }
        for (Waiter waiter : waiting) {
            if (waiter.lease == lease) {
                return waiter;
            }
        }
        return null;
    }

    private static Waiter closestOther(Collection<Waiter> waiting, RowGroupIo incumbent) {
        Waiter best = null;
        for (Waiter waiter : waiting) {
            if (waiter.lease == null || waiter.lease.isFinished() || waiter.lease == incumbent) {
                continue;
            }
            best = closer(best, waiter);
        }
        return best;
    }

    private static Waiter smallestStartSeq(Collection<Waiter> waiting) {
        Waiter oldest = null;
        for (Waiter waiter : waiting) {
            if (waiter.lease == null || waiter.lease.isFinished()) {
                continue;
            }
            if (oldest == null || waiter.lease.startSeq() < oldest.lease.startSeq()) {
                oldest = waiter;
            }
        }
        return oldest;
    }

    private static Waiter smallestOutstanding(Collection<Waiter> waiting) {
        Waiter best = null;
        for (Waiter waiter : waiting) {
            if (waiter.lease == null || waiter.lease.isFinished()) {
                continue;
            }
            best = closer(best, waiter);
        }
        return best;
    }

    private static Waiter closer(Waiter best, Waiter candidate) {
        if (best == null) {
            return candidate;
        }
        int byOutstanding = Integer.compare(candidate.outstanding(), best.outstanding());
        if (byOutstanding < 0) {
            return candidate;
        }
        if (byOutstanding == 0 && candidate.lease.startSeq() < best.lease.startSeq()) {
            return candidate;
        }
        return best;
    }

    private static Waiter oldestNull(Collection<Waiter> waiting) {
        Waiter oldest = null;
        for (Waiter waiter : waiting) {
            if (waiter.lease != null) {
                continue;
            }
            if (oldest == null || waiter.enqueueNanos < oldest.enqueueNanos) {
                oldest = waiter;
            }
        }
        return oldest;
    }

    private static Waiter oldestNullLeaseWaitingAtLeast(Collection<Waiter> waiting, long minWaitMs) {
        long minWaitNanos = TimeUnit.MILLISECONDS.toNanos(minWaitMs);
        long now = System.nanoTime();
        Waiter oldest = null;
        for (Waiter waiter : waiting) {
            if (waiter.lease != null) {
                continue;
            }
            if (now - waiter.enqueueNanos < minWaitNanos) {
                continue;
            }
            if (oldest == null || waiter.enqueueNanos < oldest.enqueueNanos) {
                oldest = waiter;
            }
        }
        return oldest;
    }

    private RowGroupIo winnerAmongRegistered() {
        RowGroupIo oldest = null;
        RowGroupIo closest = null;
        for (RowGroupIo io : registry) {
            if (io.isFinished()) {
                continue;
            }
            if (oldest == null || io.startSeq() < oldest.startSeq()) {
                oldest = io;
            }
            if (closest == null
                || io.outstanding() < closest.outstanding()
                || (io.outstanding() == closest.outstanding() && io.startSeq() < closest.startSeq())) {
                closest = io;
            }
        }
        if (oldest == null) {
            return null;
        }
        if (oldest != closest && closest != null && oldest.outstanding() - closest.outstanding() >= PREEMPT_GAP) {
            return closest;
        }
        return oldest;
    }

    // ── test accessors ─────────────────────────────────────────────────────────

    int waiterCount() {
        lock.lock();
        try {
            return waiters.size();
        } finally {
            lock.unlock();
        }
    }

    int boundLeaseCount() {
        lock.lock();
        try {
            return registry.size();
        } finally {
            lock.unlock();
        }
    }

    RowGroupIo favoured() {
        lock.lock();
        try {
            return favoured;
        } finally {
            lock.unlock();
        }
    }

    boolean isLockHeldByCurrentThread() {
        return lock.isHeldByCurrentThread();
    }

    /** Ages null waiters so {@link #NULL_LEASE_MAX_WAIT_MS} tests need not sleep. */
    void ageNullWaiters(long ms) {
        long delta = TimeUnit.MILLISECONDS.toNanos(ms);
        lock.lock();
        try {
            for (Waiter waiter : waiters) {
                if (waiter.lease == null) {
                    waiter.enqueueNanos -= delta;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    final class Waiter {
        final RowGroupIo lease;
        final boolean countGets;
        long enqueueNanos = System.nanoTime();
        final Condition condition = lock.newCondition();
        boolean granted;
        final SubscribableListener<Void> async;
        final Executor executor;
        final BooleanSupplier cancel;
        private final AtomicBoolean completed = new AtomicBoolean();
        private AdmissionTracker.Wait tracked = AdmissionTracker.NOOP_WAIT;

        Waiter(RowGroupIo lease, boolean countGets) {
            this(lease, countGets, null, null, () -> false);
        }

        Waiter(RowGroupIo lease, boolean countGets, SubscribableListener<Void> async, Executor executor, BooleanSupplier cancel) {
            this.lease = lease;
            this.countGets = countGets;
            this.async = async;
            this.executor = executor;
            this.cancel = cancel == null ? () -> false : cancel;
        }

        int outstanding() {
            return lease.outstanding();
        }

        void completeGrant() {
            pendingCompletions.add(() -> forkGrant(this::deliverGrant));
        }

        void completeGrantInline() {
            pendingCompletions.add(this::deliverGrant);
        }

        private void deliverGrant() {
            if (completed.compareAndSet(false, true) == false) {
                return;
            }
            if (cancel.getAsBoolean() || (lease != null && lease.isCancelled())) {
                tracked.finished();
                release(lease, countGets);
                async.onFailure(cancelled());
                return;
            }
            tracked.granted();
            async.onResponse(null);
        }

        void fail(Exception e) {
            pendingCompletions.add(() -> fork(() -> {
                if (completed.compareAndSet(false, true)) {
                    tracked.finished();
                    async.onFailure(e);
                }
            }));
        }

        private void forkGrant(Runnable task) {
            try {
                executor.execute(task);
            } catch (Exception e) {
                if (completed.compareAndSet(false, true)) {
                    tracked.finished();
                    release(lease, countGets);
                    async.onFailure(e);
                }
            }
        }

        private void fork(Runnable task) {
            try {
                executor.execute(task);
            } catch (Exception e) {
                if (completed.compareAndSet(false, true)) {
                    tracked.finished();
                    async.onFailure(e);
                }
            }
        }
    }
}
