/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.QueryAdmission;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupScheduler;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
    private volatile boolean closed;

    private final AtomicLong lastWarnLogTime = new AtomicLong(0);
    private static final long WARN_LOG_INTERVAL_MS = 30_000;
    private static final long WARN_WAIT_THRESHOLD_MS = 5_000;

    private final List<RowGroupIo> registry = new ArrayList<>();
    private final List<Waiter> waiters = new ArrayList<>();
    private long nextStartSeq;
    private RowGroupIo favoured;
    private RowGroupIo pinnedLease;

    // Shared singleton for the disabled/unlimited case. Because acquire() short-circuits on
    // maxPermits <= 0, none of the mutable state (lock, condition, closed) is ever exercised.
    // Closing this instance is harmless (and must remain so).
    static final QueryConcurrencyBudget UNLIMITED = new QueryConcurrencyBudget(0, QueryAdmission.DEFAULT_ACQUIRE_TIMEOUT_MS, null);

    QueryConcurrencyBudget(int maxPermits, long acquireTimeoutMs, ConcurrencyBudgetAllocator allocator) {
        this.maxPermits = maxPermits;
        this.acquireTimeoutMs = acquireTimeoutMs;
        this.allocator = allocator;
    }

    long acquireTimeoutMs() {
        return acquireTimeoutMs;
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
            if (waiters.isEmpty() && inFlight < maxPermits) {
                takePermit(lease, countGets);
                return;
            }
            Waiter waiter = new Waiter(lease, countGets);
            waiters.add(waiter);
            try {
                while (waiter.granted == false) {
                    if (closed) {
                        waiters.remove(waiter);
                        throw new TimeoutException("Budget was closed while waiting for permit");
                    }
                    long waitNanos = deadlineNanos - System.nanoTime();
                    if (waitNanos <= 0) {
                        waiters.remove(waiter);
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
            } catch (InterruptedException e) {
                if (waiter.granted == false) {
                    waiters.remove(waiter);
                    throw e;
                }
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
        lock.lock();
        try {
            assert inFlight > 0 : "release() called without a matching acquire(), inFlight=" + inFlight;
            if (inFlight > 0) {
                inFlight--;
            }
            if (countGets && lease != null) {
                lease.onGetComplete();
            }
            grantIfSpare();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Dynamically adjusts the maximum permits. Only grants blocked waiters when the budget
     * increases to avoid thundering herd on shrink.
     */
    void updateMaxPermits(int newMax) {
        int old = maxPermits;
        maxPermits = newMax;
        if (newMax > old) {
            lock.lock();
            try {
                grantIfSpare();
            } finally {
                lock.unlock();
            }
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
        closed = true;
        List<RowGroupIo> toCancel;
        lock.lock();
        try {
            toCancel = new ArrayList<>(registry);
            for (Waiter waiter : waiters) {
                waiter.condition.signal();
            }
        } finally {
            lock.unlock();
        }
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
        } finally {
            lock.unlock();
        }
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
        waiter.condition.signal();
    }

    private void grantIfSpare() {
        while (closed == false && waiters.isEmpty() == false && inFlight < maxPermits) {
            Waiter chosen = choose(waiters);
            if (chosen == null) {
                return;
            }
            grant(chosen);
        }
    }

    /**
     * Picks the next waiter. Outstanding is read at grant time, not copied onto the waiter.
     * {@code acquire(null)} is FIFO among null leases and never becomes {@code favoured}, except a
     * null waiter that has waited {@link #NULL_LEASE_MAX_WAIT_MS} takes one grant.
     */
    Waiter choose(List<Waiter> waiting) {
        Waiter nullDue = oldestNullLeaseWaitingAtLeast(waiting, NULL_LEASE_MAX_WAIT_MS);
        if (nullDue != null) {
            return nullDue;
        }

        Waiter incumbent = waiterFor(waiting, favoured);
        if (incumbent != null && favoured.isFinished() == false) {
            if (favoured.isPinned() == false) {
                Waiter challenger = closestOther(waiting, favoured);
                if (challenger != null && favoured.outstanding() - challenger.outstanding() >= PREEMPT_GAP) {
                    favoured = challenger.lease;
                    return challenger;
                }
            }
            return incumbent;
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

    private static Waiter waiterFor(List<Waiter> waiting, RowGroupIo lease) {
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

    private static Waiter closestOther(List<Waiter> waiting, RowGroupIo incumbent) {
        Waiter best = null;
        for (Waiter waiter : waiting) {
            if (waiter.lease == null || waiter.lease.isFinished() || waiter.lease == incumbent) {
                continue;
            }
            best = closer(best, waiter);
        }
        return best;
    }

    private static Waiter smallestStartSeq(List<Waiter> waiting) {
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

    private static Waiter smallestOutstanding(List<Waiter> waiting) {
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

    private static Waiter oldestNull(List<Waiter> waiting) {
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

    private static Waiter oldestNullLeaseWaitingAtLeast(List<Waiter> waiting, long minWaitMs) {
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

        Waiter(RowGroupIo lease, boolean countGets) {
            this.lease = lease;
            this.countGets = countGets;
        }

        int outstanding() {
            return lease.outstanding();
        }
    }
}
