/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Identity of one row group's remaining object-store GETs. The query budget grants permits to
 * the lease closest to done so a scan does not keep many groups open at once.
 * <p>
 * {@link #outstanding()} is the ranking key: footer-cache misses counted via {@link #addUnissued}
 * before the first acquire can block, then decremented when each async GET completes. Async
 * acquire/release move work between unissued and in-flight without changing outstanding.
 * Sync reads use the lease for grant order with {@code countGets=false} and do not touch these
 * counters.
 */
public final class RowGroupIo {

    private final AtomicInteger outstanding = new AtomicInteger();
    private final AtomicInteger unissued = new AtomicInteger();
    private final AtomicInteger inFlightGets = new AtomicInteger();
    private final AtomicReference<Runnable> wake = new AtomicReference<>();
    private volatile boolean cancelled;
    private volatile boolean finished;
    private volatile boolean pinned;
    private volatile long startSeq;
    private volatile RowGroupScheduler scheduler;

    /** Remaining GETs: not yet issued plus currently in flight. */
    public int outstanding() {
        return outstanding.get();
    }

    /**
     * Records {@code n} GETs that will be issued for this lease. Call once, before any acquire
     * can block, using the footer-cache miss count — not the merged-range count including hits.
     * {@code n == 0} is a no-op (all ranges were cache hits).
     */
    public void addUnissued(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("unissued GET count must be non-negative, got: " + n);
        }
        if (n == 0) {
            return;
        }
        unissued.addAndGet(n);
        outstanding.addAndGet(n);
    }

    /**
     * Drops {@code n} GETs that were counted in {@link #addUnissued} but will never start
     * (for example after {@code admitWait} fails and remaining misses are not issued).
     * Does not change in-flight; those GETs never called {@link #onGetStart}.
     */
    public void forgetUnissued(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("unissued GET count must be non-negative, got: " + n);
        }
        if (n == 0) {
            return;
        }
        unissued.updateAndGet(v -> v > n ? v - n : 0);
        outstanding.updateAndGet(v -> v > n ? v - n : 0);
    }

    /**
     * Moves one GET from unissued to in-flight. Called under the budget lock on async acquire.
     * Does not change {@link #outstanding()}.
     */
    public void onGetStart() {
        unissued.updateAndGet(v -> v > 0 ? v - 1 : v);
        inFlightGets.incrementAndGet();
    }

    /** Drops one in-flight GET. Called under the budget lock on async release. */
    public void onGetComplete() {
        inFlightGets.updateAndGet(v -> v > 0 ? v - 1 : v);
        outstanding.updateAndGet(v -> v > 0 ? v - 1 : v);
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public boolean isPinned() {
        return pinned;
    }

    public boolean isFinished() {
        return finished;
    }

    /** Monotonic sequence assigned at bind; older leases win ties when no gap of two exists. */
    public long startSeq() {
        return startSeq;
    }

    public RowGroupScheduler scheduler() {
        return scheduler;
    }

    /**
     * Binds this lease to {@code scheduler} so other packages and tests can pin/finish without
     * touching the package-private budget class. A second bind with a different scheduler is
     * rejected; the same scheduler may be attached again.
     */
    public void attachScheduler(RowGroupScheduler scheduler) {
        RowGroupScheduler existing = this.scheduler;
        if (existing != null && existing != scheduler) {
            throw new IllegalStateException("scheduler already attached");
        }
        this.scheduler = scheduler;
    }

    /** Assigns bind order. Called once by the budget under its lock. */
    public void setStartSeq(long startSeq) {
        this.startSeq = startSeq;
    }

    public void setPinned(boolean pinned) {
        this.pinned = pinned;
    }

    public void markFinished() {
        this.finished = true;
        this.pinned = false;
    }

    /**
     * Wake runnable invoked by {@link #cancel()}. The watermark wait installs this; the caller of
     * {@code cancel()} must not hold the budget lock, because the wake takes the watermark lock.
     * If this lease is already cancelled, {@code wake} runs immediately.
     */
    public void setWake(Runnable wake) {
        this.wake.set(wake);
        if (cancelled) {
            runAndClearWake();
        }
    }

    /**
     * Marks this lease cancelled and runs the wake runnable, if any. Must not be called while
     * holding the budget lock. A second cancel is a no-op for the wake.
     */
    public void cancel() {
        cancelled = true;
        runAndClearWake();
    }

    private void runAndClearWake() {
        Runnable w = wake.getAndSet(null);
        if (w != null) {
            w.run();
        }
    }

    /**
     * Pins this lease as the query's overshoot owner when the bound scheduler agrees.
     * Returns {@code false} when no scheduler is attached.
     */
    public boolean tryPinOvershoot() {
        RowGroupScheduler s = scheduler;
        return s != null && s.tryPinOvershoot(this);
    }

    /** Clears the overshoot pin if the bound scheduler currently has this lease pinned. */
    public void unpin() {
        RowGroupScheduler s = scheduler;
        if (s != null) {
            s.unpin(this);
        } else {
            setPinned(false);
        }
    }

    /**
     * Marks this lease finished and drops it from the bound scheduler's registry. The scheduler
     * must also unblock any {@code acquire} waiters for this lease. With no scheduler, only the
     * local finished flag is set.
     */
    public void finish() {
        RowGroupScheduler s = scheduler;
        if (s != null) {
            s.finish(this);
        } else {
            markFinished();
        }
    }
}
