/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xpack.esql.datasources.StorageRetryCancellation;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionGate;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Node-scoped admission limit on retained Parquet I/O bytes (prefetch buffers and sliding
 * windows). Copied from the ClickHouse parquet high-watermark shape: cap at {@code heap / 8},
 * shared by every query on the node. Crossing the limit does not fail the query; the REQUEST
 * circuit breaker remains the hard stop. Look-ahead is refused once {@code used + next} would
 * exceed the cap. One in-flight group may overshoot when it is larger than the remaining budget,
 * so a scan cannot stall; that overshoot is node-wide, not per iterator, and belongs to one
 * owner lease until {@link #clearOwner}. Look-ahead {@link #tryAdmit} still refuses rather than
 * fail the query. {@link #admitWaitUntil} in a coalesced PER_GET batch waits up to
 * {@link #DEFAULT_ADMIT_WAIT_MS} then charges so the REQUEST breaker can refuse, unless the
 * charge would pass {@link #FORCE_ADMIT_LIMIT_MULTIPLIER} times the cap. The owner overshoot
 * may still exceed that ceiling. {@link #admitWait} is the caller-supplied clock wrapper.
 * Lease cancellation still fails that GET with {@link EsRejectedExecutionException}. The
 * REQUEST circuit breaker remains the hard stop for allocation.
 */
final class ParquetIoWatermark implements AdmissionGate {

    private static final Logger logger = LogManager.getLogger(ParquetIoWatermark.class);

    static final int HEAP_DIVISOR = 8;

    /**
     * Waiting longer cannot help when bytes are released only by work queued behind the waiter
     * on the same compute pool. The REQUEST circuit breaker remains the hard stop. This is a
     * code constant, not a cluster Setting.
     */
    static final long DEFAULT_ADMIT_WAIT_MS = 1_000L;

    /**
     * Forced admits after the wait budget may charge up to this many times {@link #limit}.
     * The in-flight overshoot owner may already sit above this; waiters then fail instead of
     * stacking more bytes.
     */
    static final int FORCE_ADMIT_LIMIT_MULTIPLIER = 2;

    private static final long WARN_LOG_INTERVAL_MS = 30_000L;

    /**
     * How a coalesced GET batch charges this watermark. {@link #UNGATED} is a null hold's
     * {@link #forceAdd} (footer metadata, sliding window). {@link #GROUP_HOLD} is a footer
     * estimate already {@link #tryAdmit}ted. {@link #PER_GET} waits once per coalesced call
     * via {@link #admitWaitUntil} then charges. A null hold is never {@link #PER_GET}.
     */
    enum ByteGate {
        UNGATED,
        GROUP_HOLD,
        PER_GET
    }

    private final long limit;
    private final long admitWaitMs;
    private final AtomicLong used = new AtomicLong();
    private final AtomicInteger holds = new AtomicInteger();
    private final AtomicLong forcedAdmits = new AtomicLong();
    private final AtomicLong waitNanos = new AtomicLong();
    private final AtomicLong lastWarnLogTime = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private RowGroupIo overshootOwner;
    private volatile AdmissionTracker tracker = AdmissionTracker.NOOP;

    static ParquetIoWatermark forHeap() {
        long heapBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        return new ParquetIoWatermark(Math.max(1L, heapBytes / HEAP_DIVISOR));
    }

    ParquetIoWatermark(long limit) {
        this(limit, DEFAULT_ADMIT_WAIT_MS);
    }

    ParquetIoWatermark(long limit, long admitWaitMs) {
        if (limit < 1L) {
            throw new IllegalArgumentException("limit must be at least 1, got: " + limit);
        }
        if (admitWaitMs < 0L) {
            throw new IllegalArgumentException("admitWaitMs must be non-negative, got: " + admitWaitMs);
        }
        this.limit = limit;
        this.admitWaitMs = admitWaitMs;
    }

    void bindTracker(AdmissionTracker tracker) {
        this.tracker = tracker == null ? AdmissionTracker.NOOP : tracker;
        this.tracker.register(this);
    }

    /**
     * Attempts to reserve {@code bytes} of retained I/O. Refuses once {@code used + bytes} would
     * exceed the cap; the one overshoot is {@link #admitWaitUntil}. Returns {@code false} without
     * throwing; never a query failure.
     */
    boolean tryReserve(long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return true;
        }
        lock.lock();
        try {
            long current = used.get();
            long next = current + bytes;
            if (next < 0L || next > limit) {
                return false;
            }
            used.set(next);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@link #tryReserve} plus an {@link AdmitHold} so the footer estimate is swapped for
     * actual buffer sizes as they allocate, and leftover estimate is dropped when the prefetch
     * future settles. Returns {@code null} when admission refuses.
     */
    @Nullable
    AdmitHold tryAdmit(long bytes) {
        if (tryReserve(bytes) == false) {
            return null;
        }
        return new AdmitHold(this, bytes);
    }

    /**
     * Caller-supplied timeout wrapper around {@link #admitWaitUntil}. Tests use this; production
     * coalesced PER_GET shares a deadline via {@link #admitWaitUntil}.
     */
    AdmitHold admitWait(long bytes, RowGroupIo lease, long timeoutMs) {
        return admitWaitUntil(bytes, lease, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs));
    }

    /**
     * Blocks until {@code bytes} can be charged for {@code lease}, or {@code deadlineNanos} elapses.
     * On expiry the bytes are charged without taking the overshoot owner, unless that charge would
     * pass {@link #FORCE_ADMIT_LIMIT_MULTIPLIER} times {@link #limit}; then the GET fails with
     * {@link EsRejectedExecutionException}. Same-owner overshoot may still exceed that ceiling.
     * The REQUEST breaker remains the hard stop for allocation. Lease cancellation fails promptly
     * via {@link RowGroupIo#setWake}; ambient {@link StorageRetryCancellation} is sampled outside
     * this lock and fails only once the deadline has elapsed.
     * <p>
     * Lock order: this watermark lock, then the budget lock inside
     * {@link RowGroupIo#tryPinOvershoot()}. Never the reverse. {@link #clearOwner} runs after
     * the budget {@code finish()} has released its lock.
     */
    AdmitHold admitWaitUntil(long bytes, RowGroupIo lease, long deadlineNanos) {
        if (lease == null) {
            throw new IllegalArgumentException("lease is required");
        }
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return new AdmitHold(this, 0L);
        }
        boolean enteredWait = false;
        boolean forced = false;
        boolean success = false;
        long waitStartedNanos = 0L;
        boolean ambientCancelled = StorageRetryCancellation.isCancelled();
        AdmissionTracker.Wait trackedWait = AdmissionTracker.NOOP_WAIT;
        AdmitHold hold = null;
        try {
            lock.lock();
            try {
                while (true) {
                    if (lease.isCancelled()) {
                        throw cancelled();
                    }
                    long current = used.get();
                    long next = current + bytes;
                    if (next < 0L) {
                        throw new EsRejectedExecutionException("parquet I/O byte reservation overflow");
                    }
                    if (next <= limit) {
                        used.set(next);
                        hold = new AdmitHold(this, bytes);
                        break;
                    }
                    if (overshootOwner == lease) {
                        used.set(next);
                        hold = new AdmitHold(this, bytes);
                        break;
                    }
                    if (overshootOwner == null) {
                        // Expired wait plus ambient cancel must not pin the node-wide overshoot slot.
                        // Under-cap and same-owner admits above still proceed; admitWaitMs==0 can still
                        // take a vacant owner when not cancelled.
                        if (deadlineNanos - System.nanoTime() <= 0L && ambientCancelled) {
                            throw cancelled();
                        }
                        if (tryBecomeOwner(lease, next)) {
                            hold = new AdmitHold(this, bytes);
                            break;
                        }
                    }
                    lease.setWake(this::signalWaiters);
                    if (lease.isCancelled()) {
                        throw cancelled();
                    }
                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0L) {
                        if (ambientCancelled) {
                            throw cancelled();
                        }
                        if (next > forceAdmitLimit()) {
                            throw overForceLimit(bytes, next);
                        }
                        used.set(next);
                        forcedAdmits.incrementAndGet();
                        forced = true;
                        hold = new AdmitHold(this, bytes);
                        break;
                    }
                    if (enteredWait == false) {
                        enteredWait = true;
                        waitStartedNanos = System.nanoTime();
                        trackedWait = tracker.waitStarted(AdmissionTracker.GATE_BYTES, Thread.currentThread().getName());
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
                success = true;
            } finally {
                if (enteredWait) {
                    waitNanos.addAndGet(System.nanoTime() - waitStartedNanos);
                }
                lock.unlock();
            }
        } finally {
            if (success) {
                trackedWait.granted();
            } else {
                trackedWait.finished();
            }
        }
        if (forced) {
            maybeLogForcedAdmit(bytes);
        }
        return hold;
    }

    long forceAdmitLimit() {
        if (limit > Long.MAX_VALUE / FORCE_ADMIT_LIMIT_MULTIPLIER) {
            return Long.MAX_VALUE;
        }
        return limit * (long) FORCE_ADMIT_LIMIT_MULTIPLIER;
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

    /**
     * Caller holds the watermark lock. Budget lock is taken inside {@code tryPinOvershoot} /
     * {@code unpin} and released before this returns. A null scheduler (file:// and
     * {@code max_concurrent_requests=0}) assigns the owner without pinning.
     */
    private boolean tryBecomeOwner(RowGroupIo lease, long nextUsed) {
        if (lease.scheduler() != null) {
            if (lease.tryPinOvershoot()) {
                overshootOwner = lease;
                lease.setWake(this::signalWaiters);
                used.set(nextUsed);
                return true;
            }
            lease.unpin();
            return false;
        }
        overshootOwner = lease;
        used.set(nextUsed);
        return true;
    }

    /**
     * Drops {@code lease} as the node-wide overshoot owner if it currently holds that slot.
     * Does not release bytes; those already returned from {@link DirectReadBuffer#close()}.
     * No-op when another lease is the owner. Signals waiters. Call after budget {@code finish()}
     * has released the budget lock.
     */
    void clearOwner(RowGroupIo lease) {
        if (lease == null) {
            return;
        }
        lock.lock();
        try {
            if (overshootOwner == lease) {
                overshootOwner = null;
                notFull.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String name() {
        return AdmissionTracker.GATE_BYTES;
    }

    @Override
    public int holders() {
        int live = Math.max(0, holds.get());
        if (live > 0) {
            return live;
        }
        return used.get() > 0L ? 1 : 0;
    }

    @Override
    public String holderSummary() {
        RowGroupIo owner = overshootOwner();
        String ownerLabel = owner == null ? "none" : "lease#" + owner.startSeq();
        return "used=" + used.get() + "/" + limit + " owner=" + ownerLabel;
    }

    @Nullable
    RowGroupIo overshootOwner() {
        lock.lock();
        try {
            return overshootOwner;
        } finally {
            lock.unlock();
        }
    }

    private void signalWaiters() {
        lock.lock();
        try {
            notFull.signalAll();
        } finally {
            lock.unlock();
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

    private static EsRejectedExecutionException cancelled() {
        return new EsRejectedExecutionException("Cancelled while waiting for parquet I/O bytes");
    }

    /**
     * Unconditional charge used for buffers that must exist (sliding window on first use, actual
     * coalesced {@code DirectReadBuffer} size). Admission of look-ahead happens in
     * {@link #tryReserve}. When a prefetch already {@link #tryAdmit}ted a footer estimate,
     * {@link #accountingFactory(CircuitBreaker, AdmitHold)} drops that many estimate bytes on
     * each alloc so in-flight sibling GETs keep their hold until they allocate. Does not set
     * the overshoot owner.
     */
    void forceAdd(long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return;
        }
        lock.lock();
        try {
            used.addAndGet(bytes);
        } finally {
            lock.unlock();
        }
    }

    void release(long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return;
        }
        lock.lock();
        try {
            used.updateAndGet(current -> {
                long next = current - bytes;
                return next < 0L ? 0L : next;
            });
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    long used() {
        return used.get();
    }

    long limit() {
        return limit;
    }

    long admitWaitMs() {
        return admitWaitMs;
    }

    long forcedAdmits() {
        return forcedAdmits.get();
    }

    long waitNanos() {
        return waitNanos.get();
    }

    DirectBufferFactory accountingFactory(CircuitBreaker breaker) {
        return accountingFactory(breaker, null);
    }

    /**
     * Factory that charges this watermark with the actual allocated length beside the REQUEST
     * breaker, and releases both on {@link DirectReadBuffer#close()}. {@code admitHold} is a
     * {@link #tryAdmit} estimate; each alloc drops that many leftover estimate bytes so a
     * coalesced group of many GETs does not open a look-ahead hole after the first buffer.
     * {@link AdmitHold#drop()} clears any remainder when the prefetch future settles.
     */
    DirectBufferFactory accountingFactory(CircuitBreaker breaker, @Nullable AdmitHold admitHold) {
        DirectBufferFactory inner = DirectBufferFactory.forBreaker(breaker);
        return len -> {
            DirectReadBuffer allocated = inner.allocate(len);
            DirectReadBuffer wrapped = null;
            try {
                wrapped = account(allocated, len);
                if (admitHold != null) {
                    admitHold.drop(len);
                }
                return wrapped;
            } catch (Throwable t) {
                try {
                    if (wrapped != null) {
                        wrapped.close();
                    } else {
                        allocated.close();
                    }
                } catch (Throwable closeFailure) {
                    t.addSuppressed(closeFailure);
                }
                throw t;
            }
        };
    }

    private DirectReadBuffer account(DirectReadBuffer inner, int length) {
        AtomicBoolean released = new AtomicBoolean();
        DirectReadBuffer wrapped = new DirectReadBuffer(inner.buffer(), () -> {
            try {
                inner.close();
            } finally {
                if (released.compareAndSet(false, true)) {
                    release(length);
                }
            }
        });
        forceAdd(length);
        return wrapped;
    }

    static DirectBufferFactory bufferFactory(CircuitBreaker breaker, @Nullable ParquetIoWatermark watermark) {
        return bufferFactory(breaker, watermark, null);
    }

    static DirectBufferFactory bufferFactory(
        CircuitBreaker breaker,
        @Nullable ParquetIoWatermark watermark,
        @Nullable AdmitHold admitHold
    ) {
        return watermark == null ? DirectBufferFactory.forBreaker(breaker) : watermark.accountingFactory(breaker, admitHold);
    }

    /**
     * Footer-estimate reservation released as real buffers allocate ({@link #drop(long)}) and
     * cleared when the prefetch future settles ({@link #drop()}).
     */
    static final class AdmitHold {
        private final ParquetIoWatermark watermark;
        private final AtomicLong remaining;
        private final boolean counted;

        private AdmitHold(ParquetIoWatermark watermark, long bytes) {
            this.watermark = watermark;
            long reserved = Math.max(0L, bytes);
            this.remaining = new AtomicLong(reserved);
            this.counted = reserved > 0L;
            if (counted) {
                watermark.holds.incrementAndGet();
            }
        }

        /**
         * Drops up to {@code bytes} of leftover estimate, swapping that slice for a retained
         * array charged by {@link #forceAdd}. Sibling in-flight ranges keep their estimate.
         */
        void drop(long bytes) {
            if (bytes <= 0L) {
                return;
            }
            while (true) {
                long current = remaining.get();
                if (current <= 0L) {
                    return;
                }
                long release = Math.min(current, bytes);
                if (remaining.compareAndSet(current, current - release)) {
                    watermark.release(release);
                    if (current - release == 0L && counted) {
                        watermark.holds.decrementAndGet();
                    }
                    return;
                }
            }
        }

        void drop() {
            drop(Long.MAX_VALUE);
        }
    }
}
