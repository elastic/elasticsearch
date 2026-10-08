/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.NodeByteBudgetService;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionGate;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.HeapFootprint;
import org.elasticsearch.xpack.esql.datasources.spi.NodeByteBudget;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Node-scoped admission limit on retained Parquet I/O bytes (prefetch buffers and sliding
 * windows). Copied from the ClickHouse parquet high-watermark shape: cap at {@code heap / 8},
 * shared by every query on the node. Crossing the limit does not fail the query; the REQUEST
 * circuit breaker remains the hard stop. Look-ahead is refused once {@code used + next} would
 * exceed the cap. One in-flight group may overshoot when it is larger than the remaining budget,
 * so a scan cannot stall; that overshoot is node-wide, not per iterator, and belongs to one
 * owner lease until {@link #clearOwner}. Look-ahead {@link #tryAdmit} still refuses rather than
 * fail the query. Coalesced PER_GET draws a whole-unit {@link NodeByteBudget} ticket. The
 * parking {@link #admitWaitUntil} path remains for leftover parquet column iterator tests
 * until the hard cap lands. The REQUEST circuit breaker remains the hard stop for allocation.
 */
final class ParquetIoWatermark implements AdmissionGate {

    static final int HEAP_DIVISOR = NodeByteBudgetService.HEAP_DIVISOR;

    /**
     * Waiting longer cannot help when bytes are released only by work queued behind the waiter
     * on the same compute pool. The REQUEST circuit breaker remains the hard stop. This is a
     * code constant, not a cluster Setting.
     */
    static final long DEFAULT_ADMIT_WAIT_MS = NodeByteBudgetService.DEFAULT_ADMIT_WAIT_MS;

    /**
     * Forced admits after the wait budget may charge up to this many times {@link #limit}.
     * The in-flight overshoot owner may already sit above this; waiters then fail instead of
     * stacking more bytes.
     */
    static final int FORCE_ADMIT_LIMIT_MULTIPLIER = NodeByteBudgetService.FORCE_ADMIT_LIMIT_MULTIPLIER;

    /**
     * How a coalesced GET batch charges this watermark. {@link #UNGATED} is a null hold's
     * {@link #forceAdd} (footer metadata, sliding window). {@link #GROUP_HOLD} is a footer
     * estimate already {@link #tryAdmit}ted. {@link #PER_GET} draws one unit ticket covering
     * every miss in the coalesced call. A null hold is never {@link #PER_GET}.
     */
    enum ByteGate {
        UNGATED,
        GROUP_HOLD,
        PER_GET
    }

    private final NodeByteBudgetService budget;
    private final AtomicInteger holds = new AtomicInteger();
    private volatile AdmissionTracker tracker = AdmissionTracker.NOOP;

    static ParquetIoWatermark forHeap() {
        return new ParquetIoWatermark(NodeByteBudgetService.forHeap());
    }

    ParquetIoWatermark(long limit) {
        this(limit, DEFAULT_ADMIT_WAIT_MS);
    }

    ParquetIoWatermark(long limit, long admitWaitMs) {
        this(new NodeByteBudgetService(limit, admitWaitMs));
    }

    ParquetIoWatermark(NodeByteBudget nodeByteBudget) {
        if (nodeByteBudget instanceof NodeByteBudgetService service) {
            this.budget = service;
        } else {
            throw new IllegalArgumentException("node byte budget must be a NodeByteBudgetService");
        }
    }

    NodeByteBudget nodeByteBudget() {
        return budget;
    }

    void bindTracker(AdmissionTracker tracker) {
        this.tracker = tracker == null ? AdmissionTracker.NOOP : tracker;
        this.tracker.register(this);
        budget.bindTracker(this.tracker);
    }

    int waiterCount() {
        return budget.waiterCount();
    }

    /**
     * Attempts to reserve {@code bytes} of retained I/O. Refuses once {@code used + bytes} would
     * exceed the cap; the one overshoot is a ticket. Returns {@code false} without throwing;
     * never a query failure.
     */
    boolean tryReserve(long bytes) {
        NodeByteBudget.Hold hold = budget.tryAdmit(bytes);
        return hold != null;
    }

    /**
     * {@link #tryAdmit} plus an {@link AdmitHold} so the footer estimate is swapped for
     * actual buffer sizes as they allocate, and leftover estimate is dropped when the prefetch
     * future settles. Returns {@code null} when admission refuses.
     */
    @Nullable
    AdmitHold tryAdmit(long bytes) {
        NodeByteBudget.Hold hold = budget.tryAdmit(bytes);
        return hold == null ? null : new AdmitHold(this, hold);
    }

    AdmitHold wrap(NodeByteBudget.Hold hold) {
        return new AdmitHold(this, hold);
    }

    /**
     * Caller-supplied timeout wrapper around {@link #admitWaitUntil}. Tests use this; production
     * coalesced PER_GET uses a unit ticket instead.
     */
    AdmitHold admitWait(long bytes, RowGroupIo lease, long timeoutMs) {
        return admitWaitUntil(bytes, lease, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs));
    }

    /**
     * Blocks until {@code bytes} can be charged for {@code lease}, or {@code deadlineNanos} elapses.
     * Leftover OPCI / characterization tests only; production CRR no longer parks here.
     */
    AdmitHold admitWaitUntil(long bytes, RowGroupIo lease, long deadlineNanos) {
        AdmissionTracker.Wait trackedWait = tracker.waitStarted(AdmissionTracker.GATE_BYTES, Thread.currentThread().getName());
        boolean success = false;
        try {
            AdmitHold hold = new AdmitHold(this, budget.admitWaitUntil(bytes, lease, deadlineNanos));
            success = true;
            return hold;
        } finally {
            if (success) {
                trackedWait.granted();
            } else {
                trackedWait.finished();
            }
        }
    }

    long forceAdmitLimit() {
        return budget.forceAdmitLimit();
    }

    /**
     * Drops {@code lease} as the node-wide overshoot owner if it currently holds that slot.
     * Does not release bytes; those already returned from {@link DirectReadBuffer#close()}.
     * No-op when another lease is the owner. Signals waiters. Call after budget {@code finish()}
     * has released the budget lock.
     */
    void clearOwner(RowGroupIo lease) {
        budget.clearOwner(lease);
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
        return budget.used() > 0L ? 1 : 0;
    }

    @Override
    public String holderSummary() {
        RowGroupIo owner = overshootOwner();
        String ownerLabel = owner == null ? "none" : "lease#" + owner.startSeq();
        return "used=" + budget.used() + "/" + budget.limit() + " owner=" + ownerLabel;
    }

    @Nullable
    RowGroupIo overshootOwner() {
        return budget.overshootOwner();
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
        budget.add(bytes);
    }

    void release(long bytes) {
        budget.release(bytes);
    }

    long used() {
        return budget.used();
    }

    long limit() {
        return budget.limit();
    }

    long admitWaitMs() {
        return budget.admitWaitMs();
    }

    long forcedAdmits() {
        return budget.forcedAdmits();
    }

    long waitNanos() {
        return budget.waitNanos();
    }

    DirectBufferFactory accountingFactory(CircuitBreaker breaker) {
        return accountingFactory(breaker, null);
    }

    /**
     * Factory that charges this watermark with the allocated buffer's
     * {@linkplain HeapFootprint#byteArrayBytes(long) heap footprint} beside the REQUEST breaker
     * (which {@link DirectReadBuffer#allocate(CircuitBreaker, int)} charges the same figure), and
     * releases both on {@link DirectReadBuffer#close()}. {@code admitHold} is a
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
                long footprint = HeapFootprint.byteArrayBytes(len);
                wrapped = account(allocated, footprint);
                if (admitHold != null) {
                    admitHold.drop(footprint);
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

    private DirectReadBuffer account(DirectReadBuffer inner, long footprint) {
        AtomicBoolean released = new AtomicBoolean();
        DirectReadBuffer wrapped = new DirectReadBuffer(inner.buffer(), () -> {
            try {
                inner.close();
            } finally {
                if (released.compareAndSet(false, true)) {
                    release(footprint);
                }
            }
        });
        forceAdd(footprint);
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
        private final NodeByteBudget.Hold inner;
        private final AtomicBoolean counted = new AtomicBoolean(true);

        private AdmitHold(ParquetIoWatermark watermark, NodeByteBudget.Hold inner) {
            this.watermark = watermark;
            this.inner = inner;
            watermark.holds.incrementAndGet();
        }

        /**
         * Drops up to {@code bytes} of leftover estimate, swapping that slice for a retained
         * array charged by {@link #forceAdd}. Sibling in-flight ranges keep their estimate.
         */
        void drop(long bytes) {
            inner.drop(bytes);
        }

        void drop() {
            inner.close();
            if (counted.compareAndSet(true, false)) {
                watermark.holds.decrementAndGet();
            }
        }
    }
}
