/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * fail the query. {@link #admitWait} timeout or cancellation fails that GET with
 * {@link EsRejectedExecutionException}. The REQUEST circuit breaker remains the hard stop for
 * allocation.
 */
final class ParquetIoWatermark {

    static final int HEAP_DIVISOR = 8;

    /**
     * How a coalesced GET batch charges this watermark. {@link #UNGATED} is a null hold's
     * {@link #forceAdd} (footer metadata, sliding window). {@link #GROUP_HOLD} is a footer
     * estimate already {@link #tryAdmit}ted. {@link #PER_GET} waits per miss via
     * {@link #admitWait}. A null hold is never {@link #PER_GET}.
     */
    enum ByteGate {
        UNGATED,
        GROUP_HOLD,
        PER_GET
    }

    private final long limit;
    private final AtomicLong used = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private RowGroupIo overshootOwner;

    static ParquetIoWatermark forHeap() {
        long heapBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
        return new ParquetIoWatermark(Math.max(1L, heapBytes / HEAP_DIVISOR));
    }

    ParquetIoWatermark(long limit) {
        if (limit < 1L) {
            throw new IllegalArgumentException("limit must be at least 1, got: " + limit);
        }
        this.limit = limit;
    }

    /**
     * Attempts to reserve {@code bytes} of retained I/O. Refuses once {@code used + bytes} would
     * exceed the cap; the one overshoot is {@link #admitWait}. Returns {@code false} without
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
     * Blocks until {@code bytes} can be charged for {@code lease}, or {@code timeoutMs} elapses.
     * Byte wait and permit wait are separate full clocks; this deadline covers only this wait.
     * The caller supplies {@code timeoutMs} from {@code StorageObject#admissionWaitTimeoutMs()}.
     * <p>
     * Lock order: this watermark lock, then the budget lock inside
     * {@link RowGroupIo#tryPinOvershoot()}. Never the reverse. {@link #clearOwner} runs after
     * the budget {@code finish()} has released its lock.
     */
    AdmitHold admitWait(long bytes, RowGroupIo lease, long timeoutMs) {
        if (lease == null) {
            throw new IllegalArgumentException("lease is required");
        }
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return new AdmitHold(this, 0L);
        }
        // Independent of the query-budget acquire clock and the node-limiter clock.
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
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
                    return new AdmitHold(this, bytes);
                }
                if (overshootOwner == lease) {
                    used.set(next);
                    return new AdmitHold(this, bytes);
                }
                if (overshootOwner == null) {
                    if (tryBecomeOwner(lease, next)) {
                        return new AdmitHold(this, bytes);
                    }
                }
                lease.setWake(this::signalWaiters);
                if (lease.isCancelled()) {
                    throw cancelled();
                }
                long waitNanos = deadlineNanos - System.nanoTime();
                if (waitNanos <= 0L) {
                    throw rejected(timeoutMs);
                }
                try {
                    notFull.awaitNanos(waitNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new EsRejectedExecutionException("Interrupted while waiting for parquet I/O bytes: " + e);
                }
            }
        } finally {
            lock.unlock();
        }
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

    private static EsRejectedExecutionException rejected(long timeoutMs) {
        return new EsRejectedExecutionException("Timed out waiting for parquet I/O bytes after [" + timeoutMs + "]ms");
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

        private AdmitHold(ParquetIoWatermark watermark, long bytes) {
            this.watermark = watermark;
            this.remaining = new AtomicLong(Math.max(0L, bytes));
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
                    return;
                }
            }
        }

        void drop() {
            drop(Long.MAX_VALUE);
        }
    }
}
