/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node-scoped admission limit on retained Parquet I/O bytes (prefetch buffers and sliding
 * windows). Copied from the ClickHouse parquet high-watermark shape: cap at {@code heap / 8},
 * shared by every query on the node. Crossing the limit does not fail the query; the REQUEST
 * circuit breaker remains the hard stop. Look-ahead is refused once {@code used + next} would
 * exceed the cap. One in-flight group may overshoot when it is larger than the remaining budget,
 * so a scan cannot stall; that overshoot is node-wide, not per iterator.
 */
final class ParquetIoWatermark {

    static final int HEAP_DIVISOR = 8;

    private final long limit;
    private final AtomicLong used = new AtomicLong();

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
     * Attempts to reserve {@code bytes} of retained I/O. {@code lookahead} is true when the
     * caller already has a live or queued group (DuckDB-style: no extra job once over budget).
     * Returns {@code false} without throwing; never a query failure.
     */
    boolean tryReserve(long bytes, boolean lookahead) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return true;
        }
        while (true) {
            long current = used.get();
            long next = current + bytes;
            if (next < 0L) {
                return false;
            }
            if (next <= limit) {
                if (used.compareAndSet(current, next)) {
                    return true;
                }
                continue;
            }
            if (lookahead || current > limit) {
                if (used.get() != current) {
                    continue;
                }
                return false;
            }
            if (used.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    /**
     * {@link #tryReserve} plus an {@link AdmitHold} so the footer estimate is swapped for
     * actual buffer sizes as they allocate, and leftover estimate is dropped when the prefetch
     * future settles. Returns {@code null} when admission refuses.
     */
    @Nullable
    AdmitHold tryAdmit(long bytes, boolean lookahead) {
        if (tryReserve(bytes, lookahead) == false) {
            return null;
        }
        return new AdmitHold(this, bytes);
    }

    /**
     * Unconditional charge used for buffers that must exist (sliding window at open, actual
     * coalesced {@code DirectReadBuffer} size). Admission of look-ahead happens in
     * {@link #tryReserve}. When a prefetch already {@link #tryAdmit}ted a footer estimate,
     * {@link #accountingFactory(CircuitBreaker, AdmitHold)} drops that many estimate bytes on
     * each alloc so in-flight sibling GETs keep their hold until they allocate.
     */
    void forceAdd(long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return;
        }
        used.addAndGet(bytes);
    }

    void release(long bytes) {
        if (bytes < 0L) {
            throw new IllegalArgumentException("bytes must be non-negative, got: " + bytes);
        }
        if (bytes == 0L) {
            return;
        }
        used.updateAndGet(current -> {
            long next = current - bytes;
            return next < 0L ? 0L : next;
        });
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
