/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIoAffinity;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks decode charges against a footer estimate already sitting in an {@link ParquetIoWatermark}
 * hold. {@link #consume} {@code forceAdd}s any shortfall and never waits. {@link #close()}
 * releases only the extra; the hold still drops the estimate. {@link #consume} after
 * {@link #close()} is a no-op so leftover estimate cannot swallow dest and extras cannot leak.
 */
final class ParquetDecodeBudget implements Releasable {

    static final ParquetDecodeBudget NOOP = new ParquetDecodeBudget(null, 0L);

    private final ParquetIoWatermark watermark;
    private final AtomicLong remainingEstimate;
    private final AtomicLong extra = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    /** Lease captured at {@link #consume} so {@link #close} can refund without {@code StorageIoAffinity}. */
    private final AtomicReference<RowGroupIo> extraLease = new AtomicReference<>();

    private ParquetDecodeBudget(@Nullable ParquetIoWatermark watermark, long estimate) {
        this.watermark = watermark;
        this.remainingEstimate = new AtomicLong(Math.max(0L, estimate));
    }

    static ParquetDecodeBudget tracking(@Nullable ParquetIoWatermark watermark, long estimate) {
        if (watermark == null) {
            return NOOP;
        }
        return new ParquetDecodeBudget(watermark, estimate);
    }

    void consume(long bytes) {
        if (watermark == null || closed.get() || bytes <= 0L) {
            return;
        }
        long uncovered = bytes;
        while (uncovered > 0L) {
            long remaining = remainingEstimate.get();
            if (remaining <= 0L) {
                watermark.forceAdd(uncovered, extraLease());
                extra.addAndGet(uncovered);
                return;
            }
            long take = Math.min(remaining, uncovered);
            if (remainingEstimate.compareAndSet(remaining, remaining - take)) {
                uncovered -= take;
            }
        }
    }

    long extra() {
        return extra.get();
    }

    @Override
    public void close() {
        if (watermark == null || closed.compareAndSet(false, true) == false) {
            return;
        }
        long toRelease = extra.getAndSet(0L);
        if (toRelease > 0L) {
            watermark.release(toRelease, extraLease.get());
        }
    }

    private RowGroupIo extraLease() {
        RowGroupIo current = extraLease.get();
        if (current != null) {
            return current;
        }
        StorageIoAffinity.Scope scope = StorageIoAffinity.current();
        RowGroupIo captured = scope == null ? null : scope.lease();
        if (captured != null) {
            extraLease.compareAndSet(null, captured);
            return extraLease.get();
        }
        return null;
    }
}
