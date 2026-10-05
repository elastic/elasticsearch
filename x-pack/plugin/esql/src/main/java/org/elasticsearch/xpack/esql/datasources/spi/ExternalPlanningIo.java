/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Releasable;

import java.util.concurrent.atomic.LongAdder;

/**
 * Query-scoped tally of coordinator planning I/O against external storage: received bytes and
 * request count from schema resolution, split-discovery probes, and metadata GETs that never
 * hit {@code newStream}. Not attached to node {@link ExternalSourceMetrics} (NOOP sink).
 * Distinct from estimated {@code bytes_scanned}.
 * <p>
 * Shared across fan-out threads via {@link #activate(ExternalPlanningIo)}; {@link LongAdder}
 * fields are the only mutable state.
 */
public final class ExternalPlanningIo {

    private static final ThreadLocal<ExternalPlanningIo> CURRENT = new ThreadLocal<>();

    private final LongAdder bytesRead = new LongAdder();
    private final LongAdder requestCount = new LongAdder();

    public static ExternalPlanningIo current() {
        return CURRENT.get();
    }

    /**
     * Installs {@code io} as the current planning tally. Restores the previous holder on close
     * (or clears when this call installed the first one).
     */
    public static Releasable activate(ExternalPlanningIo io) {
        if (io == null) {
            return () -> {};
        }
        ExternalPlanningIo prev = CURRENT.get();
        CURRENT.set(io);
        return () -> {
            if (prev == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(prev);
            }
        };
    }

    /** Folds a finished planning object's snapshot into the current holder, if any. */
    public static void fold(StorageObject object) {
        ExternalPlanningIo io = CURRENT.get();
        if (io == null || object == null) {
            return;
        }
        io.add(object.metrics());
    }

    /**
     * Counts a metadata GET that never went through {@code newStream} (S3 suffix-range / HEAD).
     * Planning holder only — does not touch execution APM {@code storage.requests.total}.
     */
    public static void addMetadataGet(long bytes) {
        ExternalPlanningIo io = CURRENT.get();
        if (io == null) {
            return;
        }
        io.requestCount.increment();
        if (bytes > 0) {
            io.bytesRead.add(bytes);
        }
    }

    public void add(StorageObjectMetrics metrics) {
        if (metrics == null || metrics.isZero()) {
            return;
        }
        bytesRead.add(metrics.bytesRead());
        requestCount.add(metrics.requestCount());
    }

    /** Received stream bytes while this holder is current. */
    public static void addStreamBytes(long bytes) {
        ExternalPlanningIo io = CURRENT.get();
        if (io != null) {
            io.recordStreamBytes(bytes);
        }
    }

    /** One completed storage request (including {@code addRequest(nanos, 0)} sync opens). */
    public static void addRequest(long bytes) {
        ExternalPlanningIo io = CURRENT.get();
        if (io != null) {
            io.recordRequest(bytes);
        }
    }

    void recordStreamBytes(long bytes) {
        if (bytes > 0) {
            bytesRead.add(bytes);
        }
    }

    void recordRequest(long bytes) {
        requestCount.increment();
        if (bytes > 0) {
            bytesRead.add(bytes);
        }
    }

    /**
     * Takes the current totals and zeros the adders so a later snapshot (schema vs split-discovery)
     * does not double-count.
     */
    public long[] snapshotAndReset() {
        return new long[] { bytesRead.sumThenReset(), requestCount.sumThenReset() };
    }

    public long bytesRead() {
        return bytesRead.sum();
    }

    public long requestCount() {
        return requestCount.sum();
    }
}
