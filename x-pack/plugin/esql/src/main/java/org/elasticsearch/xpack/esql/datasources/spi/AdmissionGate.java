/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;

import java.util.concurrent.Executor;

/**
 * Holder side of an admission gate, polled by the stall watchdog when it renders a waiter graph.
 * Wait/grant events go through {@link AdmissionTracker}; this probe answers "who holds" and,
 * per {@link StallPolicy}, whether waiters with holders still count as a stall.
 */
public interface AdmissionGate {

    /**
     * How the stall watchdog decides this gate is wedged. Default {@link #HOLDERS} is healthy
     * saturation for a lifetime-of-stream permit. The byte gate uses {@link #GRANT_AGE} because
     * holders are always {@code ≥ 1} whenever {@code used > 0}, which is the wedge itself.
     */
    enum StallPolicy {
        /** Stall only when waiters exist and {@link AdmissionGate#holders()} is {@code 0}. */
        HOLDERS,
        /**
         * Stall when waiters exist and no grant has landed for the stall window. Holders ignored.
         * Byte releases do not count as progress.
         */
        GRANT_AGE
    }

    /**
     * Outcome of {@link #rescueHead(Executor)}. The watchdog logs {@link #REGRANT} as a lost
     * wakeup and {@link #OVER_CAP} as an over-budget safety net.
     */
    enum RescueResult {
        NONE,
        REGRANT,
        OVER_CAP
    }

    /** Stable token, also used as the telemetry dimension ({@code bytes}, {@code permits/s3}, …). */
    String name();

    /** Units currently held (permits, in-flight GETs, running segmentators). {@code 0} if idle. */
    int holders();

    /** Extra holder context for the WARN line (byte used/limit, overshoot owner). */
    default String holderSummary() {
        return "";
    }

    /** Default keeps the holders check so gzip streams that hold a permit for minutes stay silent. */
    default StallPolicy stallPolicy() {
        return StallPolicy.HOLDERS;
    }

    /**
     * Unsticks the FIFO head. The watchdog has already decided the gate is stalled.
     * Production inspect passes {@code null}: each waiter keeps its executor, so
     * {@code Runnable::run} callbacks run on inspect like any other releaser. Default is a no-op.
     */
    default RescueResult rescueHead(@Nullable Executor delivery) {
        return RescueResult.NONE;
    }

    /**
     * Fail waiters whose cancel supplier is true (or whose lease is finished), then run the
     * grant loop. Completions use each waiter's executor. Direct ({@code Runnable::run})
     * waiters therefore complete on the inspect thread.
     */
    default void failCancelledWaiters() {}
}
