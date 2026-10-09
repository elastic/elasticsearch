/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

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
        /** Stall when waiters exist and no grant has landed for the stall window. Holders ignored. */
        GRANT_AGE
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
     * Grants the FIFO head over the cap as a counted scheduling-bug signal. Default is a no-op.
     * The byte gate returns {@code true} when it issued an over-cap hold.
     */
    default boolean rescueIfStalled() {
        return false;
    }
}
