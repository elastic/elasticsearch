/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Holder side of an admission gate, polled by the stall watchdog when it renders a waiter graph.
 * Wait/grant events go through {@link AdmissionTracker}; this probe only answers "who holds".
 */
public interface AdmissionGate {

    /** Stable token, also used as the telemetry dimension ({@code bytes}, {@code permits/s3}, …). */
    String name();

    /** Units currently held (permits, in-flight GETs, running segmentators). {@code 0} if idle. */
    int holders();

    /** Extra holder context for the WARN line (byte used/limit, overshoot owner). */
    default String holderSummary() {
        return "";
    }
}
