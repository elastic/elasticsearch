/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

/**
 * Source of per-factory format-reader read stats. Implemented by operator factories that wrap a
 * shared format reader. The factory snapshots its reader's counters once when the last driver
 * finishes (at {@code releaseOperator} time), after which {@link #readNanos()} and
 * {@link #readCpuNanos()} return stable values.
 */
public interface SourceReadStats {
    /** A short identifier for the source (dataset name or path). */
    String sourceIdentifier();

    /** Total wall time spent reading, in nanoseconds. */
    long readNanos();

    /** Total CPU time spent reading, in nanoseconds. */
    long readCpuNanos();
}
