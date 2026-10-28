/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.benchmark._nightly.esql;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * JMH auxiliary counters that surface {@code rowsRead} and {@code bytesRead} as
 * separate score columns alongside the primary {@code ops/s} number in every
 * format-reader benchmark in this package. JMH divides the accumulated counter
 * values by elapsed iteration time so the two extra columns are rates — read as
 * <em>rows/sec</em> and <em>bytes/sec</em> respectively, despite JMH labelling the
 * unit as {@code ops/s}.
 * <p>
 * <b>Bytes semantics:</b> {@code bytesRead} accumulates the byte length of the
 * fixture handed to the reader, not the number of bytes the reader actually
 * dereferenced. For variants where the reader can terminate early (e.g. Parquet
 * {@code LIMIT} pushdown), this overestimates bytes consumed — the bytes/sec
 * column for those cells reflects "bytes of fixture available per second", which
 * is still the useful throughput baseline; the early-termination signal lives in
 * the rising primary {@code ops/s} and {@code rowsRead} columns.
 */
@AuxCounters(AuxCounters.Type.OPERATIONS)
@State(Scope.Thread)
public class ReadMetrics {

    /** Total rows the reader emitted during the current iteration. */
    public long rowsRead;

    /** Total bytes of source fixture handed to the reader during the current iteration. */
    public long bytesRead;

    @Setup(Level.Iteration)
    public void reset() {
        rowsRead = 0;
        bytesRead = 0;
    }

    /** Records one drained read of the reader iterator. Called once per {@code @Benchmark} invocation. */
    public void record(long rows, long bytes) {
        rowsRead += rows;
        bytesRead += bytes;
    }
}
