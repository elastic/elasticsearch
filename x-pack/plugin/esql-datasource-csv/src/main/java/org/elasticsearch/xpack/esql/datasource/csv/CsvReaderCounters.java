/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe counter struct for {@link CsvFormatReader}. One instance per reader,
 * shared across the parallel {@code CsvBatchIterator} segments. Iterators bump these counters;
 * {@link #snapshot()} produces the immutable {@link Map} folded into the operator-status envelope.
 * <p>
 * {@link LongAdder} keeps concurrent updates from segment threads non-blocking.
 * {@code headerDetected} uses {@code volatile boolean} with monotonic false→true transition
 * so any segment that sees a header row marks the reader-wide flag.
 */
public final class CsvReaderCounters {

    private final LongAdder linesRead = new LongAdder();
    private final LongAdder parseErrors = new LongAdder();
    private volatile boolean headerDetected = false;
    private final LongAdder totalReadNanos = new LongAdder();

    public void addLinesRead(long delta) {
        if (delta > 0) {
            linesRead.add(delta);
        }
    }

    public void addParseErrors(long delta) {
        if (delta > 0) {
            parseErrors.add(delta);
        }
    }

    public void markHeaderDetected() {
        // Monotonic; safe to set from multiple threads.
        headerDetected = true;
    }

    public void addReadNanos(long nanos) {
        if (nanos > 0) {
            totalReadNanos.add(nanos);
        }
    }

    /**
     * Returns an immutable snapshot of the current counter values, suitable for
     * {@code AsyncExternalSourceOperator.Status.format_reader}. Keys mirror the design doc:
     * {@code lines_read}, {@code parse_errors}, {@code header_detected}, {@code total_read_nanos}.
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("lines_read", linesRead.sum());
        snap.put("parse_errors", parseErrors.sum());
        snap.put("header_detected", headerDetected);
        snap.put("total_read_nanos", totalReadNanos.sum());
        return Map.copyOf(snap);
    }
}
