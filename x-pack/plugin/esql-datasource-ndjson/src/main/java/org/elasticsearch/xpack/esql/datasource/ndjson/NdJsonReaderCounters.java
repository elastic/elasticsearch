/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe counter struct for {@link NdJsonFormatReader}. One instance per reader,
 * shared across the parallel {@link NdJsonPageDecoder} segments spawned by the iterator. Decoders
 * bump these counters alongside their internal book-keeping; {@link #snapshot()} produces the
 * immutable {@link Map} folded into the operator-status envelope.
 * <p>
 * {@link LongAdder} keeps concurrent updates from segment threads non-blocking.
 */
public final class NdJsonReaderCounters {

    private final LongAdder rowsEmitted = new LongAdder();
    private final LongAdder parseErrors = new LongAdder();
    private final LongAdder totalReadNanos = new LongAdder();

    public void addRowsEmitted(long delta) {
        if (delta > 0) {
            rowsEmitted.add(delta);
        }
    }

    public void addParseErrors(long delta) {
        if (delta > 0) {
            parseErrors.add(delta);
        }
    }

    public void addReadNanos(long nanos) {
        if (nanos > 0) {
            totalReadNanos.add(nanos);
        }
    }

    /**
     * Returns an immutable snapshot of the current counter values, suitable for
     * {@code AsyncExternalSourceOperator.Status.format_reader}. Keys: {@code format} (discriminator),
     * {@code rows_emitted} (rows the format reader produced — same key name across all four format
     * readers for cross-format consumer aggregation), {@code parse_errors} (NDJSON-specific count
     * of malformed lines skipped under lenient policies), {@code read_nanos}.
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("format", "ndjson");
        snap.put("rows_emitted", rowsEmitted.sum());
        snap.put("parse_errors", parseErrors.sum());
        snap.put("read_nanos", totalReadNanos.sum());
        return Map.copyOf(snap);
    }
}
