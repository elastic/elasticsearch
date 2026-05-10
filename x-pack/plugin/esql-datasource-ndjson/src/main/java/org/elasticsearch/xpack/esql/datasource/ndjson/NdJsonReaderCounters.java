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

    private final LongAdder documentsParsed = new LongAdder();
    private final LongAdder parseErrors = new LongAdder();
    private final LongAdder totalReadNanos = new LongAdder();

    public void addDocumentsParsed(long delta) {
        if (delta > 0) {
            documentsParsed.add(delta);
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
     * {@code AsyncExternalSourceOperator.Status.format_reader}. Keys mirror the design doc:
     * {@code documents_parsed}, {@code parse_errors}, {@code total_read_nanos}.
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("documents_parsed", documentsParsed.sum());
        snap.put("parse_errors", parseErrors.sum());
        snap.put("total_read_nanos", totalReadNanos.sum());
        return Map.copyOf(snap);
    }
}
