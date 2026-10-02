/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import java.util.Arrays;

/**
 * Records, in page order, the file-global byte offsets of the rows a page builder ACCEPTS into the emitted page.
 *
 * <p>The CSV reader parses all rows in a batch (each with a parallel {@code rowStartBytes} offset) and then filters
 * them while building the page: a structural or field error drops a row, so the emitted page has fewer positions
 * than {@code rowStartBytes} has entries. Per-stripe attribution ({@code CsvBatchIterator.accumulateStripes}) must
 * walk offsets aligned 1:1 with the PAGE, not with the parsed rows -- otherwise a drop mis-attributes rows to the
 * wrong stripe or silently disables capture (the A1/F1 bug class). Every page builder therefore needs the identical
 * "collect the surviving rows' offsets, page-aligned" mechanic; this is that one mechanic, so no builder re-derives
 * it (and gets the null-handling or trimming subtly wrong).
 *
 * <p>Usage: construct once per built page from the parsed-row offsets; call {@link #accept(int)} with the parsed-row
 * index of each row that survives into the page, in order; then {@link #finish()} for the page-aligned array. When
 * offsets are not tracked ({@code rowStartBytes == null}, e.g. no {@code _rowPosition} and no byte tracker) this is a
 * no-op and {@link #finish()} returns {@code null} -- a legitimate "no offsets" signal that safe-misses stripe capture.
 */
final class SurvivorOffsets {

    /** Parsed-row-indexed source offsets, or {@code null} when offsets are not tracked. */
    private final long[] source;
    /** Survivor offsets in page order, or {@code null} when not tracking. */
    private final long[] accepted;
    private int count;

    private SurvivorOffsets(long[] source, int parsedRowCount) {
        this.source = source;
        this.accepted = source != null ? new long[parsedRowCount] : null;
    }

    /**
     * @param rowStartBytes  the parsed rows' byte offsets, or {@code null} when offsets are not tracked
     * @param parsedRowCount the number of parsed rows in this batch (the accept upper bound)
     */
    static SurvivorOffsets of(long[] rowStartBytes, int parsedRowCount) {
        return new SurvivorOffsets(rowStartBytes, parsedRowCount);
    }

    /** Record that the parsed row at {@code parsedRowIdx} survived into the page. Call in page order. */
    void accept(int parsedRowIdx) {
        if (accepted != null) {
            accepted[count] = source[parsedRowIdx];
        }
        count++;
    }

    /** The page-aligned survivor offsets (length == the number of accepted rows), or {@code null} when not tracking. */
    long[] finish() {
        return accepted == null ? null : Arrays.copyOf(accepted, count);
    }
}
