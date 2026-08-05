/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

/**
 * Tiny shared helper for the {@code *FixtureGenerator} build-time tools. Each generator implements
 * a split mode that writes {@code <basename>_00.<ext>}, {@code _01.<ext>}, … by partitioning the
 * parsed CSV rows into {@code numParts} contiguous, ceiling-sized slices. Centralising that math
 * keeps the four call sites (Parquet, ORC, NDJSON, TSV) in sync.
 */
public final class SplitPartitioner {

    private SplitPartitioner() {}

    /** Half-open row range {@code [from, to)} for one slice. */
    public record Range(int from, int to) {}

    /**
     * Returns the {@code [from, to)} row range for the given {@code part} (0-based) when splitting
     * {@code total} rows into {@code numParts} contiguous, ceiling-sized slices, or {@code null} if
     * the slice would be empty (i.e. {@code total} does not reach this part — callers should
     * {@code break} on null).
     */
    public static Range partitionRange(int total, int numParts, int part) {
        if (numParts < 1) {
            throw new IllegalArgumentException("numParts must be >= 1, got " + numParts);
        }
        if (part < 0) {
            throw new IllegalArgumentException("part must be >= 0, got " + part);
        }
        int partSize = (total + numParts - 1) / numParts;
        int from = part * partSize;
        if (from >= total) {
            return null;
        }
        int to = Math.min(from + partSize, total);
        return new Range(from, to);
    }
}
