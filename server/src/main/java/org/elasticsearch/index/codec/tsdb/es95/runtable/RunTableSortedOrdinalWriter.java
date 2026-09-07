/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.util.ArrayUtil;

/**
 * Accumulates the run-table data for a single-valued {@code Sorted} ordinal stream.
 *
 * <p>TSDB segments are index-sorted by {@code (_tsid, @timestamp)}, so a dimension field's per-doc
 * ordinal stream is piecewise-constant. This accumulator collapses the stream into one entry per run (a
 * maximal span of docs holding the same ordinal), so storage scales with the number of series rather
 * than the number of docs. Encoding to disk is handled by {@link SortedRunTableLayout}.
 *
 * <p>A field may be sparse: docs that have no value are represented by the reserved sentinel ordinal
 * {@code K} (equal to {@code valueCount}, one past the last real ordinal {@code K-1}), which forms
 * sentinel runs like any other ordinal. The reader reports no value for sentinel runs. Because TSDB
 * absence is contiguous per series, few extra runs are added and the sentinel never enters the terms
 * dictionary, which holds only the {@code K} real ordinals {@code 0..K-1}.
 */
public final class RunTableSortedOrdinalWriter {

    private final int valueCount;
    private int[] runStartDocs = new int[16];
    private int[] runOrds = new int[16];
    private int numRuns = 0;
    private int docCount = 0;
    private int maxOrdWritten = 0;

    /**
     * @param valueCount the field cardinality K; real ordinals passed to {@link #add} must be in {@code [0, valueCount)}
     *                   and the reserved absent sentinel {@code valueCount} is also accepted
     */
    public RunTableSortedOrdinalWriter(int valueCount) {
        if (valueCount <= 0) {
            throw new IllegalArgumentException("valueCount must be positive, got " + valueCount);
        }
        this.valueCount = valueCount;
    }

    /**
     * Returns the number of runs collected so far. Callers consult this before encoding to choose
     * between the run table and the baseline ordinal encoding without emitting any bytes.
     */
    public int numRuns() {
        return numRuns;
    }

    /**
     * Returns {@code true} when the run table is already too large to be worth writing: the average
     * run spans fewer than two docs. Codec writers call this during the doc walk to abort early.
     */
    public boolean exceedsThreshold(int maxDoc) {
        return (long) numRuns * 2 > maxDoc;
    }

    /**
     * Appends the ordinal of the next doc, opening a new run whenever it differs from the previous doc's ordinal.
     * The reserved sentinel ordinal {@code valueCount} marks an absent doc and forms sentinel runs like any other
     * ordinal.
     */
    public void add(int ord) {
        addRun(ord, 1);
    }

    /**
     * Appends a whole run of {@code docCount} consecutive docs sharing {@code ord}, opening a new run unless
     * {@code ord} equals the previous run's ordinal, in which case the two coalesce into one longer run. This is
     * the run-granularity entry used by segment merge, where a series split across source segments arrives as
     * consecutive equal-ordinal runs that must collapse, exactly as the per-doc {@link #add} path would.
     */
    public void addRun(int ord, int docCount) {
        if (ord < 0 || ord > valueCount) {
            throw new IllegalArgumentException("ord " + ord + " out of bounds for valueCount " + valueCount);
        }
        if (docCount <= 0) {
            throw new IllegalArgumentException("docCount must be positive, got " + docCount);
        }
        if (numRuns == 0 || ord != runOrds[numRuns - 1]) {
            runStartDocs = ArrayUtil.grow(runStartDocs, numRuns + 1);
            runOrds = ArrayUtil.grow(runOrds, numRuns + 1);
            runStartDocs[numRuns] = this.docCount;
            runOrds[numRuns] = ord;
            numRuns++;
        }
        maxOrdWritten = Math.max(maxOrdWritten, ord);
        this.docCount += docCount;
    }

    int valueCount() {
        return valueCount;
    }

    int[] startDocs() {
        return runStartDocs;
    }

    int[] runOrds() {
        return runOrds;
    }

    int maxOrdWritten() {
        return maxOrdWritten;
    }

    int docCount() {
        return docCount;
    }

    /**
     * The run count and the total bytes written across the data and meta streams. Returned by the
     * layout class for future size-based layout selection; not consumed by the codec writers.
     */
    public record Stats(int numRuns, long totalBytes) {}
}
