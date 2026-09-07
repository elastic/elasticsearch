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

import java.util.Arrays;

/**
 * Accumulates the run-table data for a multi-valued {@code SortedSet} ordinal stream.
 *
 * <p>Within a TSDB series the value set is constant, because {@code _tsid} hashes every element of a
 * multi-valued dimension, so a differing set means a different series. This accumulator collapses the
 * per-doc ordinal sets into one entry per run (a maximal span of docs holding the same set), lifting
 * Lucene's doc-granularity {@code SortedSet} layout to run granularity, so storage scales with the
 * number of series rather than the number of docs. Encoding to disk is handled by {@link SortedSetRunTableLayout}.
 *
 * <p>A field may be sparse: a doc with no values is represented directly by the empty set, which forms
 * empty runs like any other set. The reader reports no value for empty runs. No sentinel ordinal is
 * needed, so the empty set never enters the terms dictionary, which holds only the {@code K} real
 * ordinals {@code 0..K-1}. Because TSDB absence is contiguous per series, few extra runs are added.
 *
 * <p>Ords for each run are appended to an in-memory buffer immediately when the run boundary is
 * detected, so only the current run's set is retained for comparison at any point during the walk.
 */
public final class RunTableSortedSetOrdinalWriter {

    private final int valueCount;
    private int[] runStartDocs = new int[16];
    private int[] runOrdCounts = new int[16];
    private int[] ordStreamBuffer = new int[16];
    private int ordStreamSize = 0;
    private int[] lastSet = null;
    private int numRuns = 0;
    private int docCount = 0;
    private int totalOrds = 0;

    /**
     * @param valueCount the field cardinality K; every ord passed to {@link #add} must be in {@code [0, valueCount)}
     */
    public RunTableSortedSetOrdinalWriter(int valueCount) {
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
     * Appends the ordinal set of the next doc using the first {@code count} elements of {@code ords},
     * opening a new run whenever the set differs from the previous doc's set. {@code ords} does not
     * need to be trimmed to length {@code count}; the caller may pass a reuse buffer. The first
     * {@code count} elements must be a strictly ascending, distinct set of ordinals in
     * {@code [0, valueCount)}. The empty set ({@code count == 0}) marks an absent doc.
     */
    public void add(final int[] ords, final int count) {
        addRun(ords, count, 1);
    }

    /**
     * Appends a whole run of {@code docCount} consecutive docs sharing the ordinal set held in the first
     * {@code count} elements of {@code ords}, opening a new run unless the set equals the previous run's set,
     * in which case the two coalesce into one longer run. This is the run-granularity entry used by segment
     * merge, where a series split across source segments arrives as consecutive equal-set runs that must
     * collapse, exactly as the per-doc {@link #add} path would. The first {@code count} elements must be a
     * strictly ascending, distinct set in {@code [0, valueCount)}; the array is copied, so it may be reused.
     */
    public void addRun(final int[] ords, final int count, int docCount) {
        if (docCount <= 0) {
            throw new IllegalArgumentException("docCount must be positive, got " + docCount);
        }
        for (int i = 0; i < count; i++) {
            final int ord = ords[i];
            if (ord < 0 || ord >= valueCount) {
                throw new IllegalArgumentException("ord " + ord + " out of bounds for valueCount " + valueCount);
            }
            if (i > 0 && ord <= ords[i - 1]) {
                throw new IllegalArgumentException(
                    "doc ordinal set must be strictly ascending, got " + Arrays.toString(Arrays.copyOf(ords, count))
                );
            }
        }
        if (numRuns == 0 || lastSet.length != count || Arrays.equals(ords, 0, count, lastSet, 0, count) == false) {
            runStartDocs = ArrayUtil.grow(runStartDocs, numRuns + 1);
            runStartDocs[numRuns] = this.docCount;
            runOrdCounts = ArrayUtil.grow(runOrdCounts, numRuns + 1);
            runOrdCounts[numRuns] = count;
            ordStreamBuffer = ArrayUtil.grow(ordStreamBuffer, ordStreamSize + count);
            System.arraycopy(ords, 0, ordStreamBuffer, ordStreamSize, count);
            ordStreamSize += count;
            lastSet = Arrays.copyOf(ords, count);
            numRuns++;
            totalOrds += count;
        }
        this.docCount += docCount;
    }

    /**
     * Appends the ordinal set of the next doc. {@code ords} must be a strictly ascending, distinct set
     * of ordinals in {@code [0, valueCount)}; the array is copied, so the caller may reuse it. The
     * empty set marks an absent doc and forms empty runs like any other set.
     */
    public void add(final int[] ords) {
        add(ords, ords.length);
    }

    int valueCount() {
        return valueCount;
    }

    int[] runStartDocs() {
        return runStartDocs;
    }

    int[] runOrdCounts() {
        return runOrdCounts;
    }

    int[] ordStreamBuffer() {
        return ordStreamBuffer;
    }

    int ordStreamSize() {
        return ordStreamSize;
    }

    int totalOrds() {
        return totalOrds;
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
