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
 * Collapses the per-doc ordinal sets of a multi-valued field into runs: maximal spans of consecutive docs
 * with the same set. {@link SortedSetRunTableLayout#encode} writes the result.
 *
 * <p>A doc without a value is added as the empty set, which forms runs like any other set, so no sentinel
 * ordinal is needed. Each run's set is stored once, in a flat array of ordinals.
 */
public final class RunTableSortedSetOrdinalWriter {

    private final int valueCount;
    private int[] runStartDocs = new int[16];
    private int[] runSetStarts = new int[16];
    private int[] ords = new int[16];
    private int numOrds = 0;
    private int numRuns = 0;
    private int docCount = 0;

    /**
     * @param valueCount the field's value count {@code K}: {@link #add} accepts sets of ordinals in {@code [0, K)}
     */
    public RunTableSortedSetOrdinalWriter(int valueCount) {
        if (valueCount <= 0) {
            throw new IllegalArgumentException("valueCount must be positive, got " + valueCount);
        }
        this.valueCount = valueCount;
    }

    /** The number of runs collected so far. */
    public int numRuns() {
        return numRuns;
    }

    /**
     * Appends the set of the next doc, the first {@code count} entries of {@code set}, opening a new run when it
     * differs from the previous doc's set. The entries must be strictly ascending ordinals in {@code [0, K)};
     * {@code count == 0} marks a doc without a value. {@code set} is copied, so the caller may reuse it.
     */
    public void add(final int[] set, int count) {
        assert isValidSet(set, count) : "invalid ordinal set " + Arrays.toString(Arrays.copyOf(set, count));
        if (numRuns == 0 || sameAsLastRun(set, count) == false) {
            runStartDocs = ArrayUtil.grow(runStartDocs, numRuns + 1);
            runSetStarts = ArrayUtil.grow(runSetStarts, numRuns + 1);
            ords = ArrayUtil.grow(ords, numOrds + count);
            runStartDocs[numRuns] = docCount;
            runSetStarts[numRuns] = numOrds;
            System.arraycopy(set, 0, ords, numOrds, count);
            numOrds += count;
            numRuns++;
        }
        docCount++;
    }

    private boolean sameAsLastRun(final int[] set, int count) {
        final int lastStart = runSetStarts[numRuns - 1];
        return numOrds - lastStart == count && Arrays.equals(ords, lastStart, numOrds, set, 0, count);
    }

    private boolean isValidSet(final int[] set, int count) {
        for (int i = 0; i < count; i++) {
            if (set[i] < 0 || set[i] >= valueCount || (i > 0 && set[i] <= set[i - 1])) {
                return false;
            }
        }
        return true;
    }

    int valueCount() {
        return valueCount;
    }

    int[] runStartDocs() {
        return runStartDocs;
    }

    /** The offset in {@link #ords()} where each run's set starts; the last run's set ends at {@link #numOrds()}. */
    int[] runSetStarts() {
        return runSetStarts;
    }

    int[] ords() {
        return ords;
    }

    int numOrds() {
        return numOrds;
    }
}
