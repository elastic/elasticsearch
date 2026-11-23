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
 * Collapses the per-doc ordinal stream of a single-valued field into runs: maximal spans of consecutive
 * docs with the same ordinal. {@link SortedRunTableLayout#encode} writes the result.
 *
 * <p>Docs without a value are added as the sentinel ordinal {@code valueCount}, one past the largest real
 * ordinal, and form runs like any other ordinal.
 */
public final class RunTableSortedOrdinalWriter {

    private final int valueCount;
    private int[] runStartDocs = new int[16];
    private int[] runOrds = new int[16];
    private int numRuns = 0;
    private int docCount = 0;
    private int maxOrdWritten = 0;

    /**
     * @param valueCount the field's value count {@code K}: {@link #add} accepts the real ordinals
     *                   {@code [0, K)} and the sentinel {@code K}
     */
    public RunTableSortedOrdinalWriter(int valueCount) {
        if (valueCount <= 0) {
            throw new IllegalArgumentException("valueCount must be positive, got " + valueCount);
        }
        this.valueCount = valueCount;
    }

    /** The number of runs collected so far. */
    public int numRuns() {
        return numRuns;
    }

    /** Appends the ordinal of the next doc, opening a new run when it differs from the previous doc's. */
    public void add(int ord) {
        assert ord >= 0 && ord <= valueCount : "ord " + ord + " out of bounds for valueCount " + valueCount;
        if (numRuns == 0 || ord != runOrds[numRuns - 1]) {
            runStartDocs = ArrayUtil.grow(runStartDocs, numRuns + 1);
            runOrds = ArrayUtil.grow(runOrds, numRuns + 1);
            runStartDocs[numRuns] = docCount;
            runOrds[numRuns] = ord;
            numRuns++;
        }
        maxOrdWritten = Math.max(maxOrdWritten, ord);
        docCount++;
    }

    int valueCount() {
        return valueCount;
    }

    int[] runStartDocs() {
        return runStartDocs;
    }

    int[] runOrds() {
        return runOrds;
    }

    int maxOrdWritten() {
        return maxOrdWritten;
    }
}
