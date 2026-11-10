/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.util.LongValues;

/**
 * Maps a doc to the run that contains it, given the strictly increasing first doc of every run, and
 * remembers the current run.
 *
 * <p>Seeking is cheap for the access patterns of doc values iteration: a target in the current or the next
 * run costs O(1), so a forward scan across run boundaries stays amortized O(1); a longer forward jump is a
 * binary search, O(log numRuns); a backward target restarts the search from run 0.
 */
final class RunTableCursor {

    private final LongValues startDocs;
    private final int numRuns;
    private final int maxDoc;

    private int currentRun = 0;

    RunTableCursor(final LongValues startDocs, int numRuns, int maxDoc) {
        this.startDocs = startDocs;
        this.numRuns = numRuns;
        this.maxDoc = maxDoc;
    }

    /** The run the cursor is on. */
    int run() {
        return currentRun;
    }

    int numRuns() {
        return numRuns;
    }

    /** The first doc of {@code run}. */
    int startDoc(int run) {
        return (int) startDocs.get(run);
    }

    /** Moves the cursor to {@code run} without a search. */
    void positionOn(int run) {
        currentRun = run;
    }

    /** Moves the cursor to the run that contains {@code target}. */
    void seekDoc(int target) {
        assert target >= 0 && target < maxDoc : "target " + target + " out of range [0, " + maxDoc + ")";
        if (target < startDocs.get(currentRun)) {
            currentRun = 0;
        }
        if (target < nextRunStart(currentRun)) {
            return;
        }
        if (currentRun + 1 < numRuns && target < nextRunStart(currentRun + 1)) {
            currentRun++;
            return;
        }
        // Binary search over [currentRun, numRuns) for the last run starting at or before target.
        int lo = currentRun;
        int hi = numRuns - 1;
        while (lo < hi) {
            final int mid = (lo + hi + 1) >>> 1;
            if (startDocs.get(mid) <= target) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        currentRun = lo;
    }

    private long nextRunStart(int run) {
        return run + 1 < numRuns ? startDocs.get(run + 1) : maxDoc;
    }
}
