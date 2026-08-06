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
 * Value-agnostic doc-to-run positioner shared by the run-table doc values readers. Given the run
 * boundary column {@code startDoc[]} (strictly increasing, {@code numRuns} entries) it maps a target
 * doc to the run that owns it, holding the current run index as cursor state.
 *
 * <p>The positioning cost mirrors a TSDB forward scan: an already-covered target is O(1), a target in
 * the immediately following run is O(1) (so a {@code nextDoc} scan crossing a boundary stays amortized
 * O(1)), a large forward jump is O(log numRuns) via binary search, and a backward target resets the
 * cursor to run 0 before searching forward again. This class carries no value semantics; the reader
 * built on top reads a single ordinal or a per-run ordinal set from {@link #run()}.
 */
public final class RunCursor {

    private final LongValues startDocs;
    private final int numRuns;
    private final int maxDoc;

    private int currentRun = 0;

    public RunCursor(final LongValues startDocs, int numRuns, int maxDoc) {
        this.startDocs = startDocs;
        this.numRuns = numRuns;
        this.maxDoc = maxDoc;
    }

    /**
     * The index of the run currently under the cursor, valid after a {@link #seekDoc} call.
     */
    public int run() {
        return currentRun;
    }

    /** The number of runs in the table. */
    public int numRuns() {
        return numRuns;
    }

    /** The first doc covered by {@code run}. */
    public int startDoc(int run) {
        return (int) startDocs.get(run);
    }

    /**
     * Positions the cursor directly on {@code run}. Used by the sparse reader to skip a sentinel run to the
     * start of the next value-bearing run without re-searching {@code startDoc[]}.
     */
    public void positionOn(int run) {
        currentRun = run;
    }

    /** Rewinds the cursor to the first run. */
    public void reset() {
        currentRun = 0;
    }

    /**
     * Positions the cursor on the run containing {@code target}. Runs tile {@code [0, maxDoc)} with no gaps,
     * so every valid target is covered by exactly one run.
     */
    public void seekDoc(int target) {
        assert target >= 0 && target < maxDoc : "target " + target + " out of range [0, " + maxDoc + ")";
        if (target < startDocs.get(currentRun)) {
            currentRun = 0;
        }
        // Fast path: the cursor already covers the target. This is the one-doc-at-a-time common case.
        if (target < nextRunStart(currentRun)) {
            return;
        }
        // Sequential step: the target falls in the immediately following run (kept O(1) so a nextDoc
        // scan that crosses a run boundary stays amortized O(1)).
        if (currentRun + 1 < numRuns && target < nextRunStart(currentRun + 1)) {
            currentRun++;
            return;
        }
        // Large forward jump: binary-search startDocs over [currentRun, numRuns) for the largest run
        // whose start doc is <= target, so a random seek is O(log runs) rather than O(runs).
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
