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
 * Value-agnostic doc-to-run positioner for fields encoded in run-table format. Given the run
 * boundary column {@code startDoc[]} (strictly increasing, {@code numRuns} entries) it maps a target
 * doc to the run that owns it, holding the current run index as cursor state.
 *
 * <p>The positioning cost mirrors a TSDB forward scan: an already-covered target is O(1), a target in
 * the immediately following run is O(1) (so a {@code nextDoc} scan crossing a boundary stays amortized
 * O(1)), a large forward jump is O(log numRuns) via binary search, and a backward target resets the
 * cursor to run 0 before searching forward again. This class carries no value semantics; the reader
 * built on top reads a single ordinal or a per-run ordinal set from {@link #run()}.
 *
 * <p>For fields where run-table encoding was not viable, use {@link OrdinalCursor} instead. Both
 * implement {@link Cursor} so readers can accept either transparently.
 */
final class RunTableCursor implements Cursor {

    private final LongValues startDocs;
    private final int numRuns;
    private final int maxDoc;

    private int currentRun = 0;

    RunTableCursor(final LongValues startDocs, int numRuns, int maxDoc) {
        this.startDocs = startDocs;
        this.numRuns = numRuns;
        this.maxDoc = maxDoc;
    }

    @Override
    public int run() {
        return currentRun;
    }

    @Override
    public int numRuns() {
        return numRuns;
    }

    @Override
    public int startDoc(int run) {
        return (int) startDocs.get(run);
    }

    @Override
    public void positionOn(int run) {
        currentRun = run;
    }

    @Override
    public void reset() {
        currentRun = 0;
    }

    @Override
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
