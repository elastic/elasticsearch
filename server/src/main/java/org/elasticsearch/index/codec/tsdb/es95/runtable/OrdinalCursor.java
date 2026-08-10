/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

/**
 * Doc-to-run positioner for fields that fell back to per-document ordinal encoding. Each document
 * is treated as its own single-document run: {@link #numRuns()} equals {@code maxDoc},
 * {@link #startDoc(int)} equals the run index, and {@link #seekDoc(int)} sets the current run to
 * the target document directly.
 *
 * <p>This cursor is a transparent fallback: it satisfies the same {@link Cursor} contract as
 * {@link RunTableCursor} so readers that accept a {@link Cursor} require no branching to handle
 * either encoding.
 */
final class OrdinalCursor implements Cursor {

    private final int maxDoc;
    private int currentRun = 0;

    OrdinalCursor(int maxDoc) {
        this.maxDoc = maxDoc;
    }

    @Override
    public int run() {
        return currentRun;
    }

    @Override
    public int numRuns() {
        return maxDoc;
    }

    @Override
    public int startDoc(int run) {
        return run;
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
        currentRun = target;
    }
}
