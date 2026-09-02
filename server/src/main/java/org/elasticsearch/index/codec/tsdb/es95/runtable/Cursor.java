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
 * Common interface for doc-to-run positioners used by the run-table ordinal readers.
 *
 * <p>A cursor maps a target document to a run -- a contiguous range of documents that
 * share the same ordinal value. Callers position the cursor via {@link #seekDoc} or
 * {@link #positionOn}, then read the current run index via {@link #run} and the run's
 * first document via {@link #startDoc}.
 *
 * <p>Two implementations exist: {@link RunTableCursor}, which looks up runs stored in
 * a packed {@code startDoc[]} array; and {@link OrdinalCursor}, which treats each
 * document as its own single-document run, providing a transparent fallback when run-
 * table encoding was not viable for a field.
 */
interface Cursor {

    /** The index of the run currently under the cursor. */
    int run();

    /** The total number of runs in this cursor. */
    int numRuns();

    /** The first doc covered by {@code run}. */
    int startDoc(int run);

    /**
     * Positions the cursor directly on {@code run} without a doc search. Used by readers to
     * skip a sentinel run to the start of the next value-bearing run.
     */
    void positionOn(int run);

    /** Rewinds the cursor to the first run. */
    void reset();

    /**
     * Positions the cursor on the run containing {@code target}. Every valid document is
     * covered by exactly one run.
     */
    void seekDoc(int target);
}
