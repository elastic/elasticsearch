/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.LongValues;

/**
 * The per-doc ordinal sets of a run table: {@link #docValueCount()} is the size of the set of the run under
 * the cursor and {@link #nextValue()} walks it in ascending order. The producer wraps it into a
 * {@code SortedSetDocValues} together with the terms dictionary.
 *
 * <p>Docs in a run with the empty set have no value: {@link #advanceExact} returns {@code false} for them
 * and {@link #nextDoc}/{@link #advance} skip to the first doc of the next run. Runs are maximal, so an empty
 * run is always followed by a run with values, or by the end of the segment.
 */
final class RunTableSortedSetDocValues extends SortedNumericDocValues {

    private final RunTableCursor cursor;
    private final LongValues setOffsets;
    private final LongValues ordStream;
    private final int maxDoc;
    private final int numDocsWithField;

    private int doc = -1;
    private int loadedRun = -1;
    private long setStart;
    private long setEnd;
    private long nextOrd;

    RunTableSortedSetDocValues(
        final RunTableCursor cursor,
        final LongValues setOffsets,
        final LongValues ordStream,
        int maxDoc,
        int numDocsWithField
    ) {
        this.cursor = cursor;
        this.setOffsets = setOffsets;
        this.ordStream = ordStream;
        this.maxDoc = maxDoc;
        this.numDocsWithField = numDocsWithField;
    }

    /** Points the value iterator at the start of {@code run}'s set; consecutive docs of a run reuse the bounds. */
    private void loadSet(int run) {
        if (run != loadedRun) {
            setStart = setOffsets.get(run);
            setEnd = setOffsets.get(run + 1);
            loadedRun = run;
        }
        nextOrd = setStart;
    }

    @Override
    public boolean advanceExact(int target) {
        cursor.seekDoc(target);
        doc = target;
        loadSet(cursor.run());
        return setEnd > setStart;
    }

    @Override
    public long nextValue() {
        assert nextOrd < setEnd : "no more values for doc " + doc;
        return ordStream.get(nextOrd++);
    }

    @Override
    public int docValueCount() {
        return (int) (setEnd - setStart);
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() {
        if (doc == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
        }
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) {
        if (target >= maxDoc) {
            return doc = NO_MORE_DOCS;
        }
        cursor.seekDoc(target);
        int run = cursor.run();
        loadSet(run);
        if (setEnd > setStart) {
            return doc = target;
        }
        run++;
        if (run >= cursor.numRuns()) {
            return doc = NO_MORE_DOCS;
        }
        cursor.positionOn(run);
        loadSet(run);
        assert setEnd > setStart : "two consecutive empty runs at run " + run;
        return doc = cursor.startDoc(run);
    }

    @Override
    public long cost() {
        return numDocsWithField;
    }
}
