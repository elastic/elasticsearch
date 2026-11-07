/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.LongValues;

/**
 * The per-doc ordinals of a run table: {@link #longValue()} is the ordinal of the run under the cursor.
 * The producer wraps it into a {@code SortedDocValues} together with the terms dictionary.
 *
 * <p>Docs in a sentinel run have no value: {@link #advanceExact} returns {@code false} for them and
 * {@link #nextDoc}/{@link #advance} skip to the first doc of the next run. Runs are maximal, so a sentinel
 * run is always followed by a run with a value, or by the end of the segment.
 */
final class RunTableSortedDocValues extends NumericDocValues {

    private final RunTableCursor cursor;
    private final LongValues ordsPerRun;
    private final int maxDoc;
    private final int sentinel;
    private final int numDocsWithField;

    private int doc = -1;

    RunTableSortedDocValues(final RunTableCursor cursor, final LongValues ordsPerRun, int maxDoc, int sentinel, int numDocsWithField) {
        this.cursor = cursor;
        this.ordsPerRun = ordsPerRun;
        this.maxDoc = maxDoc;
        this.sentinel = sentinel;
        this.numDocsWithField = numDocsWithField;
    }

    @Override
    public long longValue() {
        return ordsPerRun.get(cursor.run());
    }

    @Override
    public boolean advanceExact(int target) {
        cursor.seekDoc(target);
        doc = target;
        return ordsPerRun.get(cursor.run()) != sentinel;
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
        if (ordsPerRun.get(run) != sentinel) {
            return doc = target;
        }
        run++;
        if (run >= cursor.numRuns()) {
            return doc = NO_MORE_DOCS;
        }
        cursor.positionOn(run);
        assert ordsPerRun.get(run) != sentinel : "two consecutive sentinel runs at run " + run;
        return doc = cursor.startDoc(run);
    }

    @Override
    public long cost() {
        return numDocsWithField;
    }
}
