/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * A {@link DocIdSetIterator} that computes a {@link Vocabulary.Surveyor} as a side effect of its
 * {@link #nextDoc()} walk. Passed into {@link org.elasticsearch.columnar.substrate.ColumnIteratorWriter#write
 * ColumnIteratorWriter.write} so that writing the column's {@link org.apache.lucene.codecs.lucene90.IndexedDISI
 * IndexedDISI} presence structure and surveying the vocabulary happen in the same pass over the cursor.
 *
 * <p>{@link org.apache.lucene.codecs.lucene90.IndexedDISI#writeBitSet IndexedDISI.writeBitSet} calls
 * {@link #nextDoc()} once, then advances through the default {@link DocIdSetIterator#intoBitSet intoBitSet},
 * which loops on {@link #nextDoc()}. Every document therefore passes through exactly one {@code nextDoc()}
 * call, and the survey sees every document. {@code intoBitSet} is deliberately <em>not</em> overridden here:
 * delegating it to {@code values} would advance the underlying cursor behind this iterator's back, skipping
 * documents in the survey. The assert in {@link #finish()} confirms the contract holds if {@code writeBitSet}
 * changes in a future Lucene version.
 *
 * <p>{@link #advance(int)} is not supported — {@code writeBitSet} never calls it, and that is the only
 * caller.
 */
final class SurveyingDocs extends DocIdSetIterator {

    private final StringColumnValues values;
    private final Vocabulary.Surveyor surveyor;
    private final int numDocsWithField;
    private int docsSeen;

    SurveyingDocs(StringColumnValues values, Vocabulary.Surveyor surveyor, int numDocsWithField) {
        this.values = values;
        this.surveyor = surveyor;
        this.numDocsWithField = numDocsWithField;
    }

    @Override
    public int nextDoc() throws IOException {
        final int doc = values.nextDoc();
        if (doc != NO_MORE_DOCS) {
            docsSeen++;
            for (int i = 0, count = values.valueCount(); i < count; i++) {
                values.nextValue();
                surveyor.accept(values.value());
            }
        }
        return doc;
    }

    /**
     * Returns the survey result. Must be called after {@code ColumnIteratorWriter.write} has
     * exhausted this iterator.
     */
    Vocabulary.Terms finish() {
        assert docsSeen == numDocsWithField
            : "writeBitSet advanced the iterator in an unexpected way: saw "
                + docsSeen
                + " docs but expected "
                + numDocsWithField
                + "; the survey may be incomplete";
        return surveyor.finish();
    }

    @Override
    public int docID() {
        return values.docID();
    }

    @Override
    public int advance(int target) {
        throw new UnsupportedOperationException("writeBitSet never calls advance");
    }

    @Override
    public long cost() {
        return values.cost();
    }
}
