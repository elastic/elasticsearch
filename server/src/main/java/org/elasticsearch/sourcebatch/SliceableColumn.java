/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.document.column.Column;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.DocIdSetIterator;

import java.util.List;

/**
 * A column that carries its own window ({@code [from, from + count)}) and can produce a Lucene
 * {@link Column} for that window on demand. Slicing a {@code SliceableColumn} yields a new instance
 * sharing the same backing data but adjusted to a sub-range — no copying occurs.
 */
public interface SliceableColumn {

    /**
     * Returns a view over {@code [from, from + count)} of this column's document range.
     *
     * @param from  start (inclusive) relative to this window, must be ≥ 0
     * @param count number of documents in the new window, must be ≥ 0 and {@code from + count} must
     *              be ≤ this column's document count
     */
    SliceableColumn slice(int from, int count);

    /**
     * Returns a Lucene {@link Column} for this column's current window.
     */
    Column toLuceneColumn();

    /**
     * Creates a forward-only cursor for the row-oriented (soft-update / non-{@code addBatch}) path.
     * The cursor iterates over documents in this column's current window; each position yields the
     * Lucene field(s) for that document via {@link RowFieldCursor#appendCurrentFields}.
     */
    RowFieldCursor rowFieldCursor();

    /**
     * A forward-only cursor over one column's fields for the row/soft-update indexing path.
     *
     * @see SliceableColumn#rowFieldCursor()
     */
    interface RowFieldCursor {

        /**
         * Advances to the next document with a value in this column's window. Returns the
         * batch-local doc-id (0-based, relative to the column's current window), or
         * {@link DocIdSetIterator#NO_MORE_DOCS} when the window is exhausted.
         */
        int nextDoc();

        /**
         * Appends this column's Lucene field(s) for the current document to {@code out}. Must be
         * called only after {@link #nextDoc()} returns a valid (non-{@code NO_MORE_DOCS}) doc-id.
         * The same field object(s) may be emitted on successive calls — field values are updated in
         * place between documents and are safe to reuse because the IndexWriter reads values
         * synchronously during {@code addDocument}.
         */
        void appendCurrentFields(List<? super IndexableField> out);
    }
}
