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
 * A {@link SliceableColumn} that can also produce Lucene fields for both the columnar
 * ({@code IndexWriter.addBatch}) and row-oriented ({@code IndexWriter.addDocument}) indexing paths.
 *
 * <p>ESCF columns ({@link SliceableColumn}) hold data in a windowed, Arrow-compatible layout.
 * Implementations of this interface are thin adapters that wrap an ESCF column and bridge it to the
 * two Lucene indexing paths. The {@link #slice} override is covariant so that
 * {@link MappedColumns#slice} can collect sliced {@code LuceneColumn} instances without casting.
 */
public interface LuceneColumn extends SliceableColumn {

    @Override
    LuceneColumn slice(int from, int count);

    /**
     * Returns a Lucene {@link Column} for this column's current window, for use with
     * {@code IndexWriter.addBatch}.
     */
    Column toLuceneColumn();

    /**
     * Creates a forward-only cursor for the row-oriented (soft-update / non-{@code addBatch}) path.
     * The cursor iterates over rows in this column's current window; each position yields the
     * Lucene field(s) for that row via {@link RowFieldCursor#appendCurrentFields}.
     */
    RowFieldCursor rowFieldCursor();

    /**
     * A forward-only cursor over one column's fields for the row/soft-update indexing path.
     *
     * @see LuceneColumn#rowFieldCursor()
     */
    interface RowFieldCursor {

        /**
         * Advances to the next row with a value in this column's window. Returns the
         * batch-local row-id (0-based, relative to the column's current window), or
         * {@link DocIdSetIterator#NO_MORE_DOCS} when the window is exhausted.
         */
        int nextDoc();

        /**
         * Appends this column's Lucene field(s) for the current row to {@code out}. Must be
         * called only after {@link #nextDoc()} returns a valid (non-{@code NO_MORE_DOCS}) row-id.
         * The same field object(s) may be emitted on successive calls — field values are updated in
         * place between rows and are safe to reuse because the IndexWriter reads values
         * synchronously during {@code addDocument}.
         */
        void appendCurrentFields(List<? super IndexableField> out);
    }
}
