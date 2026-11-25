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
import org.apache.lucene.util.FixedBitSet;

import java.util.List;

/**
 * A {@link SliceableColumn} that can also produce Lucene fields for both the columnar
 * ({@code IndexWriter.addBatch}) and row-oriented ({@code IndexWriter.addDocument}) indexing paths.
 */
public interface LuceneColumn extends SliceableColumn {

    @Override
    LuceneColumn slice(int from, int count);

    /**
     * Returns a copy of this column that emits only documents whose bit is set in {@code filter}.
     * When {@code filter} is non-null the returned column has {@link Column.Density#SPARSE} density.
     * Passing {@code null} is a no-op when the column already has an active filter: the existing
     * filter is preserved. Use {@link #singleFilter} to compose the incoming value with any
     * pre-existing filter.
     *
     * @param filter a bitset of length equal to this column's doc count, or {@code null}
     */
    LuceneColumn withFilter(FixedBitSet filter);

    /**
     * Returns the single non-null filter from {@code existing} and {@code replacement}, asserting
     * that they are not both non-null. If both are {@code null}, returns {@code null}. Intended for
     * use inside {@link #withFilter} implementations to prevent an active filter from being silently
     * discarded.
     */
    static FixedBitSet singleFilter(FixedBitSet existing, FixedBitSet replacement) {
        assert existing == null || replacement == null : "cannot apply a filter to a column that already has one";
        return replacement != null ? replacement : existing;
    }

    /**
     * Returns a Lucene {@link Column} for this column's current window, for use with
     * {@code IndexWriter.addBatch}.
     */
    Column toLuceneColumn();

    /**
     * Like {@link #toLuceneColumn()}, for a column that has a {@link #withFilter filter}: the documents that pass the filter are
     * renumbered to the doc ids in {@code compactedDocIds}. Lucene adds one document per doc id of a batch, so the documents a filter
     * excludes must not keep one, and every column of the batch has to number the remaining ones alike.
     *
     * <p>The result is a view and never the column itself, whose own cursors keep the numbering of the window for
     * {@link #rowFieldCursor()}. {@link CompactedColumns} has such a view for each type of Lucene column.
     *
     * @param compactedDocIds the doc id each document of this column's window takes in the batch, {@code -1} for the excluded ones
     */
    Column toLuceneColumn(int[] compactedDocIds);

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
