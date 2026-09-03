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
import org.apache.lucene.util.BitSetIterator;
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
     * The returned column retains the density of the underlying data: a dense column stays dense (it
     * emits {@code filter.cardinality()} values at the filter-bit positions), while a sparse column
     * stays sparse (it emits tuples with compact doc IDs 0-based in the filter). Pass {@code null}
     * to remove any existing filter.
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
     * Stateful helper for co-iterating a sparse data cursor with a filter bitset and mapping
     * matching positions to compact doc IDs (0-based rank within the filter). One instance is
     * created per filtered cursor; it is consumed in a single forward pass and must not be shared.
     *
     * <p>Typical usage:
     * <pre>{@code
     * FilteredIterator fi = new FilteredIterator(filter);
     * // inside nextDoc():
     * while (true) {
     *     int compact = fi.advancePast(inner.nextDoc());
     *     if (compact != LuceneColumn.FilteredIterator.EXCLUDED) return compact;
     * }
     * }</pre>
     */
    final class FilteredIterator {

        /** Returned by {@link #advancePast} when {@code innerDoc} is excluded by the filter. */
        public static final int EXCLUDED = -1;

        private final BitSetIterator filterBits;
        private int filterBit;
        private int compactDoc = 0;

        public FilteredIterator(FixedBitSet filter) {
            filterBits = new BitSetIterator(filter, filter.cardinality());
            filterBit = filterBits.nextDoc();
        }

        /**
         * Advances the filter iterator to catch up with {@code innerDoc}, then returns:
         * <ul>
         *   <li>the compact doc ID if {@code innerDoc} is a set bit in the filter,</li>
         *   <li>{@link #EXCLUDED} if {@code innerDoc} is excluded by the filter (caller should
         *       advance the data cursor and retry), or</li>
         *   <li>{@link DocIdSetIterator#NO_MORE_DOCS} if either the data or filter is
         *       exhausted.</li>
         * </ul>
         */
        public int advancePast(int innerDoc) {
            if (innerDoc == DocIdSetIterator.NO_MORE_DOCS || filterBit == DocIdSetIterator.NO_MORE_DOCS) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            while (filterBit < innerDoc) {
                filterBit = filterBits.nextDoc();
                compactDoc++;
                if (filterBit == DocIdSetIterator.NO_MORE_DOCS) {
                    return DocIdSetIterator.NO_MORE_DOCS;
                }
            }
            return filterBit == innerDoc ? compactDoc : EXCLUDED;
        }
    }

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
