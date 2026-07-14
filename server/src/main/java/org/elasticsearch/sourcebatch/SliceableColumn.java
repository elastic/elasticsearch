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
 *
 * <p>This interface unifies two formerly separate column worlds:
 * <ul>
 *   <li><em>ESCF columns</em> ({@code EscfColumn} subtypes) — the source/translog read path; slicing
 *       re-windows array-backed factors (offset vectors, type bytes) via Ref adjustment and rewrites
 *       the bitsets zero-based, while sharing the underlying backing arrays and
 *       {@link org.elasticsearch.common.bytes.BytesReference} data payloads.</li>
 *   <li><em>Engine-metadata columns</em> ({@code EscfLuceneColumn}, {@code WindowedBinaryColumn}) —
 *       the mapping→Lucene write path; slicing re-windows a backing array (mutable {@code byte[]} or
 *       {@code BytesRef[]}) written by the engine after mapping.</li>
 * </ul>
 *
 * <p>Both kinds are registered on {@link org.elasticsearch.index.mapper.BatchMappingContext} by
 * metadata mappers and assembled into a {@link SliceableColumns} by
 * {@code BatchMappingContext#columns()}. The engine slices the {@code SliceableColumns} per
 * sub-batch (version-lock acquisition), fills engine-assigned values, then calls
 * {@link SliceableColumns#toColumnBatch()} which drives {@link #toLuceneColumn()} on each column
 * for the {@code addBatch} path, or {@link #rowFieldCursor()} for the row/soft-update path.
 */
public interface SliceableColumn {

    /**
     * Returns a view over {@code [from, from + count)} of this column's document range. The returned
     * instance shares the same backing data — no copying occurs. {@code from} is relative to this
     * column's current window {@code [0, docCount)}.
     *
     * @param from  start (inclusive) relative to this window, must be ≥ 0
     * @param count number of documents in the new window, must be ≥ 0 and {@code from + count} must
     *              be ≤ this column's document count
     */
    SliceableColumn slice(int from, int count);

    /**
     * Returns a Lucene {@link Column} for this column's current window. The returned column's
     * cursors iterate exactly {@code count} documents (where {@code count} is this column's current
     * window size), reading from the backing data starting at this column's {@code from} offset.
     */
    Column toLuceneColumn();

    /**
     * Creates a forward-only cursor for the row-oriented (soft-update / non-{@code addBatch}) path.
     * The cursor iterates over documents in this column's current window; each position yields the
     * Lucene field(s) for that document via {@link RowFieldCursor#appendCurrentFields}.
     *
     * <p>Field objects emitted by the cursor may be reused across documents — their values are
     * updated in place. Callers must let the IndexWriter consume field values (e.g. via
     * {@code addDocument}) before advancing the cursor.
     *
     * <p>Multi-valued columns emit multiple entries per {@link RowFieldCursor#appendCurrentFields}
     * call; the caller accumulates all fields into a shared list before building each document.
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
