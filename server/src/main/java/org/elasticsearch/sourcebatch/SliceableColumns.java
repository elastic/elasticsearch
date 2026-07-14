/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfLuceneColumn;

import java.util.ArrayList;
import java.util.List;

/**
 * The assembled columnar output of one batch-mapping chunk, backed by full-batch byte arrays and
 * capable of producing a Lucene {@link ColumnBatch} for any contiguous sub-range without copying.
 *
 * <p>Created by {@code BatchMappingContext#columns()} after all metadata mappers have run.
 * The engine {@linkplain #slice slices} one instance per sub-batch (determined by version-lock
 * acquisition in {@code InternalEngine.indexBatch}), fills the engine-assigned values ({@code
 * _seq_no}, {@code _primary_term}, {@code _version}) via {@link #setSeqNo}, {@link #fillPrimaryTerm},
 * and {@link #setVersion}, then calls {@link #toColumnBatch()} to obtain the Lucene
 * {@link ColumnBatch} handed to {@code IndexWriter#addBatch}.
 *
 * <h2>Slicing model</h2>
 * <p>The full-batch instance ({@code from=0, count=N}) shares its backing byte arrays with every
 * slice. Slicing calls {@link SliceableColumn#slice} on each registered column (adjusting their
 * internal window) and adjusts the {@code from} offset for the engine byte-write methods. Engine
 * writes use the offset ({@code array[(from + doc) * 8]}); the column cursors read from the same
 * absolute slot via the column's {@code base} (set to {@code from} after slicing). No copying occurs.
 *
 * <h2>Metadata long backing</h2>
 * <p>Engine-assigned long fields ({@code _seq_no}, {@code _primary_term}, {@code _version}) are
 * held as {@code byte[]} arrays of length {@code docCount * 8}, written via
 * {@link ByteUtils#writeLongLE} and read by the {@link EscfLuceneColumn} wrapping each array.
 * The {@code byte[]} is a live mutable buffer — engine writes are visible to the column's Lucene
 * cursors immediately without re-wrapping.
 *
 * <h2>Column factories</h2>
 * <p>The static factory methods {@link #longColumn} and {@link #binaryColumn} replace the former
 * {@code LuceneColumns.arrayLongColumn} / {@code LuceneColumns.arrayBinaryColumn} utilities.
 * Metadata mappers call them to produce a {@link SliceableColumn} and register it via
 * {@code BatchMappingContext#addColumn}.
 */
public final class SliceableColumns {

    private final int from;
    private final int count;

    /** Backing byte array for {@code _seq_no}; {@code null} if no seq-no column was registered. */
    @Nullable
    private final byte[] seqNos;

    /** Backing byte array for {@code _primary_term}; {@code null} if no primary-term column was registered. */
    @Nullable
    private final byte[] primaryTerms;

    /** Backing byte array for {@code _version}; {@code null} if no version column was registered. */
    @Nullable
    private final byte[] versions;

    private final List<SliceableColumn> columns;

    /**
     * Constructs a {@code SliceableColumns} covering the window {@code [from, from + count)} of the
     * given backing arrays and columns.
     *
     * @param from         offset into the full-batch backing arrays (as document index, not byte index)
     * @param count        number of documents in this window
     * @param seqNos       full-batch {@code _seq_no} backing array ({@code docCount * 8} bytes), or {@code null}
     * @param primaryTerms full-batch {@code _primary_term} backing array ({@code docCount * 8} bytes), or {@code null}
     * @param versions     full-batch {@code _version} backing array ({@code docCount * 8} bytes), or {@code null}
     * @param columns      all columns registered by the metadata mappers; copied defensively
     */
    public SliceableColumns(
        int from,
        int count,
        @Nullable byte[] seqNos,
        @Nullable byte[] primaryTerms,
        @Nullable byte[] versions,
        List<SliceableColumn> columns
    ) {
        this.from = from;
        this.count = count;
        this.seqNos = seqNos;
        this.primaryTerms = primaryTerms;
        this.versions = versions;
        this.columns = List.copyOf(columns);
    }

    /** The number of documents in this window. */
    public int docCount() {
        return count;
    }

    /**
     * Sets the engine-assigned {@code _seq_no} for batch-local document {@code doc} in this window.
     * No-op if no {@code _seq_no} column was registered.
     */
    public void setSeqNo(int doc, long value) {
        if (seqNos != null) {
            ByteUtils.writeLongLE(value, seqNos, (from + doc) * 8);
        }
    }

    /**
     * Sets the engine-assigned {@code _primary_term} for every document in this window.
     * No-op if no {@code _primary_term} column was registered.
     */
    public void fillPrimaryTerm(long value) {
        if (primaryTerms != null) {
            for (int i = 0; i < count; i++) {
                ByteUtils.writeLongLE(value, primaryTerms, (from + i) * 8);
            }
        }
    }

    /**
     * Sets the engine-assigned {@code _version} for batch-local document {@code doc} in this window.
     * No-op if no {@code _version} column was registered.
     */
    public void setVersion(int doc, long value) {
        if (versions != null) {
            ByteUtils.writeLongLE(value, versions, (from + doc) * 8);
        }
    }

    /**
     * Returns a view covering {@code [from, to)} of this window's document range. Each
     * {@link SliceableColumn} is sliced to the same sub-range (adjusting its internal window); the
     * backing byte arrays are shared with no copying. {@code from} and {@code to} are relative to
     * this window's {@code [0, count)}.
     *
     * @param from start (inclusive), relative to this window's {@code [0, count)}
     * @param to   end (exclusive), relative to this window's {@code [0, count)}
     * @throws IndexOutOfBoundsException if the range is invalid
     */
    public SliceableColumns slice(int from, int to) {
        if (from < 0 || to > this.count || from > to) {
            throw new IndexOutOfBoundsException("slice [" + from + ", " + to + ") out of [0, " + this.count + ")");
        }
        int newCount = to - from;
        List<SliceableColumn> slicedColumns = new ArrayList<>(columns.size());
        for (SliceableColumn c : columns) {
            slicedColumns.add(c.slice(from, newCount));
        }
        return new SliceableColumns(this.from + from, newCount, seqNos, primaryTerms, versions, slicedColumns);
    }

    /**
     * Assembles the registered columns into a Lucene {@link ColumnBatch} covering this window.
     * Each {@link SliceableColumn} is asked to produce a windowed {@link Column} via
     * {@link SliceableColumn#toLuceneColumn()}.
     */
    public ColumnBatch toColumnBatch() {
        final List<Column> luceneColumns = columns.stream().map(SliceableColumn::toLuceneColumn).toList();
        return new SliceableColumnBatch(luceneColumns, count);
    }

    // -------------------------------------------------------------------------
    // Lucene ColumnBatch wrapper
    // -------------------------------------------------------------------------

    private static final class SliceableColumnBatch extends ColumnBatch {
        private final List<Column> columns;
        private final int numDocs;

        SliceableColumnBatch(List<Column> columns, int numDocs) {
            this.columns = columns;
            this.numDocs = numDocs;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public Iterable<Column> columns() {
            return columns;
        }
    }

    // =========================================================================
    // Column factories (replace LuceneColumns.arrayLongColumn / arrayBinaryColumn)
    // =========================================================================

    /**
     * A {@link SliceableColumn} backed by a mutable {@code byte[]} of length {@code docCount * 8}
     * spanning the full batch. The array may be mutated (e.g. by the engine filling {@code _seq_no}/
     * {@code _version}) between registration and cursor access; engine writes are visible immediately
     * to the column's Lucene cursors. Always produces a
     * {@link org.apache.lucene.document.column.Column.Density#DENSE} column.
     *
     * @param values    the full-batch backing byte array of length {@code docCount * 8} (may be mutated
     *                  by the engine via {@link ByteUtils#writeLongLE})
     * @param name      Lucene field name
     * @param fieldType the Lucene field type for this column
     * @param kind      numeric kind ({@code LONG}, {@code INT}, etc.)
     */
    public static SliceableColumn longColumn(byte[] values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        return EscfLuceneColumn.longColumn(values, name, fieldType, kind);
    }

    /**
     * A {@link SliceableColumn} backed by a {@code BytesRef[]} spanning the full batch. A
     * {@code null} entry marks an absent document; the produced column is
     * {@link org.apache.lucene.document.column.Column.Density#DENSE} only when every entry in the
     * requested window is non-{@code null}.
     *
     * @param values    the full-batch backing array
     * @param name      Lucene field name
     * @param fieldType the Lucene field type for this column
     */
    public static SliceableColumn binaryColumn(BytesRef[] values, String name, IndexableFieldType fieldType) {
        return new WindowedBinaryColumn(values, name, fieldType, 0, values.length);
    }

    // =========================================================================
    // Windowed column implementations
    // =========================================================================

    /**
     * A {@link BinaryColumn} backed by a full-batch {@code BytesRef[]} and a {@code [from, from+count)} window.
     * Density is {@code DENSE} iff every entry in the window is non-{@code null}. Implements
     * {@link SliceableColumn}: {@link #slice} re-windows over the same backing array; {@link #toLuceneColumn}
     * returns {@code this} (a {@link Column} is a {@link BinaryColumn} is a {@link Column}).
     */
    private static final class WindowedBinaryColumn extends BinaryColumn implements SliceableColumn {
        private final BytesRef[] values;
        private final int from;
        private final int count;
        private final boolean dense;
        private final IndexableFieldType fieldType;

        WindowedBinaryColumn(BytesRef[] values, String name, IndexableFieldType fieldType, int from, int count) {
            super(name, fieldType, allPresent(values, from, count) ? Density.DENSE : Density.SPARSE);
            this.values = values;
            this.from = from;
            this.count = count;
            this.dense = allPresent(values, from, count);
            this.fieldType = fieldType;
        }

        private static boolean allPresent(BytesRef[] values, int from, int count) {
            for (int i = from; i < from + count; i++) {
                if (values[i] == null) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public SliceableColumn slice(int from, int count) {
            return new WindowedBinaryColumn(values, name(), fieldType, this.from + from, count);
        }

        @Override
        public Column toLuceneColumn() {
            return this;
        }

        @Override
        public ObjectTupleCursor<BytesRef> tuples() {
            // srcIdx tracks position in the full backing array; doc is the batch-local id.
            return new ObjectTupleCursor<>() {
                private int doc = -1;
                private int srcIdx = from - 1;

                @Override
                public int nextDoc() {
                    srcIdx++;
                    final int end = from + count;
                    while (srcIdx < end && values[srcIdx] == null) {
                        srcIdx++;
                    }
                    if (srcIdx >= end) {
                        doc = DocIdSetIterator.NO_MORE_DOCS;
                    } else {
                        doc = srcIdx - from;
                    }
                    return doc;
                }

                @Override
                public BytesRef value() {
                    return values[srcIdx];
                }
            };
        }

        @Override
        public BytesRefValuesCursor values() {
            if (dense == false) {
                return super.values(); // throws; never consulted for SPARSE columns
            }
            return new BytesRefValuesCursor(count) {
                private int pos;

                @Override
                public BytesRef nextValue() {
                    if (pos >= size()) {
                        throw new IllegalStateException("nextValue() called more than size()=" + size() + " times");
                    }
                    return values[from + pos++];
                }
            };
        }
    }
}
