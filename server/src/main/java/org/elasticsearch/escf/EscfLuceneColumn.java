/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.sourcebatch.SliceableColumn;

import java.util.List;

/**
 * A {@link SliceableColumn} that binds an {@link EscfLongColumn} to a Lucene {@link LongColumn}.
 *
 * <p>The {@code byte[]} is wrapped in a {@link BytesArray} (a live, single-contiguous-page view),
 * so engine writes made after registration are immediately visible to the column's cursors —
 * {@link BytesArray#get} reads directly from the backing array on every call. No copies occur and
 * no deferred construction is needed.
 *
 * <p>Slicing ({@link #slice}) delegates to {@link EscfColumn#sliceInternal} which calls
 * {@link org.elasticsearch.common.bytes.BytesReference#slice} on the inner column's {@code data}.
 * {@link org.elasticsearch.common.bytes.BytesArray#slice} returns a live view sharing the same
 * backing array, so the engine's post-mapping writes to the absolute byte slot
 * {@code (from + doc) * 8} are immediately visible through the sliced column's cursors.
 *
 * <p>Use the static factory {@link #longColumn} to create instances; the constructor is private.
 */
public final class EscfLuceneColumn implements SliceableColumn {

    private final EscfColumn values;
    private final String name;
    private final IndexableFieldType fieldType;
    private final LongColumn.NumericKind kind;

    private EscfLuceneColumn(EscfColumn values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        this.values = values;
        this.name = name;
        this.fieldType = fieldType;
        this.kind = kind;
    }

    /**
     * Creates a sliceable long column backed by the given mutable byte array. The array is wrapped
     * in a live {@link BytesArray} view — engine writes to the array are immediately visible to
     * the column's cursors without copying.
     *
     * @param values    mutable byte array of length {@code docCount * 8}; each 8-byte slot holds
     *                  one little-endian long value (written via
     *                  {@link org.elasticsearch.common.util.ByteUtils#writeLongLE})
     * @param name      Lucene field name
     * @param fieldType the Lucene field type for this column
     * @param kind      numeric kind ({@code LONG}, {@code INT}, etc.)
     */
    public static EscfLuceneColumn longColumn(byte[] values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        BytesArray data = new BytesArray(values);
        int docCount = values.length / 8;
        // Dense: no absent set (metadata columns always have a value for every document).
        EscfLongColumn column = new EscfLongColumn(docCount, null, data);
        return new EscfLuceneColumn(column, name, fieldType, kind);
    }

    @Override
    public SliceableColumn slice(int from, int count) {
        // Safe cast: EscfColumn.sliceInternal always returns an EscfColumn subtype.
        EscfColumn sliced = values.sliceInternal(from, count);
        return new EscfLuceneColumn(sliced, name, fieldType, kind);
    }

    @Override
    public RowFieldCursor rowFieldCursor() {
        // EscfLuceneColumn is always DENSE (no absent set): every doc in [0, docCount) has a value.
        final ColumnLongField field = new ColumnLongField(name, fieldType, kind);
        final int docCount = values.docCount;
        return new RowFieldCursor() {
            private int doc = -1;

            @Override
            public int nextDoc() {
                return ++doc < docCount ? doc : DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public void appendCurrentFields(List<? super IndexableField> out) {
                field.setDocValue(values.getLongValue(doc));
                out.add(field);
            }
        };
    }

    @Override
    public Column toLuceneColumn() {
        final int docCount = values.docCount;
        return new LongColumn(name, fieldType, LongColumn.Density.DENSE, kind) {
            @Override
            public LongTupleCursor tuples() {
                return new LongTupleCursor() {
                    private int doc = -1;

                    @Override
                    public int nextDoc() {
                        return ++doc < docCount ? doc : DocIdSetIterator.NO_MORE_DOCS;
                    }

                    @Override
                    public long longValue() {
                        return values.getLongValue(doc);
                    }
                };
            }

            @Override
            public LongValuesCursor values() {
                return new LongValuesCursor(docCount) {
                    private int pos;

                    @Override
                    public long nextLong() {
                        if (pos >= size()) {
                            throw new IllegalStateException("nextLong() called more than size()=" + size() + " times");
                        }
                        return values.getLongValue(pos++);
                    }

                    @Override
                    public void fillDocValues(long[] dst, int offset, int length) {
                        if (pos + length > size()) {
                            throw new IllegalStateException("fill of " + length + " from pos " + pos + " exceeds size()=" + size());
                        }
                        // Read through EscfLongColumn (byte[]-backed BytesArray LE read) rather than
                        // System.arraycopy — the backing is byte[], not long[].
                        for (int i = 0; i < length; i++) {
                            dst[offset + i] = values.getLongValue(pos++);
                        }
                    }
                };
            }
        };
    }

    private static final class ColumnLongField extends Field {

        private final LongColumn.NumericKind kind;

        ColumnLongField(String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
            super(name, fieldType);
            this.fieldsData = 0L;
            this.kind = kind;
        }

        /** Updates this field's long value to {@code v} for the next document. */
        void setDocValue(long v) {
            fieldsData = v;
        }

        @Override
        public BytesRef binaryValue() {
            // Consulted by the indexing chain only when fieldType.pointDimensionCount() > 0.
            final long raw = (Long) fieldsData;
            return switch (kind) {
                case LONG, DOUBLE -> {
                    final byte[] buf = new byte[Long.BYTES];
                    NumericUtils.longToSortableBytes(raw, buf, 0);
                    yield new BytesRef(buf);
                }
                case INT, FLOAT -> {
                    final byte[] buf = new byte[Integer.BYTES];
                    NumericUtils.intToSortableBytes((int) raw, buf, 0);
                    yield new BytesRef(buf);
                }
            };
        }
    }
}
