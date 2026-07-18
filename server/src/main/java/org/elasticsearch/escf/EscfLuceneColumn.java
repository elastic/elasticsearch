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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.sourcebatch.LuceneColumn;

import java.util.List;

/**
 * A {@link LuceneColumn} that binds an {@link EscfLongColumn} to a Lucene {@link LongColumn}.
 *
 * <p>The {@code byte[]} is wrapped in a {@link BytesArray} (a live, single-contiguous-page view),
 * so engine writes made after registration are immediately visible to the column's cursors —
 * {@link BytesArray#get} reads directly from the backing array on every call. No copies occur and
 * no deferred construction is needed.
 *
 * <p>Use the static factory {@link #longColumn} to create instances; the constructor is private.
 *
 * <p>Currently this column implementation is always dense.
 */
public final class EscfLuceneColumn implements LuceneColumn {

    private final EscfColumn values;
    private final String name;
    private final IndexableFieldType fieldType;
    private final LongColumn.NumericKind kind;

    private EscfLuceneColumn(EscfColumn values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        this.values = values;
        this.name = name;
        this.fieldType = fieldType;
        this.kind = kind;
        // Currently always dense. Will eventually support sparse.
        assert this.values.absent == null;
    }

    public static EscfLuceneColumn longColumn(byte[] values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        assert values.length % 8 == 0;
        BytesArray data = new BytesArray(values);
        int docCount = values.length / 8;
        // Dense: no absent set.
        EscfLongColumn column = new EscfLongColumn(docCount, null, data);
        return new EscfLuceneColumn(column, name, fieldType, kind);
    }

    @Override
    public EscfLuceneColumn slice(int from, int count) {
        EscfColumn sliced = values.sliceInternal(from, count);
        return new EscfLuceneColumn(sliced, name, fieldType, kind);
    }

    @Override
    public LuceneColumn.RowFieldCursor rowFieldCursor() {
        // EscfLuceneColumn is always DENSE (no absent set): every row in [0, docCount) has a value.
        final ColumnLongField field = new ColumnLongField(name, fieldType, kind);
        final EscfLongColumn.LongCursor cursor = ((EscfLongColumn) values).longCursor();
        return new LuceneColumn.RowFieldCursor() {
            @Override
            public int nextDoc() {
                return cursor.nextRow();
            }

            @Override
            public void appendCurrentFields(List<? super IndexableField> out) {
                field.setDocValue(cursor.longValue());
                out.add(field);
            }
        };
    }

    @Override
    public Column toLuceneColumn() {
        final EscfLongColumn longValues = (EscfLongColumn) values;
        return new LongColumn(name, fieldType, LongColumn.Density.DENSE, kind) {
            @Override
            public LongTupleCursor tuples() {
                final EscfLongColumn.LongCursor cursor = longValues.longCursor();
                return new LongTupleCursor() {
                    @Override
                    public int nextDoc() {
                        return cursor.nextRow();
                    }

                    @Override
                    public long longValue() {
                        return cursor.longValue();
                    }
                };
            }

            @Override
            public LongValuesCursor values() {
                return new LongValuesCursor(longValues.docCount) {
                    private int pos;

                    @Override
                    public long nextLong() {
                        if (pos >= size()) {
                            throw new IllegalStateException("nextLong() called more than size()=" + size() + " times");
                        }
                        return longValues.getLongValue(pos++);
                    }

                    @Override
                    public void fillDocValues(long[] dst, int offset, int length) {
                        if (pos + length > size()) {
                            throw new IllegalStateException("fill of " + length + " from pos " + pos + " exceeds size()=" + size());
                        }
                        for (int i = 0; i < length; i++) {
                            dst[offset + i] = longValues.getLongValue(pos++);
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
