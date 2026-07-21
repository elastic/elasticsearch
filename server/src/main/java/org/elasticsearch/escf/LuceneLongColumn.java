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
 * A {@link LongColumn} backed by an {@link EscfLongColumn}.
 */
public final class LuceneLongColumn extends LongColumn implements LuceneColumn {

    private final EscfLongColumn data;

    private LuceneLongColumn(EscfLongColumn data, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        super(name, fieldType, Density.DENSE, kind);
        this.data = data;
    }

    public static LuceneLongColumn longColumn(byte[] values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        assert values.length % 8 == 0;
        int rowCount = values.length / 8;
        EscfLongColumn column = new EscfLongColumn(rowCount, null, new BytesArray(values));
        return new LuceneLongColumn(column, name, fieldType, kind);
    }

    @Override
    public LuceneLongColumn slice(int from, int count) {
        return new LuceneLongColumn((EscfLongColumn) data.sliceInternal(from, count), name(), fieldType(), numericKind());
    }

    @Override
    public Column toLuceneColumn() {
        return this;
    }

    @Override
    public LuceneColumn.RowFieldCursor rowFieldCursor() {
        final ColumnLongField field = new ColumnLongField(name(), fieldType(), numericKind());
        final LongTupleCursor cursor = data.longCursor();
        return new LuceneColumn.RowFieldCursor() {
            @Override
            public int nextDoc() {
                return cursor.nextDoc();
            }

            @Override
            public void appendCurrentFields(List<? super IndexableField> out) {
                field.setDocValue(cursor.longValue());
                out.add(field);
            }
        };
    }

    @Override
    public LongTupleCursor tuples() {
        return data.longCursor();
    }

    @Override
    public LongValuesCursor values() {
        return data.longValuesCursor();
    }

    private static final class ColumnLongField extends Field {

        private final LongColumn.NumericKind kind;

        ColumnLongField(String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
            super(name, fieldType);
            this.fieldsData = 0L;
            this.kind = kind;
        }

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
