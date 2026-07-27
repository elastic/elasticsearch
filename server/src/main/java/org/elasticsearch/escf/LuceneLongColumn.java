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
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.sourcebatch.LuceneColumn;

import java.util.List;

/**
 * A {@link LongColumn} backed by an {@link EscfLongColumn}.
 */
public final class LuceneLongColumn extends LongColumn implements LuceneColumn {

    private final EscfLongColumn data;

    private LuceneLongColumn(EscfLongColumn data, String name, IndexableFieldType fieldType, Density density, LongColumn.NumericKind kind) {
        super(name, fieldType, density, kind);
        this.data = data;
    }

    /**
     * Creates a dense {@link LuceneLongColumn}: every document from {@code 0} to
     * {@code values.length / 8 - 1} has a value. The byte array is interpreted as little-endian
     * 64-bit longs, one per document.
     */
    public static LuceneLongColumn longColumn(byte[] values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        assert values.length % 8 == 0;
        int rowCount = values.length / 8;
        EscfLongColumn column = new EscfLongColumn(rowCount, null, new BytesArray(values));
        return new LuceneLongColumn(column, name, fieldType, Density.DENSE, kind);
    }

    /**
     * Creates a sparse {@link LuceneLongColumn}: only documents whose bit is set in {@code validity}
     * have a value. The {@code values} array has one little-endian 64-bit slot per document
     * (including absent ones); absent slots may hold any value (they are never read).
     *
     * @param values   raw byte array, {@code docCount * 8} bytes; one 8-byte little-endian long per
     *                 document position (present or absent).
     * @param validity the presence bitset; {@code null} is not allowed for a sparse factory (use
     *                 {@link #longColumn} for a dense column).
     * @param docCount total number of documents (including absent ones).
     */
    public static LuceneLongColumn sparseLongColumn(
        byte[] values,
        FixedBitSet validity,
        int docCount,
        String name,
        IndexableFieldType fieldType,
        LongColumn.NumericKind kind
    ) {
        assert validity != null : "use longColumn() for a dense (all-present) column";
        assert values.length == docCount * 8 : "values.length must equal docCount * 8";
        EscfLongColumn column = new EscfLongColumn(docCount, validity, new BytesArray(values));
        return new LuceneLongColumn(column, name, fieldType, Density.SPARSE, kind);
    }

    /**
     * Creates a {@link LuceneLongColumn} from a LONG {@link EscfColumnData}, dispatching to
     * {@link Density#DENSE} when every document is present ({@code data.validity() == null}) and
     * {@link Density#SPARSE} otherwise.
     */
    public static LuceneLongColumn of(EscfColumnData data, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        assert data.kind() == EscfColumnKind.LONG : "expected LONG, got " + EscfColumnKind.name(data.kind());
        EscfLongColumn col = (EscfLongColumn) EscfColumn.from(data);
        Density density = data.validity() == null ? Density.DENSE : Density.SPARSE;
        return new LuceneLongColumn(col, name, fieldType, density, kind);
    }

    @Override
    public LuceneLongColumn slice(int from, int count) {
        return new LuceneLongColumn((EscfLongColumn) data.sliceInternal(from, count), name(), fieldType(), density(), numericKind());
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
        if (density() == Density.SPARSE) {
            // Sparse columns must be consumed via tuples(); the dense values cursor is undefined for absent rows.
            return super.values();
        }
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
