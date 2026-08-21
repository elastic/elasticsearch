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
 * A {@link LongColumn} backed by an {@link EscfLongColumn} (single-value) or {@link EscfArrayColumn}
 * (multi-value). Multi-value columns always use {@link Density#SPARSE}.
 */
public final class LuceneLongColumn extends LongColumn implements LuceneColumn {

    private final EscfColumn data;

    private LuceneLongColumn(EscfColumn data, String name, IndexableFieldType fieldType, Density density, LongColumn.NumericKind kind) {
        super(name, fieldType, density, kind);
        this.data = data;
    }

    /**
     * Creates a dense {@link LuceneLongColumn}: every document from {@code 0} to
     * {@code values.length / 8 - 1} has a value. The buffer is interpreted as little-endian
     * 64-bit longs, one per document.
     */
    public static LuceneLongColumn longColumn(BytesRef values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        assert values.length % 8 == 0;
        int rowCount = values.length / 8;
        EscfLongColumn column = new EscfLongColumn(rowCount, null, new BytesArray(values.bytes, values.offset, values.length));
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
        return new LuceneLongColumn(column, name, fieldType, Density.SPARSE, kind);  // validity != null → always sparse
    }

    /**
     * Creates a {@link LuceneLongColumn} from a LONG or ARRAY {@link EscfColumnData}.
     * <ul>
     *   <li>LONG: {@link Density#DENSE} when every document is present ({@code validity == null}),
     *       {@link Density#SPARSE} otherwise.</li>
     *   <li>ARRAY: always {@link Density#SPARSE} — multi-value rows are iterated element-granularly
     *       via {@link LongColumn#tuples()} and {@link LuceneColumn.RowFieldCursor#appendCurrentFields}
     *       is called once per element per row.</li>
     * </ul>
     */
    public static LuceneLongColumn of(EscfColumnData data, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        assert data.kind() == EscfColumnKind.LONG || data.kind() == EscfColumnKind.ARRAY
            : "expected LONG or ARRAY, got " + EscfColumnKind.name(data.kind());
        EscfColumn col = EscfColumn.from(data);
        Density density = (data.kind() == EscfColumnKind.LONG && data.validity() == null) ? Density.DENSE : Density.SPARSE;
        return new LuceneLongColumn(col, name, fieldType, density, kind);
    }

    @Override
    public LuceneLongColumn slice(int from, int count) {
        EscfColumn sliced = data.sliceInternal(from, count);
        Density density = (sliced instanceof EscfLongColumn l && l.validity == null) ? Density.DENSE : Density.SPARSE;
        return new LuceneLongColumn(sliced, name(), fieldType(), density, numericKind());
    }

    @Override
    public Column toLuceneColumn() {
        return this;
    }

    @Override
    public LuceneColumn.RowFieldCursor rowFieldCursor() {
        final LongTupleCursor cursor = data.longCursor();
        if (data instanceof EscfArrayColumn) {
            // Multi-value: appendCurrentFields is called multiple times for the same row. Each call must
            // produce an independent field snapshot; reusing one mutable object would corrupt earlier
            // entries in the accumulation list when the value is updated for the next element.
            return new LuceneColumn.RowFieldCursor() {
                @Override
                public int nextDoc() {
                    return cursor.nextDoc();
                }

                @Override
                public void appendCurrentFields(List<? super IndexableField> out) {
                    ColumnLongField f = new ColumnLongField(name(), fieldType(), numericKind());
                    f.setDocValue(cursor.longValue());
                    out.add(f);
                }
            };
        }
        // Single-value: the field object is safe to reuse across rows (values are read synchronously
        // per-row and the field is not shared across concurrent docs).
        final ColumnLongField field = new ColumnLongField(name(), fieldType(), numericKind());
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
        // DENSE is only set when data is an EscfLongColumn with no validity bitset (see factory methods).
        return ((EscfLongColumn) data).longValuesCursor();
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
