/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.sourcebatch.LuceneColumn;

import java.util.List;
import java.util.Objects;

/**
 * A {@link LongColumn} backed by an {@link EscfLongColumn} (single-value) or {@link EscfArrayColumn}
 * (multi-value). Multi-value columns always use {@link Density#SPARSE}.
 */
public final class LuceneLongColumn extends LongColumn implements LuceneColumn {

    private static final IndexableFieldType SORTED_NUMERIC_DV_FIELD_TYPE = SortedNumericDocValuesField.TYPE;
    private static final IndexableFieldType SORTED_NUMERIC_DV_INDEXED_FIELD_TYPE = SortedNumericDocValuesField.indexedField("_sentinel", 0L)
        .fieldType();
    private static final IndexableFieldType SORTED_NUMERIC_DV_FIELD_TYPE_STORED = storedVariant(SORTED_NUMERIC_DV_FIELD_TYPE);
    private static final IndexableFieldType SORTED_NUMERIC_DV_INDEXED_FIELD_TYPE_STORED = storedVariant(
        SORTED_NUMERIC_DV_INDEXED_FIELD_TYPE
    );

    // BKD + DV combined field types, one per NumericKind. Used by Builder to select the indexed field type.
    private static final IndexableFieldType LONG_INDEXED_FIELD_TYPE = new LongField("_sentinel", 0L, Field.Store.NO).fieldType();
    private static final IndexableFieldType INT_INDEXED_FIELD_TYPE = new IntField("_sentinel", 0, Field.Store.NO).fieldType();
    private static final IndexableFieldType FLOAT_INDEXED_FIELD_TYPE = new FloatField("_sentinel", 0f, Field.Store.NO).fieldType();
    private static final IndexableFieldType DOUBLE_INDEXED_FIELD_TYPE = new DoubleField("_sentinel", 0.0, Field.Store.NO).fieldType();

    // Stored-only field types (no doc values, no BKD), one per NumericKind.
    private static final IndexableFieldType LONG_STORED_ONLY_FIELD_TYPE = new StoredField("_sentinel", 0L).fieldType();
    private static final IndexableFieldType INT_STORED_ONLY_FIELD_TYPE = new StoredField("_sentinel", 0).fieldType();
    private static final IndexableFieldType FLOAT_STORED_ONLY_FIELD_TYPE = new StoredField("_sentinel", 0f).fieldType();
    private static final IndexableFieldType DOUBLE_STORED_ONLY_FIELD_TYPE = new StoredField("_sentinel", 0.0).fieldType();

    private static IndexableFieldType storedVariant(IndexableFieldType base) {
        FieldType ft = new FieldType(base);
        ft.setStored(true);
        ft.freeze();
        return ft;
    }

    /**
     * Returns a new {@link Builder} for constructing a {@link Spec}: a pre-computed column factory
     * that captures all static configuration ({@code name}, field type, {@link LongColumn.NumericKind})
     * at mapper construction time. Only the batch data is needed at {@link Spec#build} time.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A pre-computed column factory produced by {@link Builder#build()}. Holds the static column
     * configuration ({@code name}, {@link IndexableFieldType}, {@link LongColumn.NumericKind})
     * so that {@link #build(EscfColumnData)} only needs the per-batch data.
     */
    public static final class Spec {
        private final String name;
        private final IndexableFieldType fieldType;
        private final LongColumn.NumericKind numericKind;

        private Spec(String name, IndexableFieldType fieldType, LongColumn.NumericKind numericKind) {
            this.name = name;
            this.fieldType = fieldType;
            this.numericKind = numericKind;
        }

        /** Creates a {@link LuceneLongColumn} from the pre-computed configuration and the given batch data. */
        public LuceneLongColumn build(EscfColumnData data) {
            return LuceneLongColumn.of(data, name, fieldType, numericKind);
        }
    }

    /**
     * Builder for {@link Spec}. Captures all static column configuration at mapper construction
     * time and selects the appropriate {@link IndexableFieldType} from the combination of
     * {@link IndexType}, {@code indexed}, {@code stored}, and {@link LongColumn.NumericKind}.
     *
     * <p>The field-type selection follows the same logic as the row-major path:
     * <ul>
     *   <li>If {@link IndexType#hasDocValuesSkipper()}: a {@code SortedNumericDocValuesField} with
     *       a doc-values-skipper index, optionally with {@code stored}.</li>
     *   <li>Else if {@code indexed}: a combined BKD + DV field ({@code LongField}, {@code IntField},
     *       etc., derived from {@link LongColumn.NumericKind}), optionally with {@code stored}.</li>
     *   <li>Otherwise: a plain {@code SortedNumericDocValuesField}, optionally with {@code stored}.</li>
     * </ul>
     */
    public static final class Builder {
        private String name;
        private IndexType indexType;
        private boolean indexed;
        private boolean stored;
        private LongColumn.NumericKind numericKind = LongColumn.NumericKind.LONG;

        private Builder() {}

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder indexType(IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder indexed(boolean indexed) {
            this.indexed = indexed;
            return this;
        }

        public Builder stored(boolean stored) {
            this.stored = stored;
            return this;
        }

        public Builder numericKind(LongColumn.NumericKind numericKind) {
            this.numericKind = numericKind;
            return this;
        }

        /**
         * Selects the field type and returns the immutable {@link Spec}.
         *
         * <p>When {@link #indexType} is set, the field type is chosen from the combination of
         * {@link IndexType#hasDocValuesSkipper()}, {@code indexed}, and {@code stored}, covering
         * the full DV (and optionally BKD) range. When {@link #indexType} is absent, a
         * stored-only field type (no doc values, no BKD) is selected from {@link LongColumn.NumericKind},
         * and {@code stored} must be {@code true}.
         */
        public Spec build() {
            Objects.requireNonNull(name, "name");
            final IndexableFieldType fieldType;
            if (indexType == null) {
                assert stored : "builder without indexType must have stored(true)";
                fieldType = switch (numericKind) {
                    case LONG -> LONG_STORED_ONLY_FIELD_TYPE;
                    case INT -> INT_STORED_ONLY_FIELD_TYPE;
                    case FLOAT -> FLOAT_STORED_ONLY_FIELD_TYPE;
                    case DOUBLE -> DOUBLE_STORED_ONLY_FIELD_TYPE;
                };
            } else if (indexType.hasDocValuesSkipper()) {
                fieldType = stored ? SORTED_NUMERIC_DV_INDEXED_FIELD_TYPE_STORED : SORTED_NUMERIC_DV_INDEXED_FIELD_TYPE;
            } else if (indexed) {
                IndexableFieldType base = switch (numericKind) {
                    case LONG -> LONG_INDEXED_FIELD_TYPE;
                    case INT -> INT_INDEXED_FIELD_TYPE;
                    case FLOAT -> FLOAT_INDEXED_FIELD_TYPE;
                    case DOUBLE -> DOUBLE_INDEXED_FIELD_TYPE;
                };
                fieldType = stored ? storedVariant(base) : base;
            } else {
                fieldType = stored ? SORTED_NUMERIC_DV_FIELD_TYPE_STORED : SORTED_NUMERIC_DV_FIELD_TYPE;
            }
            return new Spec(name, fieldType, numericKind);
        }
    }

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
     * Creates a {@link LuceneLongColumn} for a {@link MultiValuedBinaryDocValuesField.SeparateCount}
     * companion {@code .counts} column. The column name is {@code name + COUNT_FIELD_SUFFIX}, the
     * field type is {@link MultiValuedBinaryDocValuesField.SeparateCount#COUNT_FIELD_TYPE}, and the
     * numeric kind is {@link LongColumn.NumericKind#LONG}.
     */
    public static LuceneLongColumn counts(EscfColumnData data, String name) {
        return of(
            data,
            name + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX,
            MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_TYPE,
            LongColumn.NumericKind.LONG
        );
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
        public Number numericValue() {
            final long raw = (Long) fieldsData;
            // For stored-only fields (no doc values), the indexing chain calls numericValue().floatValue()
            // or .doubleValue() to retrieve the actual value. Return the correctly-typed Number so that
            // the stored representation matches what StoredField would produce.
            if (fieldType().stored() && fieldType().docValuesType() == DocValuesType.NONE) {
                return switch (kind) {
                    case LONG -> raw;
                    case INT -> (int) raw;
                    case FLOAT -> NumericUtils.sortableIntToFloat((int) raw);
                    case DOUBLE -> NumericUtils.sortableLongToDouble(raw);
                };
            }
            return raw;
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
