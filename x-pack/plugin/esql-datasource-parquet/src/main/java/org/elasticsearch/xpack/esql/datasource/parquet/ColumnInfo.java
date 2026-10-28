/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * Per-column metadata used by the Parquet column iterators. Holds the Parquet column descriptor
 * alongside the ESQL type mapping and definition/repetition levels.
 * <p>
 * {@code esqlType} is the type the decode paths PRODUCE — the planner/declared attribute type,
 * not necessarily the file's own — with {@code parquetType}/{@code logicalType} carrying the
 * physical side of the pair and {@code fileEsqlType} its ESQL rendering (the element type for
 * LIST columns; see {@code DeclaredTypeCoercions} for which pairs coerce and how). When
 * {@code esqlType} differs from {@code fileEsqlType} and the pair is not fused into a decode
 * loop, the decode paths read the column {@link #fileTyped() at the file's own type} and coerce
 * the resulting block. {@code dateFormatter} carries the column's declared date parse pattern
 * for the string&rarr;datetime pair; {@code null} means the ISO default.
 * <p>
 * The synthetic {@code _rowPosition} column has no Parquet descriptor (it is materialised by the
 * iterator from the file's footer + per-row position bookkeeping); use {@link #rowPosition()} to
 * obtain the sentinel and {@link #isRowPosition()} to detect it in emit code paths.
 */
record ColumnInfo(
    ColumnDescriptor descriptor,
    PrimitiveType.PrimitiveTypeName parquetType,
    DataType esqlType,
    int maxDefLevel,
    int maxRepLevel,
    LogicalTypeAnnotation logicalType,
    @Nullable DateFormatter dateFormatter,
    @Nullable DataType fileEsqlType
) {
    /**
     * Convenience constructor for columns read at the file's own type (no declared coercion):
     * {@code fileEsqlType == esqlType}.
     */
    ColumnInfo(
        ColumnDescriptor descriptor,
        PrimitiveType.PrimitiveTypeName parquetType,
        DataType esqlType,
        int maxDefLevel,
        int maxRepLevel,
        LogicalTypeAnnotation logicalType,
        @Nullable DateFormatter dateFormatter
    ) {
        this(descriptor, parquetType, esqlType, maxDefLevel, maxRepLevel, logicalType, dateFormatter, esqlType);
    }

    /** Formatter-free convenience constructor for the (dominant) columns with no declared date format. */
    ColumnInfo(
        ColumnDescriptor descriptor,
        PrimitiveType.PrimitiveTypeName parquetType,
        DataType esqlType,
        int maxDefLevel,
        int maxRepLevel,
        LogicalTypeAnnotation logicalType
    ) {
        this(descriptor, parquetType, esqlType, maxDefLevel, maxRepLevel, logicalType, null);
    }

    /**
     * A view of this column that decodes at the file's own type ({@code esqlType == fileEsqlType}):
     * the physical half of the coerce-after-decode split — decode paths read this view natively,
     * then {@code DeclaredTypeCoercions.castBlock} coerces the block to the declared
     * {@link #esqlType}. Returns {@code this} when no retype is in play.
     */
    ColumnInfo fileTyped() {
        if (fileEsqlType == null || fileEsqlType == esqlType) {
            return this;
        }
        return new ColumnInfo(descriptor, parquetType, fileEsqlType, maxDefLevel, maxRepLevel, logicalType, dateFormatter, fileEsqlType);
    }

    /** Sentinel marker for the synthetic {@code _rowPosition} column slot. */
    private static final ColumnInfo ROW_POSITION = new ColumnInfo(null, null, DataType.LONG, 0, 0, null);

    /** Returns the {@code _rowPosition} sentinel; identity-comparable in iterator emit paths. */
    static ColumnInfo rowPosition() {
        return ROW_POSITION;
    }

    /** Whether this slot represents the synthetic {@code _rowPosition} column. */
    boolean isRowPosition() {
        return this == ROW_POSITION;
    }
}
