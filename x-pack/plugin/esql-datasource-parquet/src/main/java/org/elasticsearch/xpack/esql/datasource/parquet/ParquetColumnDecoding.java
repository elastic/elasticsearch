/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.BitSet;

/**
 * Shared Parquet decode helpers used by both the baseline {@code ParquetColumnIterator}
 * and {@link OptimizedParquetColumnIterator}. Centralises list-column decoding,
 * timestamp conversion, UUID formatting, and other utilities so that bug fixes in one
 * decode path are automatically reflected in the other.
 */
final class ParquetColumnDecoding {

    private ParquetColumnDecoding() {}

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    // ---- Timestamp helpers ----

    static long convertTimestampToMillis(long raw, LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return switch (ts.getUnit()) {
                case MILLIS -> raw;
                case MICROS -> raw / 1_000;
                case NANOS -> raw / 1_000_000;
            };
        }
        return raw;
    }

    // ---- UUID formatting ----

    /**
     * Formats a 16-byte UUID in big-endian layout as the standard 8-4-4-4-12 hex string.
     * Requires exactly 16 bytes; shorter or longer arrays indicate an upstream bug since
     * Parquet {@code FIXED_LEN_BYTE_ARRAY(16)} is always exactly 16 bytes.
     */
    static String formatUuid(byte[] bytes) {
        if (bytes == null || bytes.length != 16) {
            throw new QlIllegalArgumentException("UUID requires exactly 16 bytes, got " + (bytes == null ? "null" : bytes.length));
        }
        StringBuilder sb = new StringBuilder(36);
        for (int i = 0; i < 16; i++) {
            sb.append(HEX[(bytes[i] >> 4) & 0xF]);
            sb.append(HEX[bytes[i] & 0xF]);
            if (i == 3 || i == 5 || i == 7 || i == 9) {
                sb.append('-');
            }
        }
        return sb.toString();
    }

    // ---- Skip helpers ----

    static void skipValues(ColumnReader cr, int rows) {
        for (int i = 0; i < rows; i++) {
            cr.consume();
        }
    }

    /**
     * Skips all Parquet values for the given number of rows in a LIST column,
     * respecting repetition levels to consume entire lists per row.
     */
    static void skipListValues(ColumnReader cr, int rows) {
        for (int row = 0; row < rows; row++) {
            cr.consume();
            while (cr.getCurrentRepetitionLevel() > 0) {
                cr.consume();
            }
        }
    }

    // ---- Null bitmap helper ----

    static BitSet toBitSet(boolean[] isNull, int length) {
        BitSet bits = new BitSet(length);
        for (int i = 0; i < length; i++) {
            if (isNull[i]) {
                bits.set(i);
            }
        }
        return bits;
    }

    // ---- List column reading ----

    /**
     * Reads a LIST column using repetition levels to determine list boundaries,
     * producing multi-valued ESQL blocks. Dispatches to the appropriate typed reader
     * based on the ESQL element type. Handles null lists, empty lists, and null
     * elements within lists correctly. Unsupported types are skipped and returned
     * as a constant null block.
     */
    static Block readListColumn(ColumnReader cr, ColumnInfo info, int rows, BlockFactory blockFactory) {
        DataType elementType = info.esqlType();
        int maxDef = info.maxDefLevel();
        return switch (elementType) {
            case INTEGER -> readListIntColumn(cr, maxDef, rows, blockFactory);
            case LONG -> readListLongColumn(cr, maxDef, rows, blockFactory);
            case DOUBLE -> readListDoubleColumn(cr, maxDef, rows, blockFactory);
            case BOOLEAN -> readListBooleanColumn(cr, maxDef, rows, blockFactory);
            case KEYWORD, TEXT -> readListBytesRefColumn(cr, maxDef, rows, blockFactory);
            case DATETIME -> readListDatetimeColumn(cr, info, rows, blockFactory);
            default -> {
                skipListValues(cr, rows);
                yield blockFactory.newConstantNullBlock(rows);
            }
        };
    }

    /**
     * Reads a single list row from the Parquet column reader, handling repetition and
     * definition levels to produce multi-valued ESQL block entries. The supplied
     * {@link Runnable} is invoked once per element to append the current value from
     * the column reader into the block builder.
     *
     * <p>The caller must have positioned the column reader at the start of the row.
     * After this method returns, the reader is positioned at the start of the next row
     * (repetition level == 0).
     */
    private static void readListRow(ColumnReader cr, int maxDef, Block.Builder builder, Runnable appender) {
        int def = cr.getCurrentDefinitionLevel();
        if (def >= maxDef) {
            builder.beginPositionEntry();
            appender.run();
            cr.consume();
            while (cr.getCurrentRepetitionLevel() > 0) {
                if (cr.getCurrentDefinitionLevel() >= maxDef) {
                    appender.run();
                }
                cr.consume();
            }
            builder.endPositionEntry();
        } else {
            cr.consume();
            boolean hasValues = false;
            while (cr.getCurrentRepetitionLevel() > 0) {
                if (cr.getCurrentDefinitionLevel() >= maxDef) {
                    if (hasValues == false) {
                        builder.beginPositionEntry();
                        hasValues = true;
                    }
                    appender.run();
                }
                cr.consume();
            }
            if (hasValues) {
                builder.endPositionEntry();
            } else {
                builder.appendNull();
            }
        }
    }

    private static Block readListIntColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newIntBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendInt(cr.getInteger());
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListLongColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendLong(cr.getLong());
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListDoubleColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newDoubleBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendDouble(cr.getDouble());
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListBooleanColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newBooleanBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendBoolean(cr.getBoolean());
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListBytesRefColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newBytesRefBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendBytesRef(new BytesRef(cr.getBinary().getBytes()));
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListDatetimeColumn(ColumnReader cr, ColumnInfo info, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            int maxDef = info.maxDefLevel();
            Runnable appender = () -> builder.appendLong(convertTimestampToMillis(cr.getLong(), info.logicalType()));
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    // ---- NoOp converters for ColumnReadStoreImpl ----

    /**
     * Minimal GroupConverter that satisfies
     * {@link org.apache.parquet.column.impl.ColumnReadStoreImpl}'s constructor.
     * We never call {@code writeCurrentValueToConverter()}, so all converters are no-ops.
     */
    static final class NoOpGroupConverter extends GroupConverter {
        private final GroupType schema;

        NoOpGroupConverter(GroupType schema) {
            this.schema = schema;
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            Type field = schema.getType(fieldIndex);
            return field.isPrimitive() ? new NoOpPrimitiveConverter() : new NoOpGroupConverter(field.asGroupType());
        }

        @Override
        public void start() {}

        @Override
        public void end() {}
    }

    private static final class NoOpPrimitiveConverter extends PrimitiveConverter {}
}
