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
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;

/**
 * Shared Parquet decode helpers used by both the baseline {@code ParquetColumnIterator}
 * and {@link OptimizedParquetColumnIterator}. Centralises list-column decoding,
 * timestamp conversion, UUID formatting, {@code unsigned_long} sign-flip encoding, and other utilities so that bug
 * fixes in one decode path are automatically reflected in the other.
 */
final class ParquetColumnDecoding {

    private ParquetColumnDecoding() {}

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    // ---- Temporal constants ----

    static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();
    static final long NANOS_PER_MILLI = 1_000_000L;
    /** Julian day number for Unix epoch (1970-01-01). */
    static final int JULIAN_EPOCH_OFFSET = 2_440_588;

    // ---- Temporal helpers ----

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

    /** Converts a date32 value (days since epoch) to epoch milliseconds. */
    static long dateDaysToMillis(long days) {
        return days * MILLIS_PER_DAY;
    }

    /**
     * Converts a Parquet INT96 value (12 bytes LE: 8 bytes nanos-of-day + 4 bytes Julian day)
     * to epoch milliseconds. The bytes are read starting at {@code offset} for {@code length}
     * bytes (must be 12).
     */
    static long int96ToEpochMillis(byte[] bytes, int offset, int length) {
        if (length != 12) {
            throw new IllegalArgumentException("INT96 requires exactly 12 bytes, got " + length);
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes, offset, length).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = buf.getLong();
        int julianDay = buf.getInt();
        long epochDay = julianDay - JULIAN_EPOCH_OFFSET;
        return epochDay * MILLIS_PER_DAY + nanosOfDay / NANOS_PER_MILLI;
    }

    /**
     * Decodes a Parquet footer stat value into the same representation the scan path produces, so
     * pushed-down MIN/MAX match a scan:
     * <ul>
     *   <li>date32 -&gt; epoch-millis</li>
     *   <li>timestamp -&gt; epoch-millis (unit-scaled)</li>
     *   <li>time -&gt; raw milliseconds for TIME_MILLIS (physical INT32), nanoseconds otherwise</li>
     * </ul>
     * Returns {@code null} when the type is not one of the above (caller falls through to other
     * normalization). INT96 is deliberately excluded: its footer min/max are compared by parquet-mr
     * as unsigned little-endian bytes (nanos-of-day in the low bytes), so they are not chronological
     * and cannot be trusted for MIN/MAX pushdown — returning {@code null} forces a scan instead.
     */
    static Long decodeTemporalStat(Object value, PrimitiveType type) {
        LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
        if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return dateDaysToMillis(((Number) value).longValue());
        }
        if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            return convertTimestampToMillis(((Number) value).longValue(), logical);
        }
        if (logical instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            long raw = ((Number) value).longValue();
            // Mirror the scan path: TIME_MILLIS (physical INT32) stays raw ms; TIME_MICROS/NANOS
            // scale to nanoseconds via timeNanoMultiplier. Signed comparison of these physical
            // values is chronological, so footer min/max ordering is preserved.
            return type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32 ? raw : raw * timeNanoMultiplier(time);
        }
        return null;
    }

    /**
     * Returns the multiplier needed to convert a Parquet TIME_* value to nanoseconds.
     * TIME_MICROS values are stored as microseconds and must be multiplied by 1_000;
     * TIME_MILLIS and TIME_NANOS are stored in their final unit (ms handled as INTEGER, ns as-is).
     */
    static long timeNanoMultiplier(LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
        return time.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS ? 1_000L : 1L;
    }

    // ---- Unsigned long encoding ----

    /**
     * Sign-flip-encodes a raw {@code unsigned_long} value ({@code value ^ 2^63}) into ESQL's sortable signed
     * representation, mirroring the indexing path. ESQL stores {@code unsigned_long} inside a signed {@code LongBlock}
     * in this form so signed-long ordering matches unsigned ordering, and every value-output surface decodes it back on
     * the way out. Every Parquet read producer of an {@code unsigned_long} block must therefore route its INT64 values
     * through this method. Shared so the baseline, optimized, and list read paths cannot drift.
     */
    static long encodeUnsignedLong(long value) {
        return NumericUtils.asLongUnsigned(value);
    }

    /**
     * Applies {@link #encodeUnsignedLong(long)} to the first {@code count} values in place. Null slots within the range
     * hold undefined bits but are masked out by the caller, so encoding them is harmless.
     */
    static void encodeUnsignedLongInPlace(long[] values, int count) {
        for (int i = 0; i < count; i++) {
            values[i] = encodeUnsignedLong(values[i]);
        }
    }

    // ---- UUID formatting ----

    /**
     * Formats a 16-byte UUID in big-endian layout as the standard 8-4-4-4-12 hex string.
     * Requires exactly 16 bytes; shorter or longer arrays indicate an upstream bug since
     * Parquet {@code FIXED_LEN_BYTE_ARRAY(16)} is always exactly 16 bytes.
     */
    static String formatUuid(byte[] bytes) {
        return formatUuid(bytes, 0, bytes == null ? 0 : bytes.length);
    }

    /**
     * Formats a 16-byte UUID in big-endian layout as the standard 8-4-4-4-12 hex string.
     * The UUID bytes start at {@code offset} in the given array.
     */
    static String formatUuid(byte[] bytes, int offset, int length) {
        if (bytes == null || length != 16) {
            throw new IllegalArgumentException("UUID requires exactly 16 bytes, got " + (bytes == null ? "null" : length));
        }
        if (offset < 0 || offset + 16 > bytes.length) {
            throw new IllegalArgumentException("UUID byte offset out of bounds: offset=" + offset + ", array length=" + bytes.length);
        }
        StringBuilder sb = new StringBuilder(36);
        for (int i = 0; i < 16; i++) {
            sb.append(HEX[(bytes[offset + i] >> 4) & 0xF]);
            sb.append(HEX[bytes[offset + i] & 0xF]);
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
            case LONG -> {
                if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) {
                    // TIME_MILLIS: physical INT32 widened to long (raw ms value, no unit conversion)
                    yield readListInt32AsLongColumn(cr, maxDef, rows, blockFactory);
                }
                long multiplier = info.logicalType() instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time
                    ? timeNanoMultiplier(time)
                    : 1L;
                yield readListLongColumn(cr, maxDef, rows, blockFactory, multiplier);
            }
            case UNSIGNED_LONG -> readListUnsignedLongColumn(cr, maxDef, rows, blockFactory);
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

    private static Block readListLongColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory, long multiplier) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            Runnable appender = multiplier == 1
                ? () -> builder.appendLong(cr.getLong())
                : () -> builder.appendLong(cr.getLong() * multiplier);
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    private static Block readListInt32AsLongColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendLong(cr.getInteger());
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, appender);
            }
            return builder.build();
        }
    }

    /**
     * Reads a LIST of {@code unsigned_long} (Parquet INT64 with {@code intType(64, false)}) into a {@code LongBlock}.
     * Each element is sign-flip-encoded ({@code value ^ 2^63}) on the way in, mirroring the scalar read path and the
     * indexing path, so the always-decoding output edge produces the true unsigned value.
     */
    private static Block readListUnsignedLongColumn(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            Runnable appender = () -> builder.appendLong(encodeUnsignedLong(cr.getLong()));
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
