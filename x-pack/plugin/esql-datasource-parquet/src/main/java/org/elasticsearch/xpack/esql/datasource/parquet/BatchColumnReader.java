/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnBlockConversions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.BitSet;

/**
 * Batch-oriented column reader that produces ESQL {@link Block}s from a Parquet {@link ColumnReader}.
 *
 * <p>Replaces the row-at-a-time decode pattern (check def level → read value → consume → repeat)
 * with tight loops that separate the non-nullable fast path from the nullable path:
 * <ul>
 *   <li><b>Non-nullable (maxDef == 0)</b>: no def-level check per row; tight loop reads values
 *       directly into the output array. JIT can auto-vectorize these loops for PLAIN encoding.</li>
 *   <li><b>Nullable (maxDef &gt; 0)</b>: builds a {@link BitSet} of null positions directly
 *       (no intermediate {@code boolean[]} + conversion), reads only non-null values.</li>
 * </ul>
 *
 * <p>List columns (maxRepLevel &gt; 0) and unsupported types return {@code null} from
 * {@link #readBatch}, signaling the caller to fall back to the per-row path in
 * {@link OptimizedParquetColumnIterator}. All flat column types including DECIMAL,
 * FLOAT16, UUID, and INT96 are handled here.
 */
final class BatchColumnReader {

    private static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();
    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final int JULIAN_EPOCH_OFFSET = 2_440_588;

    private BatchColumnReader() {}

    /**
     * Reads a batch of rows from the column reader into an ESQL Block.
     * Returns null if this column/type combination is not supported by batch reading,
     * signaling the caller to fall back to the row-at-a-time path.
     */
    static Block readBatch(ColumnReader cr, ColumnInfo info, int rows, BlockFactory blockFactory) {
        if (info.maxRepLevel() > 0) {
            return null;
        }
        return switch (info.esqlType()) {
            case BOOLEAN -> readBooleanBatch(cr, info.maxDefLevel(), rows, blockFactory);
            case INTEGER -> readIntBatch(cr, info.maxDefLevel(), rows, blockFactory);
            case LONG -> {
                if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) {
                    yield readInt32AsLongBatch(cr, info.maxDefLevel(), rows, blockFactory);
                }
                yield readLongBatch(cr, info.maxDefLevel(), rows, blockFactory);
            }
            case DOUBLE -> readDoubleBatch(cr, info, rows, blockFactory);
            case KEYWORD, TEXT -> readBytesBatch(cr, info, rows, blockFactory);
            case DATETIME -> readDatetimeBatch(cr, info, rows, blockFactory);
            default -> null;
        };
    }

    // --- Boolean ---

    private static Block readBooleanBatch(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        boolean[] values = new boolean[rows];
        if (maxDef == 0) {
            for (int i = 0; i < rows; i++) {
                values[i] = cr.getBoolean();
                cr.consume();
            }
            Block constant = ConstantBlockDetection.tryConstantBoolean(values, rows, blockFactory);
            return constant != null ? constant : blockFactory.newBooleanArrayVector(values, rows).asBlock();
        }
        BitSet nulls = new BitSet(rows);
        boolean hasNulls = false;
        for (int i = 0; i < rows; i++) {
            if (cr.getCurrentDefinitionLevel() < maxDef) {
                nulls.set(i);
                hasNulls = true;
            } else {
                values[i] = cr.getBoolean();
            }
            cr.consume();
        }
        if (hasNulls) {
            Block allNull = ConstantBlockDetection.tryAllNull(nulls, rows, blockFactory);
            if (allNull != null) {
                return allNull;
            }
            return blockFactory.newBooleanArrayBlock(values, rows, null, nulls, Block.MvOrdering.UNORDERED);
        }
        Block constant = ConstantBlockDetection.tryConstantBoolean(values, rows, blockFactory);
        return constant != null ? constant : blockFactory.newBooleanArrayVector(values, rows).asBlock();
    }

    // --- Int ---

    private static Block readIntBatch(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        int[] values = new int[rows];
        if (maxDef == 0) {
            for (int i = 0; i < rows; i++) {
                values[i] = cr.getInteger();
                cr.consume();
            }
            Block constant = ConstantBlockDetection.tryConstantInt(values, rows, blockFactory);
            return constant != null ? constant : blockFactory.newIntArrayVector(values, rows).asBlock();
        }
        BitSet nulls = new BitSet(rows);
        boolean hasNulls = false;
        for (int i = 0; i < rows; i++) {
            if (cr.getCurrentDefinitionLevel() < maxDef) {
                nulls.set(i);
                hasNulls = true;
            } else {
                values[i] = cr.getInteger();
            }
            cr.consume();
        }
        if (hasNulls) {
            Block allNull = ConstantBlockDetection.tryAllNull(nulls, rows, blockFactory);
            if (allNull != null) {
                return allNull;
            }
            return blockFactory.newIntArrayBlock(values, rows, null, nulls, Block.MvOrdering.UNORDERED);
        }
        Block constant = ConstantBlockDetection.tryConstantInt(values, rows, blockFactory);
        return constant != null ? constant : blockFactory.newIntArrayVector(values, rows).asBlock();
    }

    // --- Long ---

    private static Block readLongBatch(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        long[] values = new long[rows];
        if (maxDef == 0) {
            for (int i = 0; i < rows; i++) {
                values[i] = cr.getLong();
                cr.consume();
            }
            Block constant = ConstantBlockDetection.tryConstantLong(values, rows, blockFactory);
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, rows, true, false, null);
        }
        BitSet nulls = new BitSet(rows);
        boolean hasNulls = false;
        for (int i = 0; i < rows; i++) {
            if (cr.getCurrentDefinitionLevel() < maxDef) {
                nulls.set(i);
                hasNulls = true;
            } else {
                values[i] = cr.getLong();
            }
            cr.consume();
        }
        if (hasNulls) {
            Block allNull = ConstantBlockDetection.tryAllNull(nulls, rows, blockFactory);
            if (allNull != null) {
                return allNull;
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, false, false, toBooleanArray(nulls, rows));
        }
        Block constant = ConstantBlockDetection.tryConstantLong(values, rows, blockFactory);
        return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, rows, true, false, null);
    }

    private static Block readInt32AsLongBatch(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        long[] values = new long[rows];
        if (maxDef == 0) {
            for (int i = 0; i < rows; i++) {
                values[i] = cr.getInteger();
                cr.consume();
            }
            Block constant = ConstantBlockDetection.tryConstantLong(values, rows, blockFactory);
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, rows, true, false, null);
        }
        BitSet nulls = new BitSet(rows);
        boolean hasNulls = false;
        for (int i = 0; i < rows; i++) {
            if (cr.getCurrentDefinitionLevel() < maxDef) {
                nulls.set(i);
                hasNulls = true;
            } else {
                values[i] = cr.getInteger();
            }
            cr.consume();
        }
        if (hasNulls) {
            Block allNull = ConstantBlockDetection.tryAllNull(nulls, rows, blockFactory);
            if (allNull != null) {
                return allNull;
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, false, false, toBooleanArray(nulls, rows));
        }
        Block constant = ConstantBlockDetection.tryConstantLong(values, rows, blockFactory);
        return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, rows, true, false, null);
    }

    // --- Double ---

    private static Block readDoubleBatch(ColumnReader cr, ColumnInfo info, int rows, BlockFactory blockFactory) {
        LogicalTypeAnnotation logical = info.logicalType();
        if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return readDecimalAsDoubleBatch(cr, info, decimal.getScale(), rows, blockFactory);
        }
        if (logical instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
            return readFloat16Batch(cr, info.maxDefLevel(), rows, blockFactory);
        }
        int maxDef = info.maxDefLevel();
        boolean isFloat = info.parquetType() == PrimitiveType.PrimitiveTypeName.FLOAT;
        double[] values = new double[rows];
        if (maxDef == 0) {
            if (isFloat) {
                for (int i = 0; i < rows; i++) {
                    values[i] = cr.getFloat();
                    cr.consume();
                }
            } else {
                for (int i = 0; i < rows; i++) {
                    values[i] = cr.getDouble();
                    cr.consume();
                }
            }
            Block constant = ConstantBlockDetection.tryConstantDouble(values, rows, blockFactory);
            return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, rows, true, false, null);
        }
        BitSet nulls = new BitSet(rows);
        boolean hasNulls = false;
        for (int i = 0; i < rows; i++) {
            if (cr.getCurrentDefinitionLevel() < maxDef) {
                nulls.set(i);
                hasNulls = true;
            } else {
                values[i] = isFloat ? cr.getFloat() : cr.getDouble();
            }
            cr.consume();
        }
        if (hasNulls) {
            Block allNull = ConstantBlockDetection.tryAllNull(nulls, rows, blockFactory);
            if (allNull != null) {
                return allNull;
            }
            return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, false, false, toBooleanArray(nulls, rows));
        }
        Block constant = ConstantBlockDetection.tryConstantDouble(values, rows, blockFactory);
        return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, rows, true, false, null);
    }

    private static Block readDecimalAsDoubleBatch(ColumnReader cr, ColumnInfo info, int scale, int rows, BlockFactory blockFactory) {
        int maxDef = info.maxDefLevel();
        double[] values = new double[rows];
        if (maxDef == 0) {
            for (int i = 0; i < rows; i++) {
                values[i] = decimalToDouble(cr, info.parquetType(), scale);
                cr.consume();
            }
            Block constant = ConstantBlockDetection.tryConstantDouble(values, rows, blockFactory);
            return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, rows, true, false, null);
        }
        BitSet nulls = new BitSet(rows);
        boolean hasNulls = false;
        for (int i = 0; i < rows; i++) {
            if (cr.getCurrentDefinitionLevel() < maxDef) {
                nulls.set(i);
                hasNulls = true;
            } else {
                values[i] = decimalToDouble(cr, info.parquetType(), scale);
            }
            cr.consume();
        }
        if (hasNulls) {
            Block allNull = ConstantBlockDetection.tryAllNull(nulls, rows, blockFactory);
            if (allNull != null) {
                return allNull;
            }
            return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, false, false, toBooleanArray(nulls, rows));
        }
        Block constant = ConstantBlockDetection.tryConstantDouble(values, rows, blockFactory);
        return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, rows, true, false, null);
    }

    private static double decimalToDouble(ColumnReader cr, PrimitiveType.PrimitiveTypeName parquetType, int scale) {
        BigInteger unscaled = switch (parquetType) {
            case INT32 -> BigInteger.valueOf(cr.getInteger());
            case INT64 -> BigInteger.valueOf(cr.getLong());
            case BINARY, FIXED_LEN_BYTE_ARRAY -> new BigInteger(cr.getBinary().getBytes());
            default -> throw new QlIllegalArgumentException("Unexpected DECIMAL backing type: " + parquetType);
        };
        return new BigDecimal(unscaled, scale).doubleValue();
    }

    private static Block readFloat16Batch(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        double[] values = new double[rows];
        if (maxDef == 0) {
            for (int i = 0; i < rows; i++) {
                values[i] = decodeFloat16(cr.getBinary().getBytes());
                cr.consume();
            }
            Block constant = ConstantBlockDetection.tryConstantDouble(values, rows, blockFactory);
            return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, rows, true, false, null);
        }
        BitSet nulls = new BitSet(rows);
        boolean hasNulls = false;
        for (int i = 0; i < rows; i++) {
            if (cr.getCurrentDefinitionLevel() < maxDef) {
                nulls.set(i);
                hasNulls = true;
            } else {
                values[i] = decodeFloat16(cr.getBinary().getBytes());
            }
            cr.consume();
        }
        if (hasNulls) {
            Block allNull = ConstantBlockDetection.tryAllNull(nulls, rows, blockFactory);
            if (allNull != null) {
                return allNull;
            }
            return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, false, false, toBooleanArray(nulls, rows));
        }
        Block constant = ConstantBlockDetection.tryConstantDouble(values, rows, blockFactory);
        return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, rows, true, false, null);
    }

    private static double decodeFloat16(byte[] bytes) {
        short float16Bits = (short) ((bytes[1] & 0xFF) << 8 | (bytes[0] & 0xFF));
        return Float.float16ToFloat(float16Bits);
    }

    // --- BytesRef (KEYWORD/TEXT) ---

    private static Block readBytesBatch(ColumnReader cr, ColumnInfo info, int rows, BlockFactory blockFactory) {
        boolean isUuid = info.logicalType() instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
        int maxDef = info.maxDefLevel();
        if (maxDef == 0) {
            BytesRef[] values = new BytesRef[rows];
            if (isUuid) {
                for (int i = 0; i < rows; i++) {
                    values[i] = new BytesRef(OptimizedParquetColumnIterator.formatUuid(cr.getBinary().getBytes()));
                    cr.consume();
                }
            } else {
                for (int i = 0; i < rows; i++) {
                    values[i] = new BytesRef(cr.getBinary().getBytes());
                    cr.consume();
                }
            }
            Block constant = ConstantBlockDetection.tryConstantBytesRef(values, rows, blockFactory);
            if (constant != null) {
                return constant;
            }
            try (var builder = blockFactory.newBytesRefBlockBuilder(rows)) {
                for (int i = 0; i < rows; i++) {
                    builder.appendBytesRef(values[i]);
                }
                return builder.build();
            }
        }
        BitSet nulls = new BitSet(rows);
        BytesRef[] values = new BytesRef[rows];
        boolean hasNulls = false;
        for (int i = 0; i < rows; i++) {
            if (cr.getCurrentDefinitionLevel() < maxDef) {
                nulls.set(i);
                hasNulls = true;
            } else if (isUuid) {
                values[i] = new BytesRef(OptimizedParquetColumnIterator.formatUuid(cr.getBinary().getBytes()));
            } else {
                values[i] = new BytesRef(cr.getBinary().getBytes());
            }
            cr.consume();
        }
        if (hasNulls) {
            Block allNull = ConstantBlockDetection.tryAllNull(nulls, rows, blockFactory);
            if (allNull != null) {
                return allNull;
            }
        }
        try (var builder = blockFactory.newBytesRefBlockBuilder(rows)) {
            for (int i = 0; i < rows; i++) {
                if (nulls.get(i)) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(values[i]);
                }
            }
            return builder.build();
        }
    }

    // --- Datetime ---

    private static Block readDatetimeBatch(ColumnReader cr, ColumnInfo info, int rows, BlockFactory blockFactory) {
        if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT96) {
            return readInt96Batch(cr, info.maxDefLevel(), rows, blockFactory);
        }
        int maxDef = info.maxDefLevel();
        boolean isDate = info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32;
        long[] values = new long[rows];
        if (maxDef == 0) {
            if (isDate) {
                for (int i = 0; i < rows; i++) {
                    values[i] = cr.getInteger() * MILLIS_PER_DAY;
                    cr.consume();
                }
            } else {
                LogicalTypeAnnotation logicalType = info.logicalType();
                for (int i = 0; i < rows; i++) {
                    values[i] = convertTimestampToMillis(cr.getLong(), logicalType);
                    cr.consume();
                }
            }
            Block constant = ConstantBlockDetection.tryConstantLong(values, rows, blockFactory);
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, rows, true, false, null);
        }
        BitSet nulls = new BitSet(rows);
        boolean hasNulls = false;
        LogicalTypeAnnotation logicalType = info.logicalType();
        for (int i = 0; i < rows; i++) {
            if (cr.getCurrentDefinitionLevel() < maxDef) {
                nulls.set(i);
                hasNulls = true;
            } else if (isDate) {
                values[i] = cr.getInteger() * MILLIS_PER_DAY;
            } else {
                values[i] = convertTimestampToMillis(cr.getLong(), logicalType);
            }
            cr.consume();
        }
        if (hasNulls) {
            Block allNull = ConstantBlockDetection.tryAllNull(nulls, rows, blockFactory);
            if (allNull != null) {
                return allNull;
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, false, false, toBooleanArray(nulls, rows));
        }
        Block constant = ConstantBlockDetection.tryConstantLong(values, rows, blockFactory);
        return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, rows, true, false, null);
    }

    private static long convertTimestampToMillis(long raw, LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            return switch (ts.getUnit()) {
                case MILLIS -> raw;
                case MICROS -> raw / 1_000;
                case NANOS -> raw / 1_000_000;
            };
        }
        return raw;
    }

    private static Block readInt96Batch(ColumnReader cr, int maxDef, int rows, BlockFactory blockFactory) {
        long[] values = new long[rows];
        if (maxDef == 0) {
            for (int i = 0; i < rows; i++) {
                values[i] = decodeInt96Timestamp(cr.getBinary());
                cr.consume();
            }
            Block constant = ConstantBlockDetection.tryConstantLong(values, rows, blockFactory);
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, rows, true, false, null);
        }
        BitSet nulls = new BitSet(rows);
        boolean hasNulls = false;
        for (int i = 0; i < rows; i++) {
            if (cr.getCurrentDefinitionLevel() < maxDef) {
                nulls.set(i);
                hasNulls = true;
            } else {
                values[i] = decodeInt96Timestamp(cr.getBinary());
            }
            cr.consume();
        }
        if (hasNulls) {
            Block allNull = ConstantBlockDetection.tryAllNull(nulls, rows, blockFactory);
            if (allNull != null) {
                return allNull;
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, false, false, toBooleanArray(nulls, rows));
        }
        Block constant = ConstantBlockDetection.tryConstantLong(values, rows, blockFactory);
        return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, rows, true, false, null);
    }

    private static long decodeInt96Timestamp(Binary bin) {
        ByteBuffer buf = ByteBuffer.wrap(bin.getBytes()).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = buf.getLong();
        int julianDay = buf.getInt();
        long epochDay = julianDay - JULIAN_EPOCH_OFFSET;
        return epochDay * MILLIS_PER_DAY + nanosOfDay / NANOS_PER_MILLI;
    }

    // --- Utilities ---

    /**
     * Converts a BitSet of null positions to a boolean[] as required by ColumnBlockConversions.
     */
    private static boolean[] toBooleanArray(BitSet nulls, int length) {
        boolean[] result = new boolean[length];
        for (int i = nulls.nextSetBit(0); i >= 0; i = nulls.nextSetBit(i + 1)) {
            result[i] = true;
        }
        return result;
    }
}
