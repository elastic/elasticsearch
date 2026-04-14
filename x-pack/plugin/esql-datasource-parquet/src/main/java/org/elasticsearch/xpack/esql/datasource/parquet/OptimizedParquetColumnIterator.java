/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnBlockConversions;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Optimized Parquet column iterator behind the {@code optimized_reader} feature flag.
 *
 * <p>Stage 0: functionally identical to the baseline {@code ParquetColumnIterator} but structured
 * for progressive enhancement. Accepts {@link PreloadedRowGroupMetadata} for future use by
 * dictionary pruning (Stage 1), page-level skipping (Stage 2), and batch decode (Stage 3).
 *
 * <p>The existing baseline {@code ParquetColumnIterator} is never modified — it remains as the
 * stable fallback when {@code optimized_reader=false}.
 */
final class OptimizedParquetColumnIterator implements CloseableIterator<Page> {

    private static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();
    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final int JULIAN_EPOCH_OFFSET = 2_440_588;
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final ParquetFileReader reader;
    private final MessageType projectedSchema;
    private final List<Attribute> attributes;
    private final int batchSize;
    private final BlockFactory blockFactory;
    private final String createdBy;
    private final String fileLocation;
    private final ColumnInfo[] columnInfos;
    private final PreloadedRowGroupMetadata preloadedMetadata;
    private int rowBudget;

    private PageReadStore rowGroup;
    private ColumnReader[] columnReaders;
    private long rowsRemainingInGroup;
    private boolean exhausted = false;
    private int rowGroupOrdinal = -1;
    private int pageBatchIndexInRowGroup = 0;

    OptimizedParquetColumnIterator(
        ParquetFileReader reader,
        MessageType projectedSchema,
        List<Attribute> attributes,
        int batchSize,
        BlockFactory blockFactory,
        int rowLimit,
        String createdBy,
        String fileLocation,
        ColumnInfo[] columnInfos,
        PreloadedRowGroupMetadata preloadedMetadata
    ) {
        this.reader = reader;
        this.projectedSchema = projectedSchema;
        this.attributes = attributes;
        this.batchSize = batchSize;
        this.blockFactory = blockFactory;
        this.rowBudget = rowLimit;
        this.createdBy = createdBy != null ? createdBy : "";
        this.fileLocation = fileLocation;
        this.columnInfos = columnInfos;
        this.preloadedMetadata = preloadedMetadata;

        reader.setRequestedSchema(projectedSchema);
    }

    @Override
    public boolean hasNext() {
        if (exhausted) {
            return false;
        }
        if (rowBudget != FormatReader.NO_LIMIT && rowBudget <= 0) {
            exhausted = true;
            return false;
        }
        if (rowsRemainingInGroup > 0) {
            return true;
        }
        try {
            return advanceRowGroup();
        } catch (IOException e) {
            throw new ElasticsearchException(
                "Failed to read Parquet row group [" + (rowGroupOrdinal + 1) + "] in file [" + fileLocation + "]: " + e.getMessage(),
                e
            );
        }
    }

    private boolean advanceRowGroup() throws IOException {
        if (rowGroup != null) {
            rowGroup.close();
            rowGroup = null;
        }
        rowGroup = reader.readNextFilteredRowGroup();
        if (rowGroup == null) {
            exhausted = true;
            return false;
        }
        rowGroupOrdinal++;
        pageBatchIndexInRowGroup = 0;
        rowsRemainingInGroup = rowGroup.getRowCount();
        ColumnReadStoreImpl store = new ColumnReadStoreImpl(rowGroup, new NoOpGroupConverter(projectedSchema), projectedSchema, createdBy);
        columnReaders = new ColumnReader[columnInfos.length];
        for (int i = 0; i < columnInfos.length; i++) {
            if (columnInfos[i] != null) {
                columnReaders[i] = store.getColumnReader(columnInfos[i].descriptor());
            }
        }
        return rowsRemainingInGroup > 0;
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        int effectiveBatch = batchSize;
        if (rowBudget != FormatReader.NO_LIMIT) {
            effectiveBatch = Math.min(effectiveBatch, rowBudget);
        }
        int rowsToRead = (int) Math.min(effectiveBatch, rowsRemainingInGroup);

        Block[] blocks = new Block[attributes.size()];
        try {
            for (int col = 0; col < columnInfos.length; col++) {
                ColumnInfo info = columnInfos[col];
                if (info == null) {
                    blocks[col] = blockFactory.newConstantNullBlock(rowsToRead);
                } else {
                    try {
                        blocks[col] = readColumnBlock(columnReaders[col], info, rowsToRead);
                    } catch (Exception e) {
                        Releasables.closeExpectNoException(blocks);
                        Attribute attr = attributes.get(col);
                        throw new ElasticsearchException(
                            "Failed to read Parquet column ["
                                + attr.name()
                                + "] (type "
                                + attr.dataType()
                                + ") at row group ["
                                + (rowGroupOrdinal + 1)
                                + "] page batch ["
                                + pageBatchIndexInRowGroup
                                + "] in file ["
                                + fileLocation
                                + "]: "
                                + e.getMessage(),
                            e
                        );
                    }
                }
            }
        } catch (ElasticsearchException e) {
            throw e;
        } catch (Exception e) {
            Releasables.closeExpectNoException(blocks);
            throw new ElasticsearchException(
                "Failed to create Page batch at row group ["
                    + (rowGroupOrdinal + 1)
                    + "] page batch ["
                    + pageBatchIndexInRowGroup
                    + "] in file ["
                    + fileLocation
                    + "]: "
                    + e.getMessage(),
                e
            );
        }

        pageBatchIndexInRowGroup++;
        rowsRemainingInGroup -= rowsToRead;
        if (rowBudget != FormatReader.NO_LIMIT) {
            rowBudget -= rowsToRead;
        }
        return new Page(blocks);
    }

    private Block readColumnBlock(ColumnReader cr, ColumnInfo info, int rowsToRead) {
        if (info.maxRepLevel() > 0) {
            return readListColumn(cr, info, rowsToRead);
        }
        return switch (info.esqlType()) {
            case BOOLEAN -> readBooleanColumn(cr, info.maxDefLevel(), rowsToRead);
            case INTEGER -> readIntColumn(cr, info.maxDefLevel(), rowsToRead);
            case LONG -> {
                if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) {
                    yield readInt32WidenedToLongColumn(cr, info.maxDefLevel(), rowsToRead);
                }
                yield readLongColumn(cr, info.maxDefLevel(), rowsToRead);
            }
            case DOUBLE -> readDoubleColumn(cr, info, rowsToRead);
            case KEYWORD, TEXT -> readBytesRefColumn(cr, info, rowsToRead);
            case DATETIME -> readDatetimeColumn(cr, info, rowsToRead);
            default -> {
                skipValues(cr, rowsToRead);
                yield blockFactory.newConstantNullBlock(rowsToRead);
            }
        };
    }

    // --- Primitive column readers (same logic as baseline) ---

    private Block readBooleanColumn(ColumnReader cr, int maxDef, int rows) {
        boolean[] values = new boolean[rows];
        boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
        boolean noNulls = true;
        for (int i = 0; i < rows; i++) {
            if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                isNull[i] = true;
                noNulls = false;
            } else {
                values[i] = cr.getBoolean();
            }
            cr.consume();
        }
        if (noNulls) {
            return blockFactory.newBooleanArrayVector(values, rows).asBlock();
        }
        return blockFactory.newBooleanArrayBlock(values, rows, null, toBitSet(isNull, rows), Block.MvOrdering.UNORDERED);
    }

    private Block readIntColumn(ColumnReader cr, int maxDef, int rows) {
        int[] values = new int[rows];
        boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
        boolean noNulls = true;
        for (int i = 0; i < rows; i++) {
            if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                isNull[i] = true;
                noNulls = false;
            } else {
                values[i] = cr.getInteger();
            }
            cr.consume();
        }
        if (noNulls) {
            return blockFactory.newIntArrayVector(values, rows).asBlock();
        }
        return blockFactory.newIntArrayBlock(values, rows, null, toBitSet(isNull, rows), Block.MvOrdering.UNORDERED);
    }

    private Block readInt32WidenedToLongColumn(ColumnReader cr, int maxDef, int rows) {
        long[] values = new long[rows];
        boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
        boolean noNulls = true;
        for (int i = 0; i < rows; i++) {
            if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                isNull[i] = true;
                noNulls = false;
            } else {
                values[i] = cr.getInteger();
            }
            cr.consume();
        }
        return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull);
    }

    private Block readLongColumn(ColumnReader cr, int maxDef, int rows) {
        long[] values = new long[rows];
        boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
        boolean noNulls = true;
        for (int i = 0; i < rows; i++) {
            if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                isNull[i] = true;
                noNulls = false;
            } else {
                values[i] = cr.getLong();
            }
            cr.consume();
        }
        return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull);
    }

    private Block readDoubleColumn(ColumnReader cr, ColumnInfo info, int rows) {
        LogicalTypeAnnotation logical = info.logicalType();
        if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return readDecimalAsDoubleColumn(cr, info, decimal.getScale(), rows);
        }
        if (logical instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
            return readFloat16Column(cr, info.maxDefLevel(), rows);
        }
        double[] values = new double[rows];
        boolean[] isNull = info.maxDefLevel() > 0 ? new boolean[rows] : null;
        boolean noNulls = true;
        boolean isFloat = info.parquetType() == PrimitiveType.PrimitiveTypeName.FLOAT;
        for (int i = 0; i < rows; i++) {
            if (info.maxDefLevel() > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel()) {
                isNull[i] = true;
                noNulls = false;
            } else {
                values[i] = isFloat ? cr.getFloat() : cr.getDouble();
            }
            cr.consume();
        }
        return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, noNulls, false, isNull);
    }

    private Block readDecimalAsDoubleColumn(ColumnReader cr, ColumnInfo info, int scale, int rows) {
        double[] values = new double[rows];
        boolean[] isNull = info.maxDefLevel() > 0 ? new boolean[rows] : null;
        boolean noNulls = true;
        for (int i = 0; i < rows; i++) {
            if (info.maxDefLevel() > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel()) {
                isNull[i] = true;
                noNulls = false;
            } else {
                BigInteger unscaled = switch (info.parquetType()) {
                    case INT32 -> BigInteger.valueOf(cr.getInteger());
                    case INT64 -> BigInteger.valueOf(cr.getLong());
                    case BINARY, FIXED_LEN_BYTE_ARRAY -> new BigInteger(cr.getBinary().getBytes());
                    default -> throw new QlIllegalArgumentException("Unexpected DECIMAL backing type: " + info.parquetType());
                };
                values[i] = new java.math.BigDecimal(unscaled, scale).doubleValue();
            }
            cr.consume();
        }
        return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, noNulls, false, isNull);
    }

    private Block readFloat16Column(ColumnReader cr, int maxDef, int rows) {
        double[] values = new double[rows];
        boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
        boolean noNulls = true;
        for (int i = 0; i < rows; i++) {
            if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                isNull[i] = true;
                noNulls = false;
            } else {
                byte[] bytes = cr.getBinary().getBytes();
                short float16Bits = (short) ((bytes[1] & 0xFF) << 8 | (bytes[0] & 0xFF));
                values[i] = Float.float16ToFloat(float16Bits);
            }
            cr.consume();
        }
        return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, noNulls, false, isNull);
    }

    private Block readBytesRefColumn(ColumnReader cr, ColumnInfo info, int rows) {
        boolean isUuid = info.logicalType() instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
        try (var builder = blockFactory.newBytesRefBlockBuilder(rows)) {
            for (int i = 0; i < rows; i++) {
                if (info.maxDefLevel() > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel()) {
                    builder.appendNull();
                } else if (isUuid) {
                    builder.appendBytesRef(new BytesRef(formatUuid(cr.getBinary().getBytes())));
                } else {
                    builder.appendBytesRef(new BytesRef(cr.getBinary().getBytes()));
                }
                cr.consume();
            }
            return builder.build();
        }
    }

    private Block readDatetimeColumn(ColumnReader cr, ColumnInfo info, int rows) {
        if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT96) {
            return readInt96TimestampColumn(cr, info.maxDefLevel(), rows);
        }
        long[] values = new long[rows];
        boolean[] isNull = info.maxDefLevel() > 0 ? new boolean[rows] : null;
        boolean noNulls = true;
        boolean isDate = info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32;
        for (int i = 0; i < rows; i++) {
            if (info.maxDefLevel() > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel()) {
                isNull[i] = true;
                noNulls = false;
            } else if (isDate) {
                values[i] = cr.getInteger() * MILLIS_PER_DAY;
            } else {
                long raw = cr.getLong();
                values[i] = convertTimestampToMillis(raw, info.logicalType());
            }
            cr.consume();
        }
        return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull);
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

    private Block readInt96TimestampColumn(ColumnReader cr, int maxDef, int rows) {
        long[] values = new long[rows];
        boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
        boolean noNulls = true;
        for (int i = 0; i < rows; i++) {
            if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                isNull[i] = true;
                noNulls = false;
            } else {
                Binary bin = cr.getBinary();
                ByteBuffer buf = ByteBuffer.wrap(bin.getBytes()).order(ByteOrder.LITTLE_ENDIAN);
                long nanosOfDay = buf.getLong();
                int julianDay = buf.getInt();
                long epochDay = julianDay - JULIAN_EPOCH_OFFSET;
                values[i] = epochDay * MILLIS_PER_DAY + nanosOfDay / NANOS_PER_MILLI;
            }
            cr.consume();
        }
        return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull);
    }

    // --- List column readers ---

    private Block readListColumn(ColumnReader cr, ColumnInfo info, int rows) {
        DataType elementType = info.esqlType();
        int maxDef = info.maxDefLevel();
        return switch (elementType) {
            case INTEGER -> readListIntColumn(cr, maxDef, rows);
            case LONG -> readListLongColumn(cr, maxDef, rows);
            case DOUBLE -> readListDoubleColumn(cr, maxDef, rows);
            case BOOLEAN -> readListBooleanColumn(cr, maxDef, rows);
            case KEYWORD, TEXT -> readListBytesRefColumn(cr, maxDef, rows);
            case DATETIME -> readListDatetimeColumn(cr, info, rows);
            default -> {
                skipListValues(cr, maxDef, rows);
                yield blockFactory.newConstantNullBlock(rows);
            }
        };
    }

    private static void skipListValues(ColumnReader cr, int maxDef, int rows) {
        for (int row = 0; row < rows; row++) {
            cr.consume();
            while (cr.getCurrentRepetitionLevel() > 0) {
                cr.consume();
            }
        }
    }

    private Block readListIntColumn(ColumnReader cr, int maxDef, int rows) {
        try (var builder = blockFactory.newIntBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendInt(cr.getInteger()));
            }
            return builder.build();
        }
    }

    private Block readListLongColumn(ColumnReader cr, int maxDef, int rows) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendLong(cr.getLong()));
            }
            return builder.build();
        }
    }

    private Block readListDoubleColumn(ColumnReader cr, int maxDef, int rows) {
        try (var builder = blockFactory.newDoubleBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendDouble(cr.getDouble()));
            }
            return builder.build();
        }
    }

    private Block readListBooleanColumn(ColumnReader cr, int maxDef, int rows) {
        try (var builder = blockFactory.newBooleanBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendBoolean(cr.getBoolean()));
            }
            return builder.build();
        }
    }

    private Block readListBytesRefColumn(ColumnReader cr, int maxDef, int rows) {
        try (var builder = blockFactory.newBytesRefBlockBuilder(rows)) {
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendBytesRef(new BytesRef(cr.getBinary().getBytes())));
            }
            return builder.build();
        }
    }

    private Block readListDatetimeColumn(ColumnReader cr, ColumnInfo info, int rows) {
        try (var builder = blockFactory.newLongBlockBuilder(rows)) {
            int maxDef = info.maxDefLevel();
            for (int row = 0; row < rows; row++) {
                readListRow(cr, maxDef, builder, () -> builder.appendLong(convertTimestampToMillis(cr.getLong(), info.logicalType())));
            }
            return builder.build();
        }
    }

    @FunctionalInterface
    private interface ValueAppender {
        void append();
    }

    private static void readListRow(ColumnReader cr, int maxDef, Block.Builder builder, ValueAppender appender) {
        int def = cr.getCurrentDefinitionLevel();
        if (def >= maxDef) {
            builder.beginPositionEntry();
            appender.append();
            cr.consume();
            while (cr.getCurrentRepetitionLevel() > 0) {
                if (cr.getCurrentDefinitionLevel() >= maxDef) {
                    appender.append();
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
                    appender.append();
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

    // --- Utilities ---

    private static void skipValues(ColumnReader cr, int rows) {
        for (int i = 0; i < rows; i++) {
            cr.consume();
        }
    }

    private static java.util.BitSet toBitSet(boolean[] isNull, int length) {
        java.util.BitSet bits = new java.util.BitSet(length);
        for (int i = 0; i < length; i++) {
            if (isNull[i]) {
                bits.set(i);
            }
        }
        return bits;
    }

    static String formatUuid(byte[] bytes) {
        if (bytes == null || bytes.length < 16) {
            throw new QlIllegalArgumentException("UUID requires 16 bytes, got " + (bytes == null ? "null" : bytes.length));
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

    @Override
    public void close() throws IOException {
        try {
            if (rowGroup != null) {
                rowGroup.close();
            }
        } finally {
            reader.close();
        }
    }

    /**
     * Minimal GroupConverter that satisfies {@link ColumnReadStoreImpl}'s constructor.
     */
    private static class NoOpGroupConverter extends GroupConverter {
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

    private static class NoOpPrimitiveConverter extends PrimitiveConverter {}
}
