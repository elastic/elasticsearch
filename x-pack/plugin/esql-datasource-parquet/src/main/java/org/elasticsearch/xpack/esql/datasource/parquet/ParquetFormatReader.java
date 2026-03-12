/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnBlockConversions;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * FormatReader implementation for Parquet files.
 *
 * <p>Uses Parquet's native ParquetFileReader with our StorageObject abstraction.
 * Produces ESQL Page batches directly without requiring Arrow as an intermediate format.
 *
 * <p>Key features:
 * <ul>
 *   <li>Works with any StorageProvider (HTTP, S3, local)</li>
 *   <li>Efficient columnar reading with column projection</li>
 *   <li>No Hadoop dependencies in the core path</li>
 *   <li>Direct conversion from Parquet to ESQL blocks</li>
 * </ul>
 */
public class ParquetFormatReader implements RangeAwareFormatReader {

    private final BlockFactory blockFactory;

    public ParquetFormatReader(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        InputFile parquetInputFile = new ParquetStorageObjectAdapter(object);
        ParquetReadOptions options = ParquetReadOptions.builder().build();

        try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options)) {
            FileMetaData fileMetaData = reader.getFileMetaData();
            MessageType parquetSchema = fileMetaData.getSchema();
            List<Attribute> schema = convertParquetSchemaToAttributes(parquetSchema);
            SourceStatistics statistics = extractStatistics(reader, parquetSchema);
            return new SimpleSourceMetadata(schema, formatName(), object.path().toString(), statistics, null);
        }
    }

    @SuppressWarnings("rawtypes")
    private SourceStatistics extractStatistics(ParquetFileReader reader, MessageType schema) {
        List<BlockMetaData> rowGroups = reader.getRowGroups();
        if (rowGroups.isEmpty()) {
            return null;
        }

        long totalRows = 0;
        long totalSize = 0;
        Map<String, long[]> nullCounts = new HashMap<>();
        Map<String, Comparable[]> mins = new HashMap<>();
        Map<String, Comparable[]> maxs = new HashMap<>();

        for (BlockMetaData rowGroup : rowGroups) {
            totalRows += rowGroup.getRowCount();
            totalSize += rowGroup.getTotalByteSize();
            for (ColumnChunkMetaData col : rowGroup.getColumns()) {
                String colName = col.getPath().toDotString();
                Statistics stats = col.getStatistics();
                if (stats == null || stats.isEmpty()) {
                    continue;
                }
                nullCounts.merge(colName, new long[] { stats.getNumNulls() }, (a, b) -> {
                    a[0] += b[0];
                    return a;
                });
                if (stats.hasNonNullValue()) {
                    mins.merge(colName, new Comparable[] { stats.genericGetMin() }, (a, b) -> {
                        @SuppressWarnings("unchecked")
                        int cmp = a[0].compareTo(b[0]);
                        if (cmp > 0) a[0] = b[0];
                        return a;
                    });
                    maxs.merge(colName, new Comparable[] { stats.genericGetMax() }, (a, b) -> {
                        @SuppressWarnings("unchecked")
                        int cmp = a[0].compareTo(b[0]);
                        if (cmp < 0) a[0] = b[0];
                        return a;
                    });
                }
            }
        }

        final long rowCount = totalRows;
        final long sizeBytes = totalSize;
        Map<String, SourceStatistics.ColumnStatistics> columnStats = new HashMap<>();
        for (Type field : schema.getFields()) {
            String name = field.getName();
            long[] nc = nullCounts.get(name);
            Comparable[] mn = mins.get(name);
            Comparable[] mx = maxs.get(name);
            if (nc != null || mn != null || mx != null) {
                final long nullCount = nc != null ? nc[0] : 0;
                final Object minVal = mn != null ? mn[0] : null;
                final Object maxVal = mx != null ? mx[0] : null;
                columnStats.put(name, new SourceStatistics.ColumnStatistics() {
                    @Override
                    public OptionalLong nullCount() {
                        return OptionalLong.of(nullCount);
                    }

                    @Override
                    public OptionalLong distinctCount() {
                        return OptionalLong.empty();
                    }

                    @Override
                    public Optional<Object> minValue() {
                        return Optional.ofNullable(minVal);
                    }

                    @Override
                    public Optional<Object> maxValue() {
                        return Optional.ofNullable(maxVal);
                    }
                });
            }
        }

        return new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return OptionalLong.of(rowCount);
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.of(sizeBytes);
            }

            @Override
            public Optional<Map<String, ColumnStatistics>> columnStatistics() {
                return columnStats.isEmpty() ? Optional.empty() : Optional.of(columnStats);
            }
        };
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        // Adapt StorageObject to Parquet InputFile
        InputFile parquetInputFile = new ParquetStorageObjectAdapter(object);

        // Build ParquetReadOptions for data reading
        ParquetReadOptions options = ParquetReadOptions.builder().build();

        // Open the Parquet file reader
        ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options);

        // Get the schema
        FileMetaData fileMetaData = reader.getFileMetaData();
        MessageType parquetSchema = fileMetaData.getSchema();
        List<Attribute> attributes = convertParquetSchemaToAttributes(parquetSchema);

        // Filter attributes based on projection
        List<Attribute> projectedAttributes;
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            projectedAttributes = attributes;
        } else {
            projectedAttributes = new ArrayList<>();
            Map<String, Attribute> attributeMap = new HashMap<>();
            for (Attribute attr : attributes) {
                attributeMap.put(attr.name(), attr);
            }
            for (String columnName : projectedColumns) {
                Attribute attr = attributeMap.get(columnName);
                attr = attr == null ? new ReferenceAttribute(Source.EMPTY, columnName, DataType.NULL) : attr;
                projectedAttributes.add(attr);
            }
        }

        MessageType projectedSchema = buildProjectedSchema(parquetSchema, projectedAttributes);
        String createdBy = fileMetaData.getCreatedBy();

        return new ParquetColumnIterator(reader, projectedSchema, projectedAttributes, batchSize, blockFactory, NO_LIMIT, createdBy);
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize, int rowLimit)
        throws IOException {
        InputFile parquetInputFile = new ParquetStorageObjectAdapter(object);
        ParquetReadOptions options = ParquetReadOptions.builder().build();
        ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options);

        FileMetaData fileMetaData = reader.getFileMetaData();
        MessageType parquetSchema = fileMetaData.getSchema();
        List<Attribute> attributes = convertParquetSchemaToAttributes(parquetSchema);

        List<Attribute> projectedAttributes;
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            projectedAttributes = attributes;
        } else {
            projectedAttributes = new ArrayList<>();
            Map<String, Attribute> attributeMap = new HashMap<>();
            for (Attribute attr : attributes) {
                attributeMap.put(attr.name(), attr);
            }
            for (String columnName : projectedColumns) {
                Attribute attr = attributeMap.get(columnName);
                attr = attr == null ? new ReferenceAttribute(Source.EMPTY, columnName, DataType.NULL) : attr;
                projectedAttributes.add(attr);
            }
        }

        MessageType projectedSchema = buildProjectedSchema(parquetSchema, projectedAttributes);
        String createdBy = fileMetaData.getCreatedBy();
        return new ParquetColumnIterator(reader, projectedSchema, projectedAttributes, batchSize, blockFactory, rowLimit, createdBy);
    }

    @Override
    public String formatName() {
        return "parquet";
    }

    @Override
    public List<String> fileExtensions() {
        return List.of(".parquet", ".parq");
    }

    @Override
    public void close() throws IOException {
        // No resources to close at the reader level
    }

    @Override
    public List<long[]> discoverSplitRanges(StorageObject object) throws IOException {
        InputFile parquetInputFile = new ParquetStorageObjectAdapter(object);
        ParquetReadOptions options = ParquetReadOptions.builder().build();
        try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options)) {
            List<BlockMetaData> rowGroups = reader.getRowGroups();
            if (rowGroups.size() <= 1) {
                return List.of();
            }
            List<long[]> ranges = new ArrayList<>(rowGroups.size());
            for (BlockMetaData block : rowGroups) {
                ranges.add(new long[] { block.getStartingPos(), block.getTotalByteSize() });
            }
            return ranges;
        }
    }

    /**
     * Reads only row groups whose starting position falls within {@code [rangeStart, rangeEnd)}.
     * errorPolicy is accepted for interface compliance but not applied — Parquet errors are
     * structural (corrupt page, schema mismatch) rather than row-level.
     */
    @Override
    public CloseableIterator<Page> readRange(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        long rangeStart,
        long rangeEnd,
        List<Attribute> resolvedAttributes,
        ErrorPolicy errorPolicy
    ) throws IOException {
        InputFile parquetInputFile = new ParquetStorageObjectAdapter(object);
        ParquetReadOptions options = ParquetReadOptions.builder().withRange(rangeStart, rangeEnd).build();
        ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options);

        FileMetaData fileMetaData = reader.getFileMetaData();
        MessageType parquetSchema = fileMetaData.getSchema();
        List<Attribute> attributes = convertParquetSchemaToAttributes(parquetSchema);

        List<Attribute> projectedAttributes;
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            projectedAttributes = attributes;
        } else {
            projectedAttributes = new ArrayList<>();
            Map<String, Attribute> attributeMap = new HashMap<>();
            for (Attribute attr : attributes) {
                attributeMap.put(attr.name(), attr);
            }
            for (String columnName : projectedColumns) {
                Attribute attr = attributeMap.get(columnName);
                attr = attr == null ? new ReferenceAttribute(Source.EMPTY, columnName, DataType.NULL) : attr;
                projectedAttributes.add(attr);
            }
        }

        MessageType projectedSchema = buildProjectedSchema(parquetSchema, projectedAttributes);
        String createdBy = fileMetaData.getCreatedBy();
        return new ParquetColumnIterator(reader, projectedSchema, projectedAttributes, batchSize, blockFactory, NO_LIMIT, createdBy);
    }

    private static MessageType buildProjectedSchema(MessageType fullSchema, List<Attribute> projectedAttributes) {
        List<Type> projectedFields = new ArrayList<>();
        for (Attribute attr : projectedAttributes) {
            if (fullSchema.containsField(attr.name())) {
                projectedFields.add(fullSchema.getType(attr.name()));
            }
        }
        // Parquet requires at least one field; fall back to full schema when none match
        if (projectedFields.isEmpty()) {
            return fullSchema;
        }
        return new MessageType(fullSchema.getName(), projectedFields);
    }

    private List<Attribute> convertParquetSchemaToAttributes(MessageType schema) {
        List<Attribute> attributes = new ArrayList<>();
        for (Type field : schema.getFields()) {
            String name = field.getName();
            DataType esqlType = convertParquetTypeToEsql(field);
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, esqlType));
        }
        return attributes;
    }

    private DataType convertParquetTypeToEsql(Type parquetType) {
        if (parquetType.isPrimitive() == false) {
            return convertGroupTypeToEsql(parquetType.asGroupType());
        }
        PrimitiveType primitive = parquetType.asPrimitiveType();
        LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();

        return switch (primitive.getPrimitiveTypeName()) {
            case BOOLEAN -> DataType.BOOLEAN;
            case INT32 -> {
                if (logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                    yield DataType.DATETIME;
                } else if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    yield DataType.DOUBLE;
                }
                yield DataType.INTEGER;
            }
            case INT64 -> {
                if (logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                    yield DataType.DATETIME;
                } else if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    yield DataType.DOUBLE;
                }
                yield DataType.LONG;
            }
            case INT96 -> DataType.DATETIME;
            case FLOAT, DOUBLE -> DataType.DOUBLE;
            case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    yield DataType.DOUBLE;
                }
                if (logical instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
                    yield DataType.DOUBLE;
                }
                yield DataType.KEYWORD;
            }
            default -> DataType.UNSUPPORTED;
        };
    }

    /**
     * Handles Parquet group types. Supports LIST of primitives by extracting the element type.
     */
    private DataType convertGroupTypeToEsql(GroupType groupType) {
        LogicalTypeAnnotation logical = groupType.getLogicalTypeAnnotation();
        if (logical instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation && groupType.getFieldCount() == 1) {
            GroupType repeatedGroup = groupType.getType(0).asGroupType();
            if (repeatedGroup.getFieldCount() == 1) {
                Type elementType = repeatedGroup.getType(0);
                if (elementType.isPrimitive()) {
                    return convertParquetTypeToEsql(elementType);
                }
            }
        }
        return DataType.UNSUPPORTED;
    }

    private static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();
    private static final long NANOS_PER_MILLI = 1_000_000L;
    /** Julian day number for Unix epoch (1970-01-01). */
    private static final int JULIAN_EPOCH_OFFSET = 2_440_588;

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    /**
     * Formats a 16-byte UUID in big-endian layout as the standard 8-4-4-4-12 hex string.
     */
    static String formatUuid(byte[] bytes) {
        if (bytes == null || bytes.length < 16) {
            throw new IllegalArgumentException("UUID requires 16 bytes, got " + (bytes == null ? "null" : bytes.length));
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

    /**
     * Column-at-a-time Parquet iterator. Uses {@link ColumnReadStoreImpl} and {@link ColumnReader}
     * to decode each column independently into typed arrays, eliminating Group object materialization
     * and per-row type dispatch. Primitive columns are converted via {@link ColumnBlockConversions}.
     */
    private static class ParquetColumnIterator implements CloseableIterator<Page> {
        private final ParquetFileReader reader;
        private final MessageType projectedSchema;
        private final List<Attribute> attributes;
        private final int batchSize;
        private final BlockFactory blockFactory;
        private final String createdBy;
        private int rowBudget;

        /** Per-attribute column metadata; null for attributes not present in the file. */
        private final ColumnInfo[] columnInfos;

        private ColumnReader[] columnReaders;
        private long rowsRemainingInGroup;
        private boolean exhausted = false;

        ParquetColumnIterator(
            ParquetFileReader reader,
            MessageType projectedSchema,
            List<Attribute> attributes,
            int batchSize,
            BlockFactory blockFactory,
            int rowLimit,
            String createdBy
        ) {
            this.reader = reader;
            this.projectedSchema = projectedSchema;
            this.attributes = attributes;
            this.batchSize = batchSize;
            this.blockFactory = blockFactory;
            this.rowBudget = rowLimit;
            this.createdBy = createdBy != null ? createdBy : "";

            this.columnInfos = new ColumnInfo[attributes.size()];
            Map<String, ColumnDescriptor> descByName = new HashMap<>();
            for (ColumnDescriptor desc : projectedSchema.getColumns()) {
                descByName.put(desc.getPath()[0], desc);
            }
            for (int i = 0; i < attributes.size(); i++) {
                Attribute attr = attributes.get(i);
                if (attr.dataType() == DataType.NULL || attr.dataType() == DataType.UNSUPPORTED) {
                    continue;
                }
                ColumnDescriptor desc = descByName.get(attr.name());
                if (desc != null) {
                    LogicalTypeAnnotation logicalType = desc.getPrimitiveType().getLogicalTypeAnnotation();
                    columnInfos[i] = new ColumnInfo(
                        desc,
                        desc.getPrimitiveType().getPrimitiveTypeName(),
                        attr.dataType(),
                        desc.getMaxDefinitionLevel(),
                        desc.getMaxRepetitionLevel(),
                        logicalType
                    );
                }
            }
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
                throw new RuntimeException("Failed to read Parquet row group", e);
            }
        }

        private boolean advanceRowGroup() throws IOException {
            PageReadStore rowGroup = reader.readNextRowGroup();
            if (rowGroup == null) {
                exhausted = true;
                return false;
            }
            rowsRemainingInGroup = rowGroup.getRowCount();
            ColumnReadStoreImpl store = new ColumnReadStoreImpl(
                rowGroup,
                new NoOpGroupConverter(projectedSchema),
                projectedSchema,
                createdBy
            );
            columnReaders = new ColumnReader[columnInfos.length];
            for (int i = 0; i < columnInfos.length; i++) {
                if (columnInfos[i] != null) {
                    columnReaders[i] = store.getColumnReader(columnInfos[i].descriptor);
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
                        blocks[col] = readColumnBlock(columnReaders[col], info, rowsToRead);
                    }
                }
            } catch (Exception e) {
                Releasables.closeExpectNoException(blocks);
                throw new RuntimeException("Failed to create Page batch", e);
            }

            rowsRemainingInGroup -= rowsToRead;
            if (rowBudget != FormatReader.NO_LIMIT) {
                rowBudget -= rowsToRead;
            }
            return new Page(blocks);
        }

        private Block readColumnBlock(ColumnReader cr, ColumnInfo info, int rowsToRead) {
            if (info.maxRepLevel > 0) {
                return readListColumn(cr, info, rowsToRead);
            }
            return switch (info.esqlType) {
                case BOOLEAN -> readBooleanColumn(cr, info.maxDefLevel, rowsToRead);
                case INTEGER -> readIntColumn(cr, info.maxDefLevel, rowsToRead);
                case LONG -> readLongColumn(cr, info.maxDefLevel, rowsToRead);
                case DOUBLE -> readDoubleColumn(cr, info, rowsToRead);
                case KEYWORD, TEXT -> readBytesRefColumn(cr, info, rowsToRead);
                case DATETIME -> readDatetimeColumn(cr, info, rowsToRead);
                default -> {
                    skipValues(cr, rowsToRead);
                    yield blockFactory.newConstantNullBlock(rowsToRead);
                }
            };
        }

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
            LogicalTypeAnnotation logical = info.logicalType;
            if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
                return readDecimalAsDoubleColumn(cr, info, decimal.getScale(), rows);
            }
            if (logical instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
                return readFloat16Column(cr, info.maxDefLevel, rows);
            }
            double[] values = new double[rows];
            boolean[] isNull = info.maxDefLevel > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            boolean isFloat = info.parquetType == PrimitiveType.PrimitiveTypeName.FLOAT;
            for (int i = 0; i < rows; i++) {
                if (info.maxDefLevel > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel) {
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
            boolean[] isNull = info.maxDefLevel > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            for (int i = 0; i < rows; i++) {
                if (info.maxDefLevel > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel) {
                    isNull[i] = true;
                    noNulls = false;
                } else {
                    BigInteger unscaled = switch (info.parquetType) {
                        case INT32 -> BigInteger.valueOf(cr.getInteger());
                        case INT64 -> BigInteger.valueOf(cr.getLong());
                        case BINARY, FIXED_LEN_BYTE_ARRAY -> new BigInteger(cr.getBinary().getBytes());
                        default -> throw new IllegalStateException("Unexpected DECIMAL backing type: " + info.parquetType);
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
            boolean isUuid = info.logicalType instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
            try (var builder = blockFactory.newBytesRefBlockBuilder(rows)) {
                for (int i = 0; i < rows; i++) {
                    if (info.maxDefLevel > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel) {
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
            if (info.parquetType == PrimitiveType.PrimitiveTypeName.INT96) {
                return readInt96TimestampColumn(cr, info.maxDefLevel, rows);
            }
            long[] values = new long[rows];
            boolean[] isNull = info.maxDefLevel > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            boolean isDate = info.parquetType == PrimitiveType.PrimitiveTypeName.INT32;
            for (int i = 0; i < rows; i++) {
                if (info.maxDefLevel > 0 && cr.getCurrentDefinitionLevel() < info.maxDefLevel) {
                    isNull[i] = true;
                    noNulls = false;
                } else if (isDate) {
                    values[i] = cr.getInteger() * MILLIS_PER_DAY;
                } else {
                    long raw = cr.getLong();
                    values[i] = convertTimestampToMillis(raw, info.logicalType);
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

        /**
         * Converts a Parquet INT96 value (12 bytes: 8 bytes nanos-of-day LE + 4 bytes Julian day LE)
         * to epoch milliseconds.
         */
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

        /**
         * Reads a LIST column using repetition levels to determine list boundaries,
         * producing multi-valued ESQL blocks. Handles null lists, empty lists, and
         * null elements within lists correctly.
         */
        private Block readListColumn(ColumnReader cr, ColumnInfo info, int rows) {
            DataType elementType = info.esqlType;
            int maxDef = info.maxDefLevel;
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

        /**
         * Skips all Parquet values for the given number of rows in a LIST column,
         * respecting repetition levels to consume entire lists per row.
         */
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
                    int def = cr.getCurrentDefinitionLevel();
                    if (def >= maxDef) {
                        builder.beginPositionEntry();
                        builder.appendInt(cr.getInteger());
                        cr.consume();
                        while (cr.getCurrentRepetitionLevel() > 0) {
                            if (cr.getCurrentDefinitionLevel() >= maxDef) {
                                builder.appendInt(cr.getInteger());
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
                                builder.appendInt(cr.getInteger());
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
                return builder.build();
            }
        }

        private Block readListLongColumn(ColumnReader cr, int maxDef, int rows) {
            try (var builder = blockFactory.newLongBlockBuilder(rows)) {
                for (int row = 0; row < rows; row++) {
                    int def = cr.getCurrentDefinitionLevel();
                    if (def >= maxDef) {
                        builder.beginPositionEntry();
                        builder.appendLong(cr.getLong());
                        cr.consume();
                        while (cr.getCurrentRepetitionLevel() > 0) {
                            if (cr.getCurrentDefinitionLevel() >= maxDef) {
                                builder.appendLong(cr.getLong());
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
                                builder.appendLong(cr.getLong());
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
                return builder.build();
            }
        }

        private Block readListDoubleColumn(ColumnReader cr, int maxDef, int rows) {
            try (var builder = blockFactory.newDoubleBlockBuilder(rows)) {
                for (int row = 0; row < rows; row++) {
                    int def = cr.getCurrentDefinitionLevel();
                    if (def >= maxDef) {
                        builder.beginPositionEntry();
                        builder.appendDouble(cr.getDouble());
                        cr.consume();
                        while (cr.getCurrentRepetitionLevel() > 0) {
                            if (cr.getCurrentDefinitionLevel() >= maxDef) {
                                builder.appendDouble(cr.getDouble());
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
                                builder.appendDouble(cr.getDouble());
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
                return builder.build();
            }
        }

        private Block readListBooleanColumn(ColumnReader cr, int maxDef, int rows) {
            try (var builder = blockFactory.newBooleanBlockBuilder(rows)) {
                for (int row = 0; row < rows; row++) {
                    int def = cr.getCurrentDefinitionLevel();
                    if (def >= maxDef) {
                        builder.beginPositionEntry();
                        builder.appendBoolean(cr.getBoolean());
                        cr.consume();
                        while (cr.getCurrentRepetitionLevel() > 0) {
                            if (cr.getCurrentDefinitionLevel() >= maxDef) {
                                builder.appendBoolean(cr.getBoolean());
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
                                builder.appendBoolean(cr.getBoolean());
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
                return builder.build();
            }
        }

        private Block readListBytesRefColumn(ColumnReader cr, int maxDef, int rows) {
            try (var builder = blockFactory.newBytesRefBlockBuilder(rows)) {
                for (int row = 0; row < rows; row++) {
                    int def = cr.getCurrentDefinitionLevel();
                    if (def >= maxDef) {
                        builder.beginPositionEntry();
                        builder.appendBytesRef(new BytesRef(cr.getBinary().getBytes()));
                        cr.consume();
                        while (cr.getCurrentRepetitionLevel() > 0) {
                            if (cr.getCurrentDefinitionLevel() >= maxDef) {
                                builder.appendBytesRef(new BytesRef(cr.getBinary().getBytes()));
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
                                builder.appendBytesRef(new BytesRef(cr.getBinary().getBytes()));
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
                return builder.build();
            }
        }

        private Block readListDatetimeColumn(ColumnReader cr, ColumnInfo info, int rows) {
            try (var builder = blockFactory.newLongBlockBuilder(rows)) {
                int maxDef = info.maxDefLevel;
                for (int row = 0; row < rows; row++) {
                    int def = cr.getCurrentDefinitionLevel();
                    if (def >= maxDef) {
                        builder.beginPositionEntry();
                        builder.appendLong(convertTimestampToMillis(cr.getLong(), info.logicalType));
                        cr.consume();
                        while (cr.getCurrentRepetitionLevel() > 0) {
                            if (cr.getCurrentDefinitionLevel() >= maxDef) {
                                builder.appendLong(convertTimestampToMillis(cr.getLong(), info.logicalType));
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
                                builder.appendLong(convertTimestampToMillis(cr.getLong(), info.logicalType));
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
                return builder.build();
            }
        }

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

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private record ColumnInfo(
        ColumnDescriptor descriptor,
        PrimitiveType.PrimitiveTypeName parquetType,
        DataType esqlType,
        int maxDefLevel,
        int maxRepLevel,
        LogicalTypeAnnotation logicalType
    ) {}

    /**
     * Minimal GroupConverter that satisfies {@link ColumnReadStoreImpl}'s constructor.
     * We never call {@code writeCurrentValueToConverter()}, so all converters are no-ops.
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
