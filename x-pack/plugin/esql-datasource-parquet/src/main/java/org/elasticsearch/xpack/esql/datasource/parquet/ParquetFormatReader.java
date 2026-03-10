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
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.InputFile;
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
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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
public class ParquetFormatReader implements FormatReader {

    private final BlockFactory blockFactory;

    public ParquetFormatReader(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        List<Attribute> schema = readSchema(object);
        return new SimpleSourceMetadata(schema, formatName(), object.path().toString());
    }

    private List<Attribute> readSchema(StorageObject object) throws IOException {
        // Adapt StorageObject to Parquet InputFile
        InputFile parquetInputFile = new ParquetStorageObjectAdapter(object);

        // Build ParquetReadOptions with SKIP_ROW_GROUPS to only read schema metadata
        ParquetReadOptions options = ParquetReadOptions.builder().withMetadataFilter(ParquetMetadataConverter.SKIP_ROW_GROUPS).build();

        try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options)) {
            FileMetaData fileMetaData = reader.getFileMetaData();
            MessageType parquetSchema = fileMetaData.getSchema();

            // Convert Parquet schema directly to ESQL Attributes
            return convertParquetSchemaToAttributes(parquetSchema);
        }
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
            return DataType.UNSUPPORTED; // Complex types not yet supported
        }
        PrimitiveType primitive = parquetType.asPrimitiveType();
        LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();

        return switch (primitive.getPrimitiveTypeName()) {
            case BOOLEAN -> DataType.BOOLEAN;
            case INT32 -> logical instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation ? DataType.DATETIME : DataType.INTEGER;
            case INT64 -> logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ? DataType.DATETIME : DataType.LONG;
            case FLOAT, DOUBLE -> DataType.DOUBLE;
            case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                // Check for STRING logical type
                if (logical instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                    yield DataType.KEYWORD;
                }
                // Default binary to keyword
                yield DataType.KEYWORD;
            }
            default -> DataType.UNSUPPORTED;
        };
    }

    private static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();

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
                    columnInfos[i] = new ColumnInfo(
                        desc,
                        desc.getPrimitiveType().getPrimitiveTypeName(),
                        attr.dataType(),
                        desc.getMaxDefinitionLevel()
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
            return switch (info.esqlType) {
                case BOOLEAN -> readBooleanColumn(cr, info.maxDefLevel, rowsToRead);
                case INTEGER -> readIntColumn(cr, info.maxDefLevel, rowsToRead);
                case LONG -> readLongColumn(cr, info.maxDefLevel, rowsToRead);
                case DOUBLE -> readDoubleColumn(cr, info.parquetType, info.maxDefLevel, rowsToRead);
                case KEYWORD, TEXT -> readBytesRefColumn(cr, info.maxDefLevel, rowsToRead);
                case DATETIME -> readDatetimeColumn(cr, info.parquetType, info.maxDefLevel, rowsToRead);
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

        private Block readDoubleColumn(ColumnReader cr, PrimitiveType.PrimitiveTypeName pType, int maxDef, int rows) {
            double[] values = new double[rows];
            boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            boolean isFloat = pType == PrimitiveType.PrimitiveTypeName.FLOAT;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull[i] = true;
                    noNulls = false;
                } else {
                    values[i] = isFloat ? cr.getFloat() : cr.getDouble();
                }
                cr.consume();
            }
            return ColumnBlockConversions.doubleColumn(blockFactory, values, rows, noNulls, false, isNull);
        }

        private Block readBytesRefColumn(ColumnReader cr, int maxDef, int rows) {
            try (var builder = blockFactory.newBytesRefBlockBuilder(rows)) {
                for (int i = 0; i < rows; i++) {
                    if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                        builder.appendNull();
                    } else {
                        builder.appendBytesRef(new BytesRef(cr.getBinary().getBytes()));
                    }
                    cr.consume();
                }
                return builder.build();
            }
        }

        private Block readDatetimeColumn(ColumnReader cr, PrimitiveType.PrimitiveTypeName pType, int maxDef, int rows) {
            long[] values = new long[rows];
            boolean[] isNull = maxDef > 0 ? new boolean[rows] : null;
            boolean noNulls = true;
            boolean isDate = pType == PrimitiveType.PrimitiveTypeName.INT32;
            for (int i = 0; i < rows; i++) {
                if (maxDef > 0 && cr.getCurrentDefinitionLevel() < maxDef) {
                    isNull[i] = true;
                    noNulls = false;
                } else if (isDate) {
                    values[i] = cr.getInteger() * MILLIS_PER_DAY;
                } else {
                    values[i] = cr.getLong();
                }
                cr.consume();
            }
            return ColumnBlockConversions.longColumn(blockFactory, values, rows, noNulls, false, isNull);
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
        int maxDefLevel
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
