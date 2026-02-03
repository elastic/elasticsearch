/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
        org.apache.parquet.io.InputFile parquetInputFile = new ParquetStorageObjectAdapter(object);

        // Build ParquetReadOptions with SKIP_ROW_GROUPS to only read schema metadata
        ParquetReadOptions options = ParquetReadOptions.builder().withMetadataFilter(ParquetMetadataConverter.SKIP_ROW_GROUPS).build();

        try (ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options)) {
            org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData = reader.getFileMetaData();
            MessageType parquetSchema = fileMetaData.getSchema();

            // Convert Parquet schema directly to ESQL Attributes
            return convertParquetSchemaToAttributes(parquetSchema);
        }
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        // Adapt StorageObject to Parquet InputFile
        org.apache.parquet.io.InputFile parquetInputFile = new ParquetStorageObjectAdapter(object);

        // Build ParquetReadOptions for data reading
        ParquetReadOptions options = ParquetReadOptions.builder().build();

        // Open the Parquet file reader
        ParquetFileReader reader = ParquetFileReader.open(parquetInputFile, options);

        // Get the schema
        org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData = reader.getFileMetaData();
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
                if (attr != null) {
                    projectedAttributes.add(attr);
                }
            }
        }

        return new ParquetPageIterator(reader, parquetSchema, projectedAttributes, batchSize, blockFactory);
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

    private static class ParquetPageIterator implements CloseableIterator<Page> {
        private final ParquetFileReader reader;
        private final MessageType parquetSchema;
        private final List<Attribute> attributes;
        private final int batchSize;
        private final MessageColumnIO columnIO;
        private final BlockFactory blockFactory;

        private PageReadStore currentRowGroup;
        private RecordReader<Group> recordReader;
        private long rowsRemainingInGroup;
        private boolean exhausted = false;

        ParquetPageIterator(
            ParquetFileReader reader,
            MessageType parquetSchema,
            List<Attribute> attributes,
            int batchSize,
            BlockFactory blockFactory
        ) {
            this.reader = reader;
            this.parquetSchema = parquetSchema;
            this.attributes = attributes;
            this.batchSize = batchSize;
            this.columnIO = new ColumnIOFactory().getColumnIO(parquetSchema);
            this.blockFactory = blockFactory;
        }

        @Override
        public boolean hasNext() {
            if (exhausted) {
                return false;
            }
            // Check if we have rows in current group or can read more groups
            if (rowsRemainingInGroup > 0) {
                return true;
            }
            // Try to read next row group
            try {
                currentRowGroup = reader.readNextRowGroup();
                if (currentRowGroup == null) {
                    exhausted = true;
                    return false;
                }
                rowsRemainingInGroup = currentRowGroup.getRowCount();
                recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(parquetSchema));
                return rowsRemainingInGroup > 0;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read Parquet row group", e);
            }
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }

            try {
                // Read records up to batch size
                List<Group> batch = new ArrayList<>(batchSize);
                int rowsToRead = (int) Math.min(batchSize, rowsRemainingInGroup);

                for (int i = 0; i < rowsToRead; i++) {
                    Group group = recordReader.read();
                    if (group != null) {
                        batch.add(group);
                        rowsRemainingInGroup--;
                    }
                }

                if (batch.isEmpty()) {
                    throw new NoSuchElementException("No more records");
                }

                // Convert batch to ESQL Page
                return convertToPage(batch);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create Page batch", e);
            }
        }

        private Page convertToPage(List<Group> batch) {
            int rowCount = batch.size();
            Block[] blocks = new Block[attributes.size()];

            // Create a block for each attribute
            for (int col = 0; col < attributes.size(); col++) {
                Attribute attribute = attributes.get(col);
                String fieldName = attribute.name();
                DataType dataType = attribute.dataType();

                blocks[col] = createBlock(batch, fieldName, dataType, rowCount);
            }

            return new Page(blocks);
        }

        private Block createBlock(List<Group> batch, String fieldName, DataType dataType, int rowCount) {
            // Find field index in Parquet schema
            int fieldIndex = findFieldIndex(batch.get(0), fieldName);
            if (fieldIndex == -1) {
                // Field not found, return null block
                return blockFactory.newConstantNullBlock(rowCount);
            }

            return switch (dataType) {
                case BOOLEAN -> createBooleanBlock(batch, fieldName, fieldIndex, rowCount);
                case INTEGER -> createIntBlock(batch, fieldName, fieldIndex, rowCount);
                case LONG -> createLongBlock(batch, fieldName, fieldIndex, rowCount);
                case DOUBLE -> createDoubleBlock(batch, fieldName, fieldIndex, rowCount);
                case KEYWORD, TEXT -> createBytesRefBlock(batch, fieldName, fieldIndex, rowCount);
                case DATETIME -> createLongBlock(batch, fieldName, fieldIndex, rowCount); // Timestamps as longs
                default -> blockFactory.newConstantNullBlock(rowCount);
            };
        }

        private int findFieldIndex(Group group, String fieldName) {
            org.apache.parquet.schema.GroupType groupType = group.getType();
            int fieldCount = groupType.getFieldCount();
            for (int i = 0; i < fieldCount; i++) {
                Type fieldType = groupType.getType(i);
                String name = fieldType.getName();
                if (name.equals(fieldName)) {
                    return i;
                }
            }
            return -1;
        }

        private Block createBooleanBlock(List<Group> batch, String fieldName, int fieldIndex, int rowCount) {
            try (var builder = blockFactory.newBooleanBlockBuilder(rowCount)) {
                for (Group group : batch) {
                    if (group.getFieldRepetitionCount(fieldIndex) == 0) {
                        builder.appendNull();
                    } else {
                        builder.appendBoolean(group.getBoolean(fieldName, 0));
                    }
                }
                return builder.build();
            }
        }

        private Block createIntBlock(List<Group> batch, String fieldName, int fieldIndex, int rowCount) {
            try (var builder = blockFactory.newIntBlockBuilder(rowCount)) {
                for (Group group : batch) {
                    if (group.getFieldRepetitionCount(fieldIndex) == 0) {
                        builder.appendNull();
                    } else {
                        builder.appendInt(group.getInteger(fieldName, 0));
                    }
                }
                return builder.build();
            }
        }

        private Block createLongBlock(List<Group> batch, String fieldName, int fieldIndex, int rowCount) {
            try (var builder = blockFactory.newLongBlockBuilder(rowCount)) {
                for (Group group : batch) {
                    if (group.getFieldRepetitionCount(fieldIndex) == 0) {
                        builder.appendNull();
                    } else {
                        builder.appendLong(group.getLong(fieldName, 0));
                    }
                }
                return builder.build();
            }
        }

        private Block createDoubleBlock(List<Group> batch, String fieldName, int fieldIndex, int rowCount) {
            try (var builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
                for (Group group : batch) {
                    if (group.getFieldRepetitionCount(fieldIndex) == 0) {
                        builder.appendNull();
                    } else {
                        // Handle both float and double
                        org.apache.parquet.schema.GroupType groupType = group.getType();
                        org.apache.parquet.schema.Type fieldType = groupType.getType(fieldIndex);
                        PrimitiveType primitiveType = fieldType.asPrimitiveType();
                        PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
                        if (typeName == PrimitiveType.PrimitiveTypeName.FLOAT) {
                            builder.appendDouble(group.getFloat(fieldName, 0));
                        } else {
                            builder.appendDouble(group.getDouble(fieldName, 0));
                        }
                    }
                }
                return builder.build();
            }
        }

        private Block createBytesRefBlock(List<Group> batch, String fieldName, int fieldIndex, int rowCount) {
            try (var builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                for (Group group : batch) {
                    if (group.getFieldRepetitionCount(fieldIndex) == 0) {
                        builder.appendNull();
                    } else {
                        String value = group.getString(fieldName, 0);
                        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                        builder.appendBytesRef(new org.apache.lucene.util.BytesRef(bytes));
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
