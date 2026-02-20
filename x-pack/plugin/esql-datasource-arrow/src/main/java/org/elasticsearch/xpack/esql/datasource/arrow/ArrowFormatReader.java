/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.arrow.ArrowToBlockConverter;
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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * FormatReader implementation for Arrow IPC streaming format files.
 *
 * <p>Uses Apache Arrow's ArrowStreamReader with our StorageObject abstraction.
 * Produces ESQL Page batches by leveraging {@link ArrowToBlockConverter} for
 * Arrow vector to ESQL Block conversion.
 *
 * <p>Key features:
 * <ul>
 *   <li>Works with any StorageProvider (HTTP, S3, local)</li>
 *   <li>Sequential streaming read (no random access needed)</li>
 *   <li>Column projection for efficient reads</li>
 *   <li>Direct conversion from Arrow vectors to ESQL blocks</li>
 * </ul>
 */
public class ArrowFormatReader implements FormatReader {

    private final BlockFactory blockFactory;

    public ArrowFormatReader(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        List<Attribute> schema = readSchema(object);
        return new SimpleSourceMetadata(schema, formatName(), object.path().toString());
    }

    private List<Attribute> readSchema(StorageObject object) throws IOException {
        try (
            BufferAllocator allocator = newTrackedAllocator(blockFactory);
            InputStream stream = object.newStream();
            ArrowStreamReader reader = new ArrowStreamReader(stream, allocator)
        ) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            Schema arrowSchema = root.getSchema();
            return convertArrowSchemaToAttributes(arrowSchema);
        }
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        BufferAllocator allocator = newTrackedAllocator(blockFactory);
        try {
            InputStream stream = object.newStream();
            ArrowStreamReader reader = new ArrowStreamReader(stream, allocator);
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            Schema arrowSchema = root.getSchema();

            List<Attribute> attributes = convertArrowSchemaToAttributes(arrowSchema);
            List<Attribute> projectedAttributes = applyProjection(attributes, projectedColumns);

            return new ArrowPageIterator(reader, stream, allocator, root, arrowSchema, projectedAttributes, batchSize, blockFactory);
        } catch (Exception e) {
            allocator.close();
            throw e;
        }
    }

    @Override
    public String formatName() {
        return "arrow";
    }

    @Override
    public List<String> fileExtensions() {
        return List.of(".arrow", ".ipc");
    }

    @Override
    public void close() throws IOException {
        // No resources to close at the reader level
    }

    /**
     * Creates a RootAllocator whose allocations and releases are tracked by the given BlockFactory's circuit breaker.
     * This ensures Arrow buffer memory is accounted for in ESQL's memory management, preventing
     * unbounded off-heap allocation.
     */
    static BufferAllocator newTrackedAllocator(BlockFactory blockFactory) {
        return new RootAllocator(new BreakerAllocationListener(blockFactory.breaker()), Long.MAX_VALUE);
    }

    private static List<Attribute> applyProjection(List<Attribute> attributes, List<String> projectedColumns) {
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            return attributes;
        }
        Map<String, Attribute> attributeMap = new HashMap<>();
        for (Attribute attr : attributes) {
            attributeMap.put(attr.name(), attr);
        }
        List<Attribute> projected = new ArrayList<>();
        for (String columnName : projectedColumns) {
            Attribute attr = attributeMap.get(columnName);
            if (attr != null) {
                projected.add(attr);
            }
        }
        return projected;
    }

    private static List<Attribute> convertArrowSchemaToAttributes(Schema schema) {
        List<Attribute> attributes = new ArrayList<>();
        for (Field field : schema.getFields()) {
            String name = field.getName();
            DataType esqlType = convertArrowTypeToEsql(field);
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, esqlType));
        }
        return attributes;
    }

    private static DataType convertArrowTypeToEsql(Field field) {
        Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
        return switch (minorType) {
            case INT -> DataType.INTEGER;
            case BIGINT -> DataType.LONG;
            case FLOAT4, FLOAT8 -> DataType.DOUBLE;
            case BIT -> DataType.BOOLEAN;
            case VARCHAR -> DataType.KEYWORD;
            case VARBINARY -> DataType.KEYWORD;
            case TIMESTAMPMICRO, TIMESTAMPMICROTZ -> DataType.DATETIME;
            default -> DataType.UNSUPPORTED;
        };
    }

    private static class ArrowPageIterator implements CloseableIterator<Page> {
        private final ArrowStreamReader reader;
        private final InputStream stream;
        private final BufferAllocator allocator;
        private final VectorSchemaRoot root;
        private final Schema arrowSchema;
        private final List<Attribute> attributes;
        private final int batchSize;
        private final BlockFactory blockFactory;

        private boolean exhausted = false;
        private boolean batchLoaded = false;
        private int batchOffset = 0;

        ArrowPageIterator(
            ArrowStreamReader reader,
            InputStream stream,
            BufferAllocator allocator,
            VectorSchemaRoot root,
            Schema arrowSchema,
            List<Attribute> attributes,
            int batchSize,
            BlockFactory blockFactory
        ) {
            this.reader = reader;
            this.stream = stream;
            this.allocator = allocator;
            this.root = root;
            this.arrowSchema = arrowSchema;
            this.attributes = attributes;
            this.batchSize = batchSize;
            this.blockFactory = blockFactory;
        }

        @Override
        public boolean hasNext() {
            if (exhausted) {
                return false;
            }
            // If we have remaining rows in the current batch, there's more data
            if (batchLoaded && batchOffset < root.getRowCount()) {
                return true;
            }
            // Try to load the next batch
            try {
                batchLoaded = reader.loadNextBatch();
                if (batchLoaded == false) {
                    exhausted = true;
                    return false;
                }
                batchOffset = 0;
                return root.getRowCount() > 0;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read Arrow batch", e);
            }
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }

            int rowCount = root.getRowCount();
            int rowsToRead = Math.min(batchSize, rowCount - batchOffset);

            Block[] blocks = new Block[attributes.size()];
            for (int col = 0; col < attributes.size(); col++) {
                Attribute attribute = attributes.get(col);
                String fieldName = attribute.name();

                int fieldIndex = findFieldIndex(arrowSchema, fieldName);
                if (fieldIndex == -1) {
                    blocks[col] = blockFactory.newConstantNullBlock(rowsToRead);
                    continue;
                }

                FieldVector vector = root.getVector(fieldIndex);
                Types.MinorType minorType = Types.getMinorTypeForArrowType(vector.getField().getType());
                ArrowToBlockConverter converter = ArrowToBlockConverter.forType(minorType);

                if (converter == null) {
                    blocks[col] = blockFactory.newConstantNullBlock(rowsToRead);
                    continue;
                }

                if (batchOffset == 0 && rowsToRead == rowCount) {
                    blocks[col] = converter.convert(vector, blockFactory);
                } else {
                    var transferPair = vector.getTransferPair(allocator);
                    transferPair.splitAndTransfer(batchOffset, rowsToRead);
                    FieldVector sliced = (FieldVector) transferPair.getTo();
                    try {
                        blocks[col] = converter.convert(sliced, blockFactory);
                    } finally {
                        sliced.close();
                    }
                }
            }

            batchOffset += rowsToRead;
            return new Page(blocks);
        }

        private static int findFieldIndex(Schema schema, String fieldName) {
            List<Field> fields = schema.getFields();
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).getName().equals(fieldName)) {
                    return i;
                }
            }
            return -1;
        }

        @Override
        public void close() throws IOException {
            try {
                reader.close();
            } finally {
                try {
                    stream.close();
                } finally {
                    allocator.close();
                }
            }
        }
    }
}
