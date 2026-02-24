/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
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
 * {@link FormatReader} implementation for Apache ORC files.
 *
 * <p>Uses ORC's vectorized reader ({@link VectorizedRowBatch}) which maps naturally to
 * ESQL's columnar {@link Block}/{@link Page} model. Each ORC {@link ColumnVector} is
 * converted directly to the corresponding ESQL Block type.
 *
 * <p>Key features:
 * <ul>
 *   <li>Works with any StorageProvider (HTTP, S3, local) via {@link OrcStorageObjectAdapter}</li>
 *   <li>Efficient columnar reading with column projection</li>
 *   <li>Direct conversion from ORC VectorizedRowBatch to ESQL Page</li>
 * </ul>
 */
public class OrcFormatReader implements FormatReader {

    private static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();

    private final BlockFactory blockFactory;

    public OrcFormatReader(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    @Override
    public SourceMetadata metadata(StorageObject object) throws IOException {
        List<Attribute> schema = readSchema(object);
        return new SimpleSourceMetadata(schema, formatName(), object.path().toString());
    }

    private static List<Attribute> readSchema(StorageObject object) throws IOException {
        OrcStorageObjectAdapter fs = new OrcStorageObjectAdapter(object);
        Path path = new Path(object.path().toString());
        OrcFile.ReaderOptions options = OrcFile.readerOptions(new Configuration(false)).filesystem(fs);
        try (Reader reader = OrcFile.createReader(path, options)) {
            TypeDescription schema = reader.getSchema();
            return convertOrcSchemaToAttributes(schema);
        }
    }

    @Override
    public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) throws IOException {
        OrcStorageObjectAdapter fs = new OrcStorageObjectAdapter(object);
        Path path = new Path(object.path().toString());
        OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(new Configuration(false)).filesystem(fs);
        Reader reader = OrcFile.createReader(path, readerOptions);

        TypeDescription schema = reader.getSchema();
        List<Attribute> attributes = convertOrcSchemaToAttributes(schema);

        List<Attribute> projectedAttributes;
        boolean[] include = null;
        if (projectedColumns == null || projectedColumns.isEmpty()) {
            projectedAttributes = attributes;
        } else {
            projectedAttributes = new ArrayList<>();
            Map<String, Integer> nameToIndex = new HashMap<>();
            List<String> fieldNames = schema.getFieldNames();
            for (int i = 0; i < fieldNames.size(); i++) {
                nameToIndex.put(fieldNames.get(i), i);
            }
            // ORC include array: index 0 is the root struct, then 1..N for each field
            include = new boolean[schema.getMaximumId() + 1];
            include[0] = true;
            Map<String, Attribute> attributeMap = new HashMap<>();
            for (Attribute attr : attributes) {
                attributeMap.put(attr.name(), attr);
            }
            for (String columnName : projectedColumns) {
                Attribute attr = attributeMap.get(columnName);
                if (attr != null) {
                    projectedAttributes.add(attr);
                    Integer idx = nameToIndex.get(columnName);
                    if (idx != null) {
                        TypeDescription child = schema.getChildren().get(idx);
                        include[child.getId()] = true;
                    }
                } else {
                    projectedAttributes.add(new ReferenceAttribute(Source.EMPTY, columnName, DataType.NULL));
                }
            }
        }

        Reader.Options readOptions = reader.options().rowBatchSize(batchSize);
        if (include != null) {
            readOptions.include(include);
        }
        RecordReader rows = reader.rows(readOptions);

        return new OrcPageIterator(reader, rows, schema, projectedAttributes, batchSize, blockFactory);
    }

    @Override
    public String formatName() {
        return "orc";
    }

    @Override
    public List<String> fileExtensions() {
        return List.of(".orc");
    }

    @Override
    public void close() throws IOException {
        // No resources to close at the reader level
    }

    private static List<Attribute> convertOrcSchemaToAttributes(TypeDescription schema) {
        List<Attribute> attributes = new ArrayList<>();
        List<String> fieldNames = schema.getFieldNames();
        List<TypeDescription> children = schema.getChildren();
        for (int i = 0; i < fieldNames.size(); i++) {
            String name = fieldNames.get(i);
            DataType esqlType = convertOrcTypeToEsql(children.get(i));
            attributes.add(new ReferenceAttribute(Source.EMPTY, name, esqlType));
        }
        return attributes;
    }

    private static DataType convertOrcTypeToEsql(TypeDescription orcType) {
        return switch (orcType.getCategory()) {
            case BOOLEAN -> DataType.BOOLEAN;
            case BYTE, SHORT, INT -> DataType.INTEGER;
            case LONG -> DataType.LONG;
            case FLOAT, DOUBLE -> DataType.DOUBLE;
            case STRING, VARCHAR, CHAR -> DataType.KEYWORD;
            case BINARY -> DataType.KEYWORD;
            case TIMESTAMP, TIMESTAMP_INSTANT, DATE -> DataType.DATETIME;
            case DECIMAL -> DataType.DOUBLE;
            default -> DataType.UNSUPPORTED;
        };
    }

    private static class OrcPageIterator implements CloseableIterator<Page> {
        private final Reader reader;
        private final RecordReader rows;
        private final TypeDescription schema;
        private final List<Attribute> attributes;
        private final BlockFactory blockFactory;
        private final VectorizedRowBatch batch;
        private boolean exhausted = false;
        private boolean batchReady = false;
        private final Map<String, Integer> fieldNameToIndex;

        OrcPageIterator(
            Reader reader,
            RecordReader rows,
            TypeDescription schema,
            List<Attribute> attributes,
            int batchSize,
            BlockFactory blockFactory
        ) {
            this.reader = reader;
            this.rows = rows;
            this.schema = schema;
            this.attributes = attributes;
            this.blockFactory = blockFactory;
            this.batch = schema.createRowBatch(batchSize);

            fieldNameToIndex = new HashMap<>(schema.getFieldNames().size());
            int i = 0;
            for (var fieldName : schema.getFieldNames()) {
                fieldNameToIndex.put(fieldName, i++);
            }
        }

        @Override
        public boolean hasNext() {
            if (exhausted) {
                return false;
            }
            if (batchReady) {
                return true;
            }
            try {
                if (rows.nextBatch(batch)) {
                    batchReady = true;
                    return true;
                } else {
                    exhausted = true;
                    return false;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read ORC batch", e);
            }
        }

        @Override
        public Page next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            batchReady = false;
            return convertToPage();
        }

        private Page convertToPage() {
            int rowCount = batch.size;
            Block[] blocks = new Block[attributes.size()];

            for (int col = 0; col < attributes.size(); col++) {
                Attribute attribute = attributes.get(col);
                String fieldName = attribute.name();
                DataType dataType = attribute.dataType();

                try {
                    var fieldIndex = fieldNameToIndex.get(fieldName);
                    if (fieldIndex == null) {
                        blocks[col] = blockFactory.newConstantNullBlock(rowCount);
                    } else {
                        ColumnVector vector = batch.cols[fieldIndex];
                        blocks[col] = createBlock(vector, dataType, rowCount);
                    }
                } catch (Exception e) {
                    Releasables.closeExpectNoException(blocks);
                    throw e;
                }
            }

            return new Page(blocks);
        }

        private Block createBlock(ColumnVector vector, DataType dataType, int rowCount) {
            return switch (dataType) {
                case BOOLEAN -> createBooleanBlock((LongColumnVector) vector, rowCount);
                case INTEGER -> createIntBlock((LongColumnVector) vector, rowCount);
                case LONG -> createLongBlock((LongColumnVector) vector, rowCount);
                case DOUBLE -> createDoubleBlock(vector, rowCount);
                case KEYWORD, TEXT -> createBytesRefBlock(vector, rowCount);
                case DATETIME -> createDatetimeBlock(vector, rowCount);
                default -> blockFactory.newConstantNullBlock(rowCount);
            };
        }

        private Block createBooleanBlock(LongColumnVector vector, int rowCount) {
            try (var builder = blockFactory.newBooleanBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (vector.noNulls == false && vector.isNull[i]) {
                        builder.appendNull();
                    } else {
                        int idx = vector.isRepeating ? 0 : i;
                        builder.appendBoolean(vector.vector[idx] != 0);
                    }
                }
                return builder.build();
            }
        }

        private Block createIntBlock(LongColumnVector vector, int rowCount) {
            try (var builder = blockFactory.newIntBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (vector.noNulls == false && vector.isNull[i]) {
                        builder.appendNull();
                    } else {
                        int idx = vector.isRepeating ? 0 : i;
                        builder.appendInt((int) vector.vector[idx]);
                    }
                }
                return builder.build();
            }
        }

        private Block createLongBlock(LongColumnVector vector, int rowCount) {
            try (var builder = blockFactory.newLongBlockBuilder(rowCount)) {
                for (int i = 0; i < rowCount; i++) {
                    if (vector.noNulls == false && vector.isNull[i]) {
                        builder.appendNull();
                    } else {
                        int idx = vector.isRepeating ? 0 : i;
                        builder.appendLong(vector.vector[idx]);
                    }
                }
                return builder.build();
            }
        }

        private Block createDoubleBlock(ColumnVector vector, int rowCount) {
            try (var builder = blockFactory.newDoubleBlockBuilder(rowCount)) {
                if (vector instanceof DoubleColumnVector doubleVector) {
                    for (int i = 0; i < rowCount; i++) {
                        if (doubleVector.noNulls == false && doubleVector.isNull[i]) {
                            builder.appendNull();
                        } else {
                            int idx = doubleVector.isRepeating ? 0 : i;
                            builder.appendDouble(doubleVector.vector[idx]);
                        }
                    }
                } else if (vector instanceof LongColumnVector longVector) {
                    // DECIMAL types may come as LongColumnVector
                    for (int i = 0; i < rowCount; i++) {
                        if (longVector.noNulls == false && longVector.isNull[i]) {
                            builder.appendNull();
                        } else {
                            int idx = longVector.isRepeating ? 0 : i;
                            builder.appendDouble(longVector.vector[idx]);
                        }
                    }
                } else {
                    throw new QlIllegalArgumentException("Unsupported column type: " + vector.getClass().getSimpleName());
                }
                return builder.build();
            }
        }

        private Block createBytesRefBlock(ColumnVector vector, int rowCount) {
            try (var builder = blockFactory.newBytesRefBlockBuilder(rowCount)) {
                Check.isTrue(vector instanceof BytesColumnVector, "Unsupported column type: " + vector.getClass().getSimpleName());
                BytesColumnVector bytesVector = (BytesColumnVector) vector;
                for (int i = 0; i < rowCount; i++) {
                    if (bytesVector.noNulls == false && bytesVector.isNull[i]) {
                        builder.appendNull();
                    } else {
                        int idx = bytesVector.isRepeating ? 0 : i;
                        byte[] src = bytesVector.vector[idx];
                        int start = bytesVector.start[idx];
                        int len = bytesVector.length[idx];
                        builder.appendBytesRef(new org.apache.lucene.util.BytesRef(src, start, len));
                    }
                }
                return builder.build();
            }
        }

        private Block createDatetimeBlock(ColumnVector vector, int rowCount) {
            try (var builder = blockFactory.newLongBlockBuilder(rowCount)) {
                if (vector instanceof TimestampColumnVector tsVector) {
                    for (int i = 0; i < rowCount; i++) {
                        if (tsVector.noNulls == false && tsVector.isNull[i]) {
                            builder.appendNull();
                        } else {
                            int idx = tsVector.isRepeating ? 0 : i;
                            builder.appendLong(tsVector.getTime(idx));
                        }
                    }
                } else if (vector instanceof LongColumnVector longVector) {
                    // DATE type uses LongColumnVector with days since epoch
                    for (int i = 0; i < rowCount; i++) {
                        if (longVector.noNulls == false && longVector.isNull[i]) {
                            builder.appendNull();
                        } else {
                            int idx = longVector.isRepeating ? 0 : i;
                            builder.appendLong(longVector.vector[idx] * MILLIS_PER_DAY);
                        }
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() throws IOException {
            try {
                rows.close();
            } finally {
                reader.close();
            }
        }
    }
}
