/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

public class ParquetFormatReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    public void testFormatName() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        assertEquals("parquet", reader.formatName());
    }

    public void testFileExtensions() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        List<String> extensions = reader.fileExtensions();
        assertEquals(2, extensions.size());
        assertTrue(extensions.contains(".parquet"));
        assertTrue(extensions.contains(".parq"));
    }

    public void testReadSchemaFromSimpleParquet() throws Exception {
        // Create a simple parquet file with known schema
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("active")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("id", 1L);
            group1.add("name", "Alice");
            group1.add("age", 30);
            group1.add("active", true);
            return List.of(group1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        List<Attribute> attributes = metadata.schema();

        assertEquals(4, attributes.size());

        assertEquals("id", attributes.get(0).name());
        assertEquals(DataType.LONG, attributes.get(0).dataType());

        assertEquals("name", attributes.get(1).name());
        assertEquals(DataType.KEYWORD, attributes.get(1).dataType());

        assertEquals("age", attributes.get(2).name());
        assertEquals(DataType.INTEGER, attributes.get(2).dataType());

        assertEquals("active", attributes.get(3).name());
        assertEquals(DataType.BOOLEAN, attributes.get(3).dataType());
    }

    public void testReadDataFromSimpleParquet() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("id", 1L);
            group1.add("name", "Alice");
            group1.add("score", 95.5);

            Group group2 = factory.newGroup();
            group2.add("id", 2L);
            group2.add("name", "Bob");
            group2.add("score", 87.3);

            Group group3 = factory.newGroup();
            group3.add("id", 3L);
            group3.add("name", "Charlie");
            group3.add("score", 92.1);

            return List.of(group1, group2, group3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());
            assertEquals(3, page.getBlockCount());

            // Check first row
            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);

            // Check second row
            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(2)).getDouble(1), 0.001);

            // Check third row
            assertEquals(3L, ((LongBlock) page.getBlock(0)).getLong(2));
            assertEquals(new BytesRef("Charlie"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(2, new BytesRef()));
            assertEquals(92.1, ((DoubleBlock) page.getBlock(2)).getDouble(2), 0.001);

            assertFalse(iterator.hasNext());
        }
    }

    public void testReadWithColumnProjection() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("id", 1L);
            group1.add("name", "Alice");
            group1.add("score", 95.5);

            Group group2 = factory.newGroup();
            group2.add("id", 2L);
            group2.add("name", "Bob");
            group2.add("score", 87.3);

            return List.of(group1, group2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // Project only name and score columns
        try (CloseableIterator<Page> iterator = reader.read(storageObject, List.of("name", "score"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount()); // Only 2 projected columns

            // Check values - note: order matches projection order
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(1)).getDouble(0), 0.001);

            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(1)).getDouble(1), 0.001);
        }
    }

    public void testReadWithBatching() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new java.util.ArrayList<>();
            for (int i = 1; i <= 25; i++) {
                Group group = factory.newGroup();
                group.add("id", (long) i);
                group.add("value", i * 10);
                groups.add(group);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        int batchSize = 10;
        int totalRows = 0;

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, batchSize)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
            }
        }

        assertEquals(25, totalRows);
    }

    public void testReadBooleanColumn() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("active")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("id", 1L);
            group1.add("active", true);

            Group group2 = factory.newGroup();
            group2.add("id", 2L);
            group2.add("active", false);

            return List.of(group1, group2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());

            assertTrue(((BooleanBlock) page.getBlock(1)).getBoolean(0));
            assertFalse(((BooleanBlock) page.getBlock(1)).getBoolean(1));
        }
    }

    public void testReadIntegerColumn() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("count").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("count", 100);

            Group group2 = factory.newGroup();
            group2.add("count", 200);

            Group group3 = factory.newGroup();
            group3.add("count", 300);

            return List.of(group1, group2, group3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());

            assertEquals(100, ((IntBlock) page.getBlock(0)).getInt(0));
            assertEquals(200, ((IntBlock) page.getBlock(0)).getInt(1));
            assertEquals(300, ((IntBlock) page.getBlock(0)).getInt(2));
        }
    }

    public void testReadFloatColumn() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.FLOAT).named("temperature").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group1 = factory.newGroup();
            group1.add("temperature", 98.6f);

            Group group2 = factory.newGroup();
            group2.add("temperature", 37.0f);

            return List.of(group1, group2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());

            // Float is converted to double
            assertEquals(98.6, ((DoubleBlock) page.getBlock(0)).getDouble(0), 0.1);
            assertEquals(37.0, ((DoubleBlock) page.getBlock(0)).getDouble(1), 0.1);
        }
    }

    public void testMetadataReturnsCorrectSourceType() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group = factory.newGroup();
            group.add("id", 1L);
            return List.of(group);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals("parquet", metadata.sourceType());
    }

    @FunctionalInterface
    private interface GroupCreator {
        List<Group> create(SimpleGroupFactory factory);
    }

    private byte[] createParquetFile(MessageType schema, GroupCreator groupCreator) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        OutputFile outputFile = new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) throws IOException {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() throws IOException {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        outputStream.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        outputStream.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        outputStream.close();
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://test.parquet";
            }
        };

        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        List<Group> groups = groupCreator.create(groupFactory);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {

            for (Group group : groups) {
                writer.write(group);
            }
        }

        return outputStream.toByteArray();
    }

    private StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
                return new ByteArrayInputStream(data, pos, len);
            }

            @Override
            public long length() throws IOException {
                return data.length;
            }

            @Override
            public Instant lastModified() throws IOException {
                return Instant.now();
            }

            @Override
            public boolean exists() throws IOException {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://test.parquet");
            }
        };
    }
}
