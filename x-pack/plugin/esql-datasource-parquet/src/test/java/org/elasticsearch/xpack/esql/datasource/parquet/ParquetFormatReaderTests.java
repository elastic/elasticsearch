/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class ParquetFormatReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
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

    public void testCircuitBreaker() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            var groups = new ArrayList<Group>();
            for (int i = 0; i < 1000; i++) {
                Group group = factory.newGroup();
                group.add("id", (long) i);
                group.add("score", i * 1.5);
                groups.add(group);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        {
            var limitedFactory = new BlockFactory(new LimitedBreaker("test", ByteSizeValue.ofBytes(100)), this.blockFactory.bigArrays());
            var reader = new ParquetFormatReader(limitedFactory);

            // Read the schema without creating any ESQL block. This is enough to trip the breaker.
            assertThrows(CircuitBreakingException.class, () -> reader.metadata(storageObject));

            // Sanity check
            assertEquals(0, limitedFactory.breaker().getUsed());
        }

        {
            var limitedFactory = new BlockFactory(new LimitedBreaker("test", ByteSizeValue.ofBytes(1000)), this.blockFactory.bigArrays());
            var reader = new ParquetFormatReader(limitedFactory);

            // Read the schema is now ok
            var metadata = reader.metadata(storageObject);
            assertEquals(0, limitedFactory.breaker().getUsed());

            // Reading a page trips the breaker
            assertThrows(CircuitBreakingException.class, () -> {
                try (var iter = reader.read(storageObject, List.of("id", "score"), 1000)) {
                    iter.next();
                }
            });
            reader.close();
            assertEquals(0, limitedFactory.breaker().getUsed());
        }
    }

    public void testCircuitBreakerTripsOnLargerRowGroup() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("test_schema");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        // Write rows with increasing payload sizes so that the Parquet writer produces row groups
        // of increasing byte size when it flushes at the 1 KB row-group threshold.
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(1024L)
                .withPageSize(512)
                .build()
        ) {
            for (int i = 0; i < 1000; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                // Payload grows with the row index so later row groups contain heavier rows
                g.add("payload", "x".repeat(10 + i));
                writer.write(g);
            }
        }
        byte[] parquetData = outputStream.toByteArray();
        assertThat(parquetData.length, greaterThan(2 * 1024)); // sanity: file has multiple row groups

        StorageObject storageObject = createStorageObject(parquetData);

        // Set the breaker limit so that the first (smallest) row groups can be read,
        // but some later larger one trips the breaker.
        var limitedBreaker = new LimitedBreaker("test", ByteSizeValue.ofKb(32));
        var limitedFactory = new BlockFactory(limitedBreaker, this.blockFactory.bigArrays());

        var pageCount = new AtomicInteger(); // mutable int holder

        try (
            var reader = new ParquetFormatReader(limitedFactory);
            var iter = reader.read(storageObject, List.of("id", "payload"), 1_000_000)
        ) {
            expectThrows(CircuitBreakingException.class, () -> {
                while (iter.hasNext()) {
                    var page = iter.next();
                    page.close();
                    pageCount.incrementAndGet();
                }
            });
        }

        // Check that we read at least 1 page and that all memory has been released
        assertThat(pageCount.get(), greaterThan(1));
        assertEquals(0, limitedBreaker.getUsed());
    }

    public void testProjectedColumnMissingFromFileReturnsNullBlock() throws Exception {
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

        try (CloseableIterator<Page> iterator = reader.read(storageObject, List.of("id", "nonexistent", "score"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(3, page.getBlockCount());

            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertTrue(page.getBlock(1).isNull(0));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);

            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertTrue(page.getBlock(1).isNull(1));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(2)).getDouble(1), 0.001);
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
            List<Group> groups = new ArrayList<>();
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

    public void testReadWithRowLimit() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                Group group = factory.newGroup();
                group.add("id", (long) i);
                group.add("value", i * 10);
                groups.add(group);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // Read with a row limit of 10 — Parquet reader respects the budget natively
        try (
            CloseableIterator<Page> iterator = reader.read(storageObject, FormatReadContext.builder().batchSize(50).rowLimit(10).build())
        ) {
            int totalRows = 0;
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
            }
            assertEquals(10, totalRows);
        }
    }

    public void testReadWithRowLimitNoLimit() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 1; i <= 25; i++) {
                Group group = factory.newGroup();
                group.add("id", (long) i);
                groups.add(group);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // NO_LIMIT should read all rows
        try (CloseableIterator<Page> iterator = reader.read(storageObject, FormatReadContext.of(null, 10))) {
            int totalRows = 0;
            while (iterator.hasNext()) {
                totalRows += iterator.next().getPositionCount();
            }
            assertEquals(25, totalRows);
        }
    }

    public void testReadWithColumnProjectionAndLimit() throws Exception {
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
            List<Group> groups = new ArrayList<>();
            for (int i = 1; i <= 50; i++) {
                Group group = factory.newGroup();
                group.add("id", (long) i);
                group.add("name", "name_" + i);
                group.add("score", i * 1.5);
                groups.add(group);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        // Project only "name" column with limit of 5
        try (CloseableIterator<Page> iterator = reader.read(storageObject, FormatReadContext.of(List.of("name"), 10).withRowLimit(5))) {
            int totalRows = 0;
            int totalBlocks = 0;
            while (iterator.hasNext()) {
                Page page = iterator.next();
                totalRows += page.getPositionCount();
                totalBlocks = page.getBlockCount();
            }
            assertEquals(1, totalBlocks); // Only 1 projected column
            assertEquals(5, totalRows);
        }
    }

    public void testReadOptionalColumnsWithNulls() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("active")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("id", 1L);
            g1.add("name", "Alice");
            g1.add("age", 30);
            g1.add("score", 95.5);
            g1.add("active", true);

            Group g2 = factory.newGroup();
            g2.add("id", 2L);
            // name, age, score, active are all null

            Group g3 = factory.newGroup();
            g3.add("id", 3L);
            g3.add("name", "Charlie");
            // age is null
            g3.add("score", 88.0);
            g3.add("active", false);

            return List.of(g1, g2, g3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());
            assertEquals(5, page.getBlockCount());

            LongBlock idBlock = (LongBlock) page.getBlock(0);
            assertEquals(1L, idBlock.getLong(0));
            assertEquals(2L, idBlock.getLong(1));
            assertEquals(3L, idBlock.getLong(2));

            // name: "Alice", null, "Charlie"
            BytesRefBlock nameBlock = (BytesRefBlock) page.getBlock(1);
            assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
            assertTrue(nameBlock.isNull(1));
            assertEquals(new BytesRef("Charlie"), nameBlock.getBytesRef(2, new BytesRef()));

            // age: 30, null, null
            IntBlock ageBlock = (IntBlock) page.getBlock(2);
            assertFalse(ageBlock.isNull(0));
            assertEquals(30, ageBlock.getInt(ageBlock.getFirstValueIndex(0)));
            assertTrue(ageBlock.isNull(1));
            assertTrue(ageBlock.isNull(2));

            // score: 95.5, null, 88.0
            DoubleBlock scoreBlock = (DoubleBlock) page.getBlock(3);
            assertFalse(scoreBlock.isNull(0));
            assertEquals(95.5, scoreBlock.getDouble(scoreBlock.getFirstValueIndex(0)), 0.001);
            assertTrue(scoreBlock.isNull(1));
            assertFalse(scoreBlock.isNull(2));
            assertEquals(88.0, scoreBlock.getDouble(scoreBlock.getFirstValueIndex(2)), 0.001);

            // active: true, null, false
            BooleanBlock activeBlock = (BooleanBlock) page.getBlock(4);
            assertFalse(activeBlock.isNull(0));
            assertTrue(activeBlock.getBoolean(activeBlock.getFirstValueIndex(0)));
            assertTrue(activeBlock.isNull(1));
            assertFalse(activeBlock.isNull(2));
            assertFalse(activeBlock.getBoolean(activeBlock.getFirstValueIndex(2)));

            assertFalse(iterator.hasNext());
        }
    }

    public void testReadOptionalLongWithNulls() throws Exception {
        MessageType schema = Types.buildMessage().optional(PrimitiveType.PrimitiveTypeName.INT64).named("value").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("value", 100L);
            Group g2 = factory.newGroup();
            // value is null
            Group g3 = factory.newGroup();
            g3.add("value", 300L);
            return List.of(g1, g2, g3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());
            LongBlock block = (LongBlock) page.getBlock(0);
            assertFalse(block.isNull(0));
            assertEquals(100L, block.getLong(block.getFirstValueIndex(0)));
            assertTrue(block.isNull(1));
            assertFalse(block.isNull(2));
            assertEquals(300L, block.getLong(block.getFirstValueIndex(2)));
        }
    }

    // --- DECIMAL tests ---

    public void testReadDecimalInt32Column() throws Exception {
        // DECIMAL(9, 2) backed by INT32: value 12345 represents 123.45
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(2, 9))
            .named("price")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("price", 12345); // 123.45
            Group g2 = factory.newGroup();
            g2.add("price", -9900); // -99.00
            Group g3 = factory.newGroup();
            g3.add("price", 0);
            return List.of(g1, g2, g3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DOUBLE, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(123.45, block.getDouble(0), 0.001);
            assertEquals(-99.00, block.getDouble(1), 0.001);
            assertEquals(0.0, block.getDouble(2), 0.001);
        }
    }

    public void testReadDecimalInt64Column() throws Exception {
        // DECIMAL(18, 4) backed by INT64: value 123456789 represents 12345.6789
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.decimalType(4, 18))
            .named("amount")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("amount", 123456789L); // 12345.6789
            Group g2 = factory.newGroup();
            g2.add("amount", -50000L); // -5.0000
            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DOUBLE, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(12345.6789, block.getDouble(0), 0.0001);
            assertEquals(-5.0, block.getDouble(1), 0.0001);
        }
    }

    public void testReadDecimalFixedLenColumn() throws Exception {
        // DECIMAL(10, 2) backed by FIXED_LEN_BYTE_ARRAY(8)
        int fixedLen = 8;
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(fixedLen)
            .as(LogicalTypeAnnotation.decimalType(2, 10))
            .named("total")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("total", Binary.fromConstantByteArray(toFixedLenDecimal(1234567, fixedLen))); // 12345.67
            Group g2 = factory.newGroup();
            g2.add("total", Binary.fromConstantByteArray(toFixedLenDecimal(-100, fixedLen))); // -1.00
            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(12345.67, block.getDouble(0), 0.01);
            assertEquals(-1.00, block.getDouble(1), 0.01);
        }
    }

    public void testReadDecimalBinaryColumn() throws Exception {
        // DECIMAL(10, 2) backed by BINARY
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.decimalType(2, 10))
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("value", Binary.fromConstantByteArray(BigInteger.valueOf(9999).toByteArray())); // 99.99
            return List.of(g1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(99.99, block.getDouble(0), 0.01);
        }
    }

    // --- TIMESTAMP MICROS/NANOS tests ---

    public void testReadTimestampMicrosColumn() throws Exception {
        // TIMESTAMP(MICROS, adjustedToUTC=true)
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .named("test_schema");

        long epochMillis = 946728000000L; // 2000-01-01T12:00:00Z
        long epochMicros = epochMillis * 1000;

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("ts", epochMicros);
            return List.of(g1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DATETIME, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals(epochMillis, block.getLong(0));
        }
    }

    public void testReadTimestampNanosColumn() throws Exception {
        // TIMESTAMP(NANOS, adjustedToUTC=true)
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("ts")
            .named("test_schema");

        long epochMillis = 946728000000L; // 2000-01-01T12:00:00Z
        long epochNanos = epochMillis * 1_000_000;

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("ts", epochNanos);
            return List.of(g1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals(epochMillis, block.getLong(0));
        }
    }

    public void testReadTimestampMillisColumn() throws Exception {
        // TIMESTAMP(MILLIS, adjustedToUTC=true) — existing behavior, verifying it still works
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts")
            .named("test_schema");

        long epochMillis = 946728000000L;

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("ts", epochMillis);
            return List.of(g1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals(epochMillis, block.getLong(0));
        }
    }

    // --- INT96 timestamp tests ---

    public void testReadInt96TimestampColumn() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT96).named("ts").named("test_schema");

        // 2000-01-01T12:00:00Z → Julian day 2451545, nanos = 12h = 43_200_000_000_000
        int julianDay = 2_451_545;
        long nanosOfDay = 43_200_000_000_000L;
        long expectedMillis = 946728000000L;

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("ts", new NanoTime(julianDay, nanosOfDay));
            return List.of(g1);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DATETIME, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            LongBlock block = (LongBlock) page.getBlock(0);
            assertEquals(expectedMillis, block.getLong(0));
        }
    }

    // --- FLOAT16 tests ---

    public void testReadFloat16Column() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(2)
            .as(LogicalTypeAnnotation.float16Type())
            .named("val")
            .named("test_schema");

        float value1 = 3.14f;
        float value2 = -1.0f;
        float value3 = 0.0f;

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("val", Binary.fromConstantByteArray(toFloat16Bytes(value1)));
            Group g2 = factory.newGroup();
            g2.add("val", Binary.fromConstantByteArray(toFloat16Bytes(value2)));
            Group g3 = factory.newGroup();
            g3.add("val", Binary.fromConstantByteArray(toFloat16Bytes(value3)));
            return List.of(g1, g2, g3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.DOUBLE, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());
            DoubleBlock block = (DoubleBlock) page.getBlock(0);
            assertEquals(Float.float16ToFloat(Float.floatToFloat16(value1)), block.getDouble(0), 0.01);
            assertEquals(Float.float16ToFloat(Float.floatToFloat16(value2)), block.getDouble(1), 0.001);
            assertEquals(0.0, block.getDouble(2), 0.001);
        }
    }

    // --- UUID tests ---

    public void testReadUuidColumn() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(16)
            .as(LogicalTypeAnnotation.uuidType())
            .named("id")
            .named("test_schema");

        UUID uuid1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000000");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            g1.add("id", Binary.fromConstantByteArray(toUuidBytes(uuid1)));
            Group g2 = factory.newGroup();
            g2.add("id", Binary.fromConstantByteArray(toUuidBytes(uuid2)));
            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.KEYWORD, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            assertEquals(uuid1.toString(), block.getBytesRef(0, new BytesRef()).utf8ToString());
            assertEquals(uuid2.toString(), block.getBytesRef(1, new BytesRef()).utf8ToString());
        }
    }

    // --- LIST tests ---

    public void testReadListOfIntegersColumn() throws Exception {
        Type listType = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT32).named("numbers");
        MessageType schema = new MessageType("test_schema", listType);

        byte[] parquetData = createParquetFile(schema, factory -> {
            // Row 0: [1, 2, 3]
            Group g1 = factory.newGroup();
            Group list1 = g1.addGroup("numbers");
            list1.addGroup("list").append("element", 1);
            list1.addGroup("list").append("element", 2);
            list1.addGroup("list").append("element", 3);

            // Row 1: [10]
            Group g2 = factory.newGroup();
            Group list2 = g2.addGroup("numbers");
            list2.addGroup("list").append("element", 10);

            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.INTEGER, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());

            IntBlock block = (IntBlock) page.getBlock(0);
            // Row 0: [1, 2, 3]
            assertEquals(3, block.getValueCount(0));
            int start0 = block.getFirstValueIndex(0);
            assertEquals(1, block.getInt(start0));
            assertEquals(2, block.getInt(start0 + 1));
            assertEquals(3, block.getInt(start0 + 2));

            // Row 1: [10]
            assertEquals(1, block.getValueCount(1));
            assertEquals(10, block.getInt(block.getFirstValueIndex(1)));
        }
    }

    public void testReadListOfStringsColumn() throws Exception {
        Type listType = Types.optionalList()
            .optionalElement(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("tags");
        MessageType schema = new MessageType("test_schema", listType);

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g1 = factory.newGroup();
            Group list1 = g1.addGroup("tags");
            list1.addGroup("list").append("element", "red");
            list1.addGroup("list").append("element", "blue");

            Group g2 = factory.newGroup();
            Group list2 = g2.addGroup("tags");
            list2.addGroup("list").append("element", "green");

            return List.of(g1, g2);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals(DataType.KEYWORD, metadata.schema().get(0).dataType());

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());

            BytesRefBlock block = (BytesRefBlock) page.getBlock(0);
            // Row 0: ["red", "blue"]
            assertEquals(2, block.getValueCount(0));
            int start0 = block.getFirstValueIndex(0);
            assertEquals(new BytesRef("red"), block.getBytesRef(start0, new BytesRef()));
            assertEquals(new BytesRef("blue"), block.getBytesRef(start0 + 1, new BytesRef()));

            // Row 1: ["green"]
            assertEquals(1, block.getValueCount(1));
            assertEquals(new BytesRef("green"), block.getBytesRef(block.getFirstValueIndex(1), new BytesRef()));
        }
    }

    public void testReadListWithNullList() throws Exception {
        Type listType = Types.optionalList().optionalElement(PrimitiveType.PrimitiveTypeName.INT64).named("values");
        MessageType schema = new MessageType("test_schema", listType);

        byte[] parquetData = createParquetFile(schema, factory -> {
            // Row 0: [100, 200]
            Group g1 = factory.newGroup();
            Group list1 = g1.addGroup("values");
            list1.addGroup("list").append("element", 100L);
            list1.addGroup("list").append("element", 200L);

            // Row 1: null (no addGroup call)
            Group g2 = factory.newGroup();

            // Row 2: [300]
            Group g3 = factory.newGroup();
            Group list3 = g3.addGroup("values");
            list3.addGroup("list").append("element", 300L);

            return List.of(g1, g2, g3);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(3, page.getPositionCount());

            LongBlock block = (LongBlock) page.getBlock(0);
            // Row 0: [100, 200]
            assertFalse(block.isNull(0));
            assertEquals(2, block.getValueCount(0));
            int start0 = block.getFirstValueIndex(0);
            assertEquals(100L, block.getLong(start0));
            assertEquals(200L, block.getLong(start0 + 1));

            // Row 1: null
            assertTrue(block.isNull(1));

            // Row 2: [300]
            assertFalse(block.isNull(2));
            assertEquals(1, block.getValueCount(2));
            assertEquals(300L, block.getLong(block.getFirstValueIndex(2)));
        }
    }

    // --- UUID formatting unit test ---

    public void testFormatUuid() {
        UUID uuid = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        byte[] bytes = toUuidBytes(uuid);
        String formatted = ParquetFormatReader.formatUuid(bytes);
        assertEquals("550e8400-e29b-41d4-a716-446655440000", formatted);
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

    public void testStatisticsSurviveEmbedding() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("score")
            .named("stats_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("age", 20 + (i % 60));
                g.add("score", (long) (i * 10));
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertTrue("statistics() should be present", metadata.statistics().isPresent());

        var stats = metadata.statistics().get();
        assertTrue("Row count should be present", stats.rowCount().isPresent());
        assertEquals(100L, stats.rowCount().getAsLong());

        var enriched = org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.embedStatistics(
            metadata.sourceMetadata(),
            stats
        );

        assertEquals(100L, enriched.get("_stats.row_count"));
        assertEquals(0L, enriched.get("_stats.columns.age.null_count"));
        assertEquals(20, enriched.get("_stats.columns.age.min"));
        assertEquals(79, enriched.get("_stats.columns.age.max"));
        assertEquals(0L, enriched.get("_stats.columns.score.null_count"));
        assertNotNull("Score min should be present", enriched.get("_stats.columns.score.min"));
        assertNotNull("Score max should be present", enriched.get("_stats.columns.score.max"));
    }

    public void testStatisticsForStringColumnsAreJdkTypes() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("city")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("pop")
            .named("string_stats_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (String name : List.of("alpha", "bravo", "charlie", "delta")) {
                Group g = factory.newGroup();
                g.add("city", name);
                g.add("pop", 1000);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(storageObject);

        var stats = metadata.statistics().orElseThrow();
        var enriched = org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer.embedStatistics(
            metadata.sourceMetadata(),
            stats
        );

        Object minCity = enriched.get("_stats.columns.city.min");
        Object maxCity = enriched.get("_stats.columns.city.max");
        assertNotNull("city min should be present", minCity);
        assertNotNull("city max should be present", maxCity);
        assertThat("min must be a JDK String, not Parquet Binary", minCity, instanceOf(String.class));
        assertThat("max must be a JDK String, not Parquet Binary", maxCity, instanceOf(String.class));
        assertEquals("alpha", minCity);
        assertEquals("delta", maxCity);

        // Also verify per-split stats if we can force multi-row-group
        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            for (Map.Entry<String, Object> entry : range.statistics().entrySet()) {
                assertFalse("Split stat value must not be a Parquet Binary: " + entry.getKey(), entry.getValue() instanceof Binary);
            }
        }
    }

    public void testDiscoverSplitRangesMultipleRowGroups() throws Exception {
        byte[] parquetData = createWideMultiRowGroupFile(500);

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Expected multiple ranges for multi-row-group file, got " + ranges.size(), ranges.size() > 1);

        for (RangeAwareFormatReader.SplitRange range : ranges) {
            assertTrue("Range offset must be non-negative", range.offset() >= 0);
            assertTrue("Range length must be positive", range.length() > 0);
            assertNotNull("Per-row-group statistics should be present", range.statistics());
            assertTrue("Statistics should contain row count", range.statistics().containsKey("_stats.row_count"));
        }
    }

    public void testDiscoverSplitRangesSingleRowGroup() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            Group group = factory.newGroup();
            group.add("id", 1L);
            return List.of(group);
        });

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Single row group file should return empty ranges", ranges.isEmpty());
    }

    public void testInvalidParquetOpenGarbageIncludesUriInMessage() throws Exception {
        byte[] garbage = new byte[64];
        Arrays.fill(garbage, (byte) 0x5a);
        StorageObject storageObject = createStorageObject(garbage, "s3://bucket/path/file.parquet");
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        IOException ex = expectThrows(IOException.class, () -> reader.metadata(storageObject));
        assertThat(
            ex.getMessage(),
            allOf(
                containsString("Could not read [s3://bucket/path/file.parquet] as a Parquet file"),
                containsString("is not a Parquet file. Expected magic number at tail, but found [")
            )
        );
    }

    public void testInvalidParquetOpenEmptyFile() throws Exception {
        StorageObject storageObject = createStorageObject(new byte[0], "memory://empty.parquet");
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        IOException ex = expectThrows(IOException.class, () -> reader.metadata(storageObject));
        assertThat(
            ex.getMessage(),
            allOf(
                containsString("Could not read [memory://empty.parquet] as a Parquet file:"),
                containsString("is not a Parquet file (length is too low: 0)")
            )
        );
    }

    public void testInvalidParquetOpenTruncated() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");
        byte[] full = createParquetFile(schema, factory -> {
            Group group = factory.newGroup();
            group.add("id", 1L);
            return List.of(group);
        });
        byte[] truncated = Arrays.copyOf(full, Math.max(1, full.length / 8));
        StorageObject storageObject = createStorageObject(truncated, "https://host/obj.parquet");
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        IOException ex = expectThrows(IOException.class, () -> reader.metadata(storageObject));
        assertTrue(ex.getMessage(), ex.getMessage().contains("https://host/obj.parquet"));
    }

    public void testValidParquetZeroRowsMetadata() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> List.of());
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals("id", metadata.schema().get(0).name());
    }

    /**
     * Planner type LONG (widened across globbed files) with INT32 in this file must still decode:
     * widening matches {@link org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter#commonType}.
     */
    public void testPlannerLongCompatibleWithInt32InFileReadRange() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("x").named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", 42);
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerTypes = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.LONG));
        try (
            CloseableIterator<Page> iterator = reader.readRange(
                storageObject,
                List.of("x"),
                100,
                0,
                parquetData.length,
                plannerTypes,
                ErrorPolicy.STRICT
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertEquals(42L, ((LongBlock) page.getBlock(0)).getLong(0));
        }
    }

    /**
     * Parquet string-annotated BINARY maps to KEYWORD; planner KEYWORD is still readable (both ESQL strings).
     */
    public void testPlannerKeywordCompatibleWithTextParquetColumnReadRange() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("x")
            .named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", "hello");
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader r = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerTypes = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.KEYWORD));
        try (
            CloseableIterator<Page> it = r.readRange(
                storageObject,
                List.of("x"),
                10,
                0,
                parquetData.length,
                plannerTypes,
                ErrorPolicy.STRICT
            )
        ) {
            Page page = it.next();
            assertEquals(1, page.getPositionCount());
            assertFalse(page.getBlock(0).isNull(0));
            assertEquals(new BytesRef("hello"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
        }
    }

    public void testSchemaMismatchInt32VsKeywordReturnsNullsOnReadRange() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT32).named("x").named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", 42);
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData, "s3://b/mismatch1.parquet");
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerTypes = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.KEYWORD));
        try (
            CloseableIterator<Page> iterator = reader.readRange(
                storageObject,
                List.of("x"),
                100,
                0,
                parquetData.length,
                plannerTypes,
                ErrorPolicy.STRICT
            )
        ) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(1, page.getPositionCount());
            assertTrue(page.getBlock(0).isNull(0));
        }
    }

    public void testSchemaMismatchStringVsLongReturnsNullsOnReadRange() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("x")
            .named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", "hello");
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader r = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerTypes = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.LONG));
        try (
            CloseableIterator<Page> it = r.readRange(
                storageObject,
                List.of("x"),
                10,
                0,
                parquetData.length,
                plannerTypes,
                ErrorPolicy.STRICT
            )
        ) {
            Page page = it.next();
            assertTrue(page.getBlock(0).isNull(0));
        }
    }

    public void testSchemaMismatchBooleanVsDoubleReturnsNullsOnReadRange() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("x").named("test_schema");
        byte[] parquetData = createParquetFile(schema, factory -> {
            Group g = factory.newGroup();
            g.add("x", true);
            return List.of(g);
        });
        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader r = new ParquetFormatReader(blockFactory);
        List<Attribute> plannerTypes = List.of(new ReferenceAttribute(Source.EMPTY, "x", DataType.DOUBLE));
        try (
            CloseableIterator<Page> it = r.readRange(
                storageObject,
                List.of("x"),
                10,
                0,
                parquetData.length,
                plannerTypes,
                ErrorPolicy.STRICT
            )
        ) {
            Page page = it.next();
            assertTrue(page.getBlock(0).isNull(0));
        }
    }

    public void testReadRangeSelectsCorrectRowGroups() throws Exception {
        byte[] parquetData = createWideMultiRowGroupFile(500);

        StorageObject storageObject = createStorageObject(parquetData);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);

        List<RangeAwareFormatReader.SplitRange> ranges = reader.discoverSplitRanges(storageObject);
        assertTrue("Need at least 2 ranges for this test, got " + ranges.size(), ranges.size() >= 2);

        int totalRowsFromRanges = 0;
        for (RangeAwareFormatReader.SplitRange range : ranges) {
            long rangeStart = range.offset();
            long rangeEnd = rangeStart + range.length();
            try (
                CloseableIterator<Page> iterator = reader.readRange(
                    storageObject,
                    null,
                    1000,
                    rangeStart,
                    rangeEnd,
                    List.of(),
                    ErrorPolicy.STRICT
                )
            ) {
                while (iterator.hasNext()) {
                    Page page = iterator.next();
                    totalRowsFromRanges += page.getPositionCount();
                }
            }
        }

        int totalRowsDirect = 0;
        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 1000)) {
            while (iterator.hasNext()) {
                totalRowsDirect += iterator.next().getPositionCount();
            }
        }
        assertEquals("Reading all ranges should produce the same total as a full read", totalRowsDirect, totalRowsFromRanges);
    }

    // --- Test helpers ---

    private static byte[] toFloat16Bytes(float value) {
        short float16 = Float.floatToFloat16(value);
        byte[] bytes = new byte[2];
        bytes[0] = (byte) (float16 & 0xFF);
        bytes[1] = (byte) ((float16 >> 8) & 0xFF);
        return bytes;
    }

    private static byte[] toUuidBytes(UUID uuid) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }

    private static byte[] toFixedLenDecimal(long unscaledValue, int fixedLen) {
        byte[] unscaledBytes = BigInteger.valueOf(unscaledValue).toByteArray();
        byte[] padded = new byte[fixedLen];
        byte fill = unscaledValue < 0 ? (byte) 0xFF : (byte) 0x00;
        Arrays.fill(padded, fill);
        System.arraycopy(unscaledBytes, 0, padded, fixedLen - unscaledBytes.length, unscaledBytes.length);
        return padded;
    }

    @FunctionalInterface
    private interface GroupCreator {
        List<Group> create(SimpleGroupFactory factory);
    }

    /**
     * Creates a Parquet file with wide rows (INT64 id + 200-char BINARY payload) and a small
     * row group size to guarantee multiple row groups in the output.
     */
    private byte[] createWideMultiRowGroupFile(int numRows) throws IOException {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("test_schema");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        String padding = "x".repeat(200);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(4 * 1024L)
                .withPageSize(512)
                .build()
        ) {
            for (int i = 0; i < numRows; i++) {
                Group g = groupFactory.newGroup();
                g.add("id", (long) i);
                g.add("payload", "row-" + i + "-" + padding);
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    private byte[] createParquetFile(MessageType schema, GroupCreator groupCreator) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        OutputFile outputFile = createOutputFile(outputStream);

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

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
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
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
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
    }

    private StorageObject createStorageObject(byte[] data) {
        return createStorageObject(data, "memory://test.parquet");
    }

    private StorageObject createStorageObject(byte[] data, String locationUri) {
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
                return StoragePath.of(locationUri);
            }
        };
    }
}
