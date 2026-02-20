/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
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
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

public class ArrowFormatReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    public void testFormatName() {
        ArrowFormatReader reader = new ArrowFormatReader(blockFactory);
        assertEquals("arrow", reader.formatName());
    }

    public void testFileExtensions() {
        ArrowFormatReader reader = new ArrowFormatReader(blockFactory);
        List<String> extensions = reader.fileExtensions();
        assertEquals(2, extensions.size());
        assertTrue(extensions.contains(".arrow"));
        assertTrue(extensions.contains(".ipc"));
    }

    public void testReadSchemaFromSimpleArrow() throws Exception {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("age", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("active", FieldType.notNullable(new ArrowType.Bool()), null)
            )
        );

        byte[] arrowData = createArrowFile(schema, (allocator, root) -> {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            IntVector ageVec = (IntVector) root.getVector("age");
            BitVector activeVec = (BitVector) root.getVector("active");

            idVec.allocateNew(1);
            nameVec.allocateNew(1);
            ageVec.allocateNew(1);
            activeVec.allocateNew(1);

            idVec.set(0, 1L);
            nameVec.set(0, "Alice".getBytes(StandardCharsets.UTF_8));
            ageVec.set(0, 30);
            activeVec.set(0, 1);

            root.setRowCount(1);
        });

        StorageObject storageObject = createStorageObject(arrowData);
        ArrowFormatReader reader = new ArrowFormatReader(blockFactory);

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

    public void testReadDataFromSimpleArrow() throws Exception {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field(
                    "score",
                    FieldType.notNullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                    null
                )
            )
        );

        byte[] arrowData = createArrowFile(schema, (allocator, root) -> {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            Float8Vector scoreVec = (Float8Vector) root.getVector("score");

            idVec.allocateNew(3);
            nameVec.allocateNew(3);
            scoreVec.allocateNew(3);

            idVec.set(0, 1L);
            nameVec.set(0, "Alice".getBytes(StandardCharsets.UTF_8));
            scoreVec.set(0, 95.5);

            idVec.set(1, 2L);
            nameVec.set(1, "Bob".getBytes(StandardCharsets.UTF_8));
            scoreVec.set(1, 87.3);

            idVec.set(2, 3L);
            nameVec.set(2, "Charlie".getBytes(StandardCharsets.UTF_8));
            scoreVec.set(2, 92.1);

            root.setRowCount(3);
        });

        StorageObject storageObject = createStorageObject(arrowData);
        ArrowFormatReader reader = new ArrowFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(3, page.getPositionCount());
            assertEquals(3, page.getBlockCount());

            assertEquals(1L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.001);

            assertEquals(2L, ((LongBlock) page.getBlock(0)).getLong(1));
            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(2)).getDouble(1), 0.001);

            assertEquals(3L, ((LongBlock) page.getBlock(0)).getLong(2));
            assertEquals(new BytesRef("Charlie"), ((BytesRefBlock) page.getBlock(1)).getBytesRef(2, new BytesRef()));
            assertEquals(92.1, ((DoubleBlock) page.getBlock(2)).getDouble(2), 0.001);

            assertFalse(iterator.hasNext());
        }
    }

    public void testReadWithColumnProjection() throws Exception {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field(
                    "score",
                    FieldType.notNullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                    null
                )
            )
        );

        byte[] arrowData = createArrowFile(schema, (allocator, root) -> {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            Float8Vector scoreVec = (Float8Vector) root.getVector("score");

            idVec.allocateNew(2);
            nameVec.allocateNew(2);
            scoreVec.allocateNew(2);

            idVec.set(0, 1L);
            nameVec.set(0, "Alice".getBytes(StandardCharsets.UTF_8));
            scoreVec.set(0, 95.5);

            idVec.set(1, 2L);
            nameVec.set(1, "Bob".getBytes(StandardCharsets.UTF_8));
            scoreVec.set(1, 87.3);

            root.setRowCount(2);
        });

        StorageObject storageObject = createStorageObject(arrowData);
        ArrowFormatReader reader = new ArrowFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, List.of("name", "score"), 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());
            assertEquals(2, page.getBlockCount());

            assertEquals(new BytesRef("Alice"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(0, new BytesRef()));
            assertEquals(95.5, ((DoubleBlock) page.getBlock(1)).getDouble(0), 0.001);

            assertEquals(new BytesRef("Bob"), ((BytesRefBlock) page.getBlock(0)).getBytesRef(1, new BytesRef()));
            assertEquals(87.3, ((DoubleBlock) page.getBlock(1)).getDouble(1), 0.001);
        }
    }

    public void testReadWithBatching() throws Exception {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("value", FieldType.notNullable(new ArrowType.Int(32, true)), null)
            )
        );

        byte[] arrowData = createArrowFile(schema, (allocator, root) -> {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            IntVector valueVec = (IntVector) root.getVector("value");

            idVec.allocateNew(25);
            valueVec.allocateNew(25);

            for (int i = 0; i < 25; i++) {
                idVec.set(i, (long) (i + 1));
                valueVec.set(i, (i + 1) * 10);
            }

            root.setRowCount(25);
        });

        StorageObject storageObject = createStorageObject(arrowData);
        ArrowFormatReader reader = new ArrowFormatReader(blockFactory);

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
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("active", FieldType.notNullable(new ArrowType.Bool()), null)
            )
        );

        byte[] arrowData = createArrowFile(schema, (allocator, root) -> {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            BitVector activeVec = (BitVector) root.getVector("active");

            idVec.allocateNew(2);
            activeVec.allocateNew(2);

            idVec.set(0, 1L);
            activeVec.set(0, 1);

            idVec.set(1, 2L);
            activeVec.set(1, 0);

            root.setRowCount(2);
        });

        StorageObject storageObject = createStorageObject(arrowData);
        ArrowFormatReader reader = new ArrowFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());

            assertTrue(((BooleanBlock) page.getBlock(1)).getBoolean(0));
            assertFalse(((BooleanBlock) page.getBlock(1)).getBoolean(1));
        }
    }

    public void testReadIntegerColumn() throws Exception {
        Schema schema = new Schema(List.of(new Field("count", FieldType.notNullable(new ArrowType.Int(32, true)), null)));

        byte[] arrowData = createArrowFile(schema, (allocator, root) -> {
            IntVector countVec = (IntVector) root.getVector("count");

            countVec.allocateNew(3);

            countVec.set(0, 100);
            countVec.set(1, 200);
            countVec.set(2, 300);

            root.setRowCount(3);
        });

        StorageObject storageObject = createStorageObject(arrowData);
        ArrowFormatReader reader = new ArrowFormatReader(blockFactory);

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
        Schema schema = new Schema(
            List.of(
                new Field(
                    "temperature",
                    FieldType.notNullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)),
                    null
                )
            )
        );

        byte[] arrowData = createArrowFile(schema, (allocator, root) -> {
            Float4Vector tempVec = (Float4Vector) root.getVector("temperature");

            tempVec.allocateNew(2);

            tempVec.set(0, 98.6f);
            tempVec.set(1, 37.0f);

            root.setRowCount(2);
        });

        StorageObject storageObject = createStorageObject(arrowData);
        ArrowFormatReader reader = new ArrowFormatReader(blockFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();

            assertEquals(2, page.getPositionCount());

            assertEquals(98.6, ((DoubleBlock) page.getBlock(0)).getDouble(0), 0.1);
            assertEquals(37.0, ((DoubleBlock) page.getBlock(0)).getDouble(1), 0.1);
        }
    }

    public void testMetadataReturnsCorrectSourceType() throws Exception {
        Schema schema = new Schema(List.of(new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

        byte[] arrowData = createArrowFile(schema, (allocator, root) -> {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            idVec.allocateNew(1);
            idVec.set(0, 1L);
            root.setRowCount(1);
        });

        StorageObject storageObject = createStorageObject(arrowData);
        ArrowFormatReader reader = new ArrowFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertEquals("arrow", metadata.sourceType());
    }

    public void testCircuitBreakerTracksArrowAllocations() throws Exception {
        Schema schema = new Schema(
            List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null)
            )
        );

        byte[] arrowData = createArrowFile(schema, (allocator, root) -> {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            idVec.allocateNew(2);
            nameVec.allocateNew(2);
            idVec.set(0, 1L);
            nameVec.set(0, "Alice".getBytes(StandardCharsets.UTF_8));
            idVec.set(1, 2L);
            nameVec.set(1, "Bob".getBytes(StandardCharsets.UTF_8));
            root.setRowCount(2);
        });

        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("test-breaker", ByteSizeValue.ofMb(10));
        BlockFactory trackedFactory = BlockFactory.getInstance(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        assertEquals(0, breaker.getUsed());

        StorageObject storageObject = createStorageObject(arrowData);
        ArrowFormatReader reader = new ArrowFormatReader(trackedFactory);

        try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
            assertTrue(iterator.hasNext());
            Page page = iterator.next();
            assertEquals(2, page.getPositionCount());
            // Breaker should be non-zero while Arrow buffers and ESQL blocks are live
            assertTrue(breaker.getUsed() > 0);
            page.releaseBlocks();
        }
        // After closing iterator (Arrow allocator) and releasing page blocks, breaker returns to zero
        assertEquals(0, breaker.getUsed());
    }

    public void testCircuitBreakerTripsOnExcessiveArrowAllocation() throws Exception {
        Schema schema = new Schema(List.of(new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null)));

        byte[] arrowData = createArrowFile(schema, (allocator, root) -> {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            idVec.allocateNew(1000);
            for (int i = 0; i < 1000; i++) {
                idVec.set(i, (long) i);
            }
            root.setRowCount(1000);
        });

        // Set a very tight breaker limit that will be exceeded when Arrow loads the batch
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("test-breaker", ByteSizeValue.ofBytes(64));
        BlockFactory tightFactory = BlockFactory.getInstance(breaker, BigArrays.NON_RECYCLING_INSTANCE);

        StorageObject storageObject = createStorageObject(arrowData);
        ArrowFormatReader reader = new ArrowFormatReader(tightFactory);

        expectThrows(CircuitBreakingException.class, () -> {
            try (CloseableIterator<Page> iterator = reader.read(storageObject, null, 10)) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        });
    }

    @FunctionalInterface
    private interface VectorPopulator {
        void populate(BufferAllocator allocator, VectorSchemaRoot root);
    }

    private byte[] createArrowFile(Schema schema, VectorPopulator populator) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try (
            BufferAllocator allocator = new RootAllocator();
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(outputStream))
        ) {
            writer.start();
            populator.populate(allocator, root);
            writer.writeBatch();
            writer.end();
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
                return StoragePath.of("memory://test.arrow");
            }
        };
    }
}
