/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

public class ArrowToBlockConverterTests extends ESTestCase {

    private RootAllocator allocator;
    private BlockFactory blockFactory;

    @Before
    public void setup() {
        allocator = new RootAllocator();
        blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    @After
    public void cleanup() {
        allocator.close();
    }

    public void testFromFloat64() {
        try (Float8Vector vector = new Float8Vector("test", allocator)) {
            vector.allocateNew(5);
            vector.set(0, 1.5);
            vector.set(1, 2.5);
            vector.setNull(2);
            vector.set(3, 3.5);
            vector.set(4, 4.5);
            vector.setValueCount(5);

            ArrowToBlockConverter converter = new ArrowToBlockConverter.FromFloat64();
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof DoubleBlock);
                DoubleBlock doubleBlock = (DoubleBlock) block;

                assertEquals(5, doubleBlock.getPositionCount());
                assertEquals(1.5, doubleBlock.getDouble(0), 0.0);
                assertEquals(2.5, doubleBlock.getDouble(1), 0.0);
                assertTrue(doubleBlock.isNull(2));
                assertEquals(3.5, doubleBlock.getDouble(3), 0.0);
                assertEquals(4.5, doubleBlock.getDouble(4), 0.0);
            }
        }
    }

    public void testFromFloat64AllNulls() {
        try (Float8Vector vector = new Float8Vector("test", allocator)) {
            vector.allocateNew(3);
            vector.setNull(0);
            vector.setNull(1);
            vector.setNull(2);
            vector.setValueCount(3);

            ArrowToBlockConverter converter = new ArrowToBlockConverter.FromFloat64();
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof DoubleBlock);
                DoubleBlock doubleBlock = (DoubleBlock) block;

                assertEquals(3, doubleBlock.getPositionCount());
                assertTrue(doubleBlock.isNull(0));
                assertTrue(doubleBlock.isNull(1));
                assertTrue(doubleBlock.isNull(2));
            }
        }
    }

    public void testFromInt64() {
        try (BigIntVector vector = new BigIntVector("test", allocator)) {
            vector.allocateNew(5);
            vector.set(0, 100L);
            vector.set(1, 200L);
            vector.setNull(2);
            vector.set(3, 300L);
            vector.set(4, 400L);
            vector.setValueCount(5);

            ArrowToBlockConverter converter = new ArrowToBlockConverter.FromInt64();
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof LongBlock);
                LongBlock longBlock = (LongBlock) block;

                assertEquals(5, longBlock.getPositionCount());
                assertEquals(100L, longBlock.getLong(0));
                assertEquals(200L, longBlock.getLong(1));
                assertTrue(longBlock.isNull(2));
                assertEquals(300L, longBlock.getLong(3));
                assertEquals(400L, longBlock.getLong(4));
            }
        }
    }

    public void testFromInt32() {
        try (IntVector vector = new IntVector("test", allocator)) {
            vector.allocateNew(5);
            vector.set(0, 10);
            vector.set(1, 20);
            vector.setNull(2);
            vector.set(3, 30);
            vector.set(4, 40);
            vector.setValueCount(5);

            ArrowToBlockConverter converter = new ArrowToBlockConverter.FromInt32();
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof IntBlock);
                IntBlock intBlock = (IntBlock) block;

                assertEquals(5, intBlock.getPositionCount());
                assertEquals(10, intBlock.getInt(0));
                assertEquals(20, intBlock.getInt(1));
                assertTrue(intBlock.isNull(2));
                assertEquals(30, intBlock.getInt(3));
                assertEquals(40, intBlock.getInt(4));
            }
        }
    }

    public void testFromBoolean() {
        try (BitVector vector = new BitVector("test", allocator)) {
            vector.allocateNew(5);
            vector.set(0, 1);
            vector.set(1, 0);
            vector.setNull(2);
            vector.set(3, 1);
            vector.set(4, 0);
            vector.setValueCount(5);

            ArrowToBlockConverter converter = new ArrowToBlockConverter.FromBoolean();
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof BooleanBlock);
                BooleanBlock booleanBlock = (BooleanBlock) block;

                assertEquals(5, booleanBlock.getPositionCount());
                assertTrue(booleanBlock.getBoolean(0));
                assertFalse(booleanBlock.getBoolean(1));
                assertTrue(booleanBlock.isNull(2));
                assertTrue(booleanBlock.getBoolean(3));
                assertFalse(booleanBlock.getBoolean(4));
            }
        }
    }

    public void testFromVarChar() {
        try (VarCharVector vector = new VarCharVector("test", allocator)) {
            vector.allocateNew(5);
            vector.set(0, "hello".getBytes(StandardCharsets.UTF_8));
            vector.set(1, "world".getBytes(StandardCharsets.UTF_8));
            vector.setNull(2);
            vector.set(3, "foo".getBytes(StandardCharsets.UTF_8));
            vector.set(4, "bar".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(5);

            ArrowToBlockConverter converter = new ArrowToBlockConverter.FromVarChar();
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof BytesRefBlock);
                BytesRefBlock bytesRefBlock = (BytesRefBlock) block;

                assertEquals(5, bytesRefBlock.getPositionCount());
                assertEquals(new BytesRef("hello"), bytesRefBlock.getBytesRef(0, new BytesRef()));
                assertEquals(new BytesRef("world"), bytesRefBlock.getBytesRef(1, new BytesRef()));
                assertTrue(bytesRefBlock.isNull(2));
                assertEquals(new BytesRef("foo"), bytesRefBlock.getBytesRef(3, new BytesRef()));
                assertEquals(new BytesRef("bar"), bytesRefBlock.getBytesRef(4, new BytesRef()));
            }
        }
    }

    public void testFromVarBinary() {
        try (VarBinaryVector vector = new VarBinaryVector("test", allocator)) {
            vector.allocateNew(5);
            vector.set(0, new byte[] { 1, 2, 3 });
            vector.set(1, new byte[] { 4, 5, 6 });
            vector.setNull(2);
            vector.set(3, new byte[] { 7, 8, 9 });
            vector.set(4, new byte[] { 10, 11, 12 });
            vector.setValueCount(5);

            ArrowToBlockConverter converter = new ArrowToBlockConverter.FromVarBinary();
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof BytesRefBlock);
                BytesRefBlock bytesRefBlock = (BytesRefBlock) block;

                assertEquals(5, bytesRefBlock.getPositionCount());
                assertEquals(new BytesRef(new byte[] { 1, 2, 3 }), bytesRefBlock.getBytesRef(0, new BytesRef()));
                assertEquals(new BytesRef(new byte[] { 4, 5, 6 }), bytesRefBlock.getBytesRef(1, new BytesRef()));
                assertTrue(bytesRefBlock.isNull(2));
                assertEquals(new BytesRef(new byte[] { 7, 8, 9 }), bytesRefBlock.getBytesRef(3, new BytesRef()));
                assertEquals(new BytesRef(new byte[] { 10, 11, 12 }), bytesRefBlock.getBytesRef(4, new BytesRef()));
            }
        }
    }

    public void testForTypeFactory() {
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.FLOAT8));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.BIGINT));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.INT));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.BIT));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.VARCHAR));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.VARBINARY));
        assertNull(ArrowToBlockConverter.forType(Types.MinorType.NULL));
        assertNull(ArrowToBlockConverter.forType(Types.MinorType.STRUCT));
    }

    public void testFromFloat64EmptyVector() {
        try (Float8Vector vector = new Float8Vector("test", allocator)) {
            vector.allocateNew(0);
            vector.setValueCount(0);

            ArrowToBlockConverter converter = new ArrowToBlockConverter.FromFloat64();
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof DoubleBlock);
                DoubleBlock doubleBlock = (DoubleBlock) block;
                assertEquals(0, doubleBlock.getPositionCount());
            }
        }
    }

    public void testFromInt32LargeVector() {
        int size = 10000;
        try (IntVector vector = new IntVector("test", allocator)) {
            vector.allocateNew(size);
            for (int i = 0; i < size; i++) {
                if (i % 100 == 0) {
                    vector.setNull(i);
                } else {
                    vector.set(i, i);
                }
            }
            vector.setValueCount(size);

            ArrowToBlockConverter converter = new ArrowToBlockConverter.FromInt32();
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof IntBlock);
                IntBlock intBlock = (IntBlock) block;

                assertEquals(size, intBlock.getPositionCount());
                for (int i = 0; i < size; i++) {
                    if (i % 100 == 0) {
                        assertTrue("Position " + i + " should be null", intBlock.isNull(i));
                    } else {
                        assertEquals("Position " + i + " value mismatch", i, intBlock.getInt(i));
                    }
                }
            }
        }
    }

    public void testSymmetricConversionDouble() {
        // Test round-trip: Block → Arrow → Block
        try (DoubleBlock.Builder builder = blockFactory.newDoubleBlockBuilder(3)) {
            builder.appendDouble(1.5);
            builder.appendNull();
            builder.appendDouble(3.5);

            try (DoubleBlock originalBlock = builder.build()) {
                // Convert Block → Arrow using BlockConverter
                try (Float8Vector vector = new Float8Vector("test", allocator)) {
                    vector.allocateNew(originalBlock.getPositionCount());
                    for (int i = 0; i < originalBlock.getPositionCount(); i++) {
                        if (originalBlock.isNull(i)) {
                            vector.setNull(i);
                        } else {
                            vector.set(i, originalBlock.getDouble(i));
                        }
                    }
                    vector.setValueCount(originalBlock.getPositionCount());

                    // Convert Arrow → Block using ArrowToBlockConverter
                    ArrowToBlockConverter converter = new ArrowToBlockConverter.FromFloat64();
                    try (Block convertedBlock = converter.convert(vector, blockFactory)) {
                        assertTrue(convertedBlock instanceof DoubleBlock);
                        DoubleBlock convertedDoubleBlock = (DoubleBlock) convertedBlock;

                        assertEquals(originalBlock.getPositionCount(), convertedDoubleBlock.getPositionCount());
                        for (int i = 0; i < originalBlock.getPositionCount(); i++) {
                            assertEquals(originalBlock.isNull(i), convertedDoubleBlock.isNull(i));
                            if (originalBlock.isNull(i) == false) {
                                assertEquals(originalBlock.getDouble(i), convertedDoubleBlock.getDouble(i), 0.0);
                            }
                        }
                    }
                }
            }
        }
    }
}
