/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

/**
 * Tests for the narrower/transformed ArrowBuf types: UInt8, Int8, UInt16, Int16, UInt32, Timestamp1k, and Float16.
 */
public class NarrowArrowBufTests extends ESTestCase {

    private BufferAllocator allocator;
    private BlockFactory blockFactory;

    @Before
    public void setup() {
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
        allocator = blockFactory.arrowAllocator();
    }

    @After
    public void cleanup() {
        allocator.close();
    }

    // ----- UInt8 (unsigned byte -> Int) -----

    public void testUInt8VectorBasics() {
        ArrowBuf buf = allocator.buffer(4);
        buf.setByte(0, 0);
        buf.setByte(1, 127);
        buf.setByte(2, (byte) 128);   // unsigned: 128
        buf.setByte(3, (byte) 0xFF);  // unsigned: 255

        try (var vector = new UInt8ArrowBufVector(buf, 4, blockFactory)) {
            assertEquals(4, vector.getPositionCount());
            assertEquals(ElementType.INT, vector.elementType());
            assertEquals(0, vector.getInt(0));
            assertEquals(127, vector.getInt(1));
            assertEquals(128, vector.getInt(2));
            assertEquals(255, vector.getInt(3));
            assertEquals(0, vector.min());
            assertEquals(255, vector.max());
        }
    }

    public void testUInt8VectorFilter() {
        ArrowBuf buf = allocator.buffer(4);
        buf.setByte(0, 10);
        buf.setByte(1, 20);
        buf.setByte(2, (byte) 200); // unsigned: 200
        buf.setByte(3, (byte) 255); // unsigned: 255

        try (var vector = new UInt8ArrowBufVector(buf, 4, blockFactory)) {
            try (var filtered = vector.filter(false, 0, 2)) {
                assertEquals(2, filtered.getPositionCount());
                assertEquals(10, filtered.getInt(0));
                assertEquals(200, filtered.getInt(1));
            }
        }
    }

    public void testUInt8BlockWithNulls() {
        ArrowBuf valueBuf = allocator.buffer(3);
        valueBuf.setByte(0, 10);
        valueBuf.setByte(1, 0);   // value at null position (ignored)
        valueBuf.setByte(2, (byte) 200);

        ArrowBuf validityBuf = allocator.buffer(8);
        validityBuf.setZero(0, 8);
        // Set bits for positions 0 and 2, leave position 1 null
        validityBuf.setByte(0, 0b00000101);

        try (var block = new UInt8ArrowBufBlock(valueBuf, validityBuf, null, 3, 0, blockFactory)) {
            assertEquals(3, block.getPositionCount());
            assertTrue(block.mayHaveNulls());

            assertFalse(block.isNull(0));
            assertEquals(10, block.getInt(0));

            assertTrue(block.isNull(1));

            assertFalse(block.isNull(2));
            assertEquals(200, block.getInt(2));
        }
    }

    public void testUInt8BlockFilter() {
        ArrowBuf valueBuf = allocator.buffer(4);
        valueBuf.setByte(0, 10);
        valueBuf.setByte(1, 20);
        valueBuf.setByte(2, (byte) 0xAB);
        valueBuf.setByte(3, (byte) 0xFF);

        try (var block = new UInt8ArrowBufBlock(valueBuf, null, null, 4, 0, blockFactory)) {
            try (IntBlock filtered = block.filter(false, 1, 3)) {
                assertEquals(2, filtered.getPositionCount());
                assertEquals(20, filtered.getInt(0));
                assertEquals(255, filtered.getInt(1));
            }
        }
    }

    // ----- Int8 (signed byte -> Int) -----

    public void testInt8VectorBasics() {
        ArrowBuf buf = allocator.buffer(4);
        buf.setByte(0, 0);
        buf.setByte(1, 127);
        buf.setByte(2, (byte) -128);
        buf.setByte(3, (byte) -1);

        try (var vector = new Int8ArrowBufVector(buf, 4, blockFactory)) {
            assertEquals(4, vector.getPositionCount());
            assertEquals(ElementType.INT, vector.elementType());
            assertEquals(0, vector.getInt(0));
            assertEquals(127, vector.getInt(1));
            assertEquals(-128, vector.getInt(2));
            assertEquals(-1, vector.getInt(3));
            assertEquals(-128, vector.min());
            assertEquals(127, vector.max());
        }
    }

    public void testInt8VectorFilter() {
        ArrowBuf buf = allocator.buffer(3);
        buf.setByte(0, 10);
        buf.setByte(1, (byte) -50);
        buf.setByte(2, 42);

        try (var vector = new Int8ArrowBufVector(buf, 3, blockFactory)) {
            try (var filtered = vector.filter(false, 1, 2)) {
                assertEquals(2, filtered.getPositionCount());
                assertEquals(-50, filtered.getInt(0));
                assertEquals(42, filtered.getInt(1));
            }
        }
    }

    // ----- UInt16 (unsigned short -> Int) -----

    public void testUInt16VectorBasics() {
        ArrowBuf buf = allocator.buffer(4 * Short.BYTES);
        buf.setShort(0, (short) 0);
        buf.setShort(Short.BYTES, (short) 1000);
        buf.setShort(2L * Short.BYTES, (short) 32767);
        buf.setShort(3L * Short.BYTES, (short) 0xFFFF); // unsigned: 65535

        try (var vector = new UInt16ArrowBufVector(buf, 4, blockFactory)) {
            assertEquals(4, vector.getPositionCount());
            assertEquals(ElementType.INT, vector.elementType());
            assertEquals(0, vector.getInt(0));
            assertEquals(1000, vector.getInt(1));
            assertEquals(32767, vector.getInt(2));
            assertEquals(65535, vector.getInt(3));
            assertEquals(0, vector.min());
            assertEquals(65535, vector.max());
        }
    }

    public void testUInt16VectorFilter() {
        ArrowBuf buf = allocator.buffer(3 * Short.BYTES);
        buf.setShort(0, (short) 100);
        buf.setShort(Short.BYTES, (short) 50000);
        buf.setShort(2L * Short.BYTES, (short) 0xFFFF);

        try (var vector = new UInt16ArrowBufVector(buf, 3, blockFactory)) {
            try (var filtered = vector.filter(false, 0, 2)) {
                assertEquals(2, filtered.getPositionCount());
                assertEquals(100, filtered.getInt(0));
                assertEquals(65535, filtered.getInt(1));
            }
        }
    }

    // ----- Int16 (signed short -> Int) -----

    public void testInt16VectorBasics() {
        ArrowBuf buf = allocator.buffer(4 * Short.BYTES);
        buf.setShort(0, (short) 0);
        buf.setShort(Short.BYTES, (short) 32767);
        buf.setShort(2L * Short.BYTES, (short) -32768);
        buf.setShort(3L * Short.BYTES, (short) -1);

        try (var vector = new Int16ArrowBufVector(buf, 4, blockFactory)) {
            assertEquals(4, vector.getPositionCount());
            assertEquals(0, vector.getInt(0));
            assertEquals(32767, vector.getInt(1));
            assertEquals(-32768, vector.getInt(2));
            assertEquals(-1, vector.getInt(3));
            assertEquals(-32768, vector.min());
            assertEquals(32767, vector.max());
        }
    }

    // ----- UInt32 (unsigned int -> Long) -----

    public void testUInt32VectorBasics() {
        ArrowBuf buf = allocator.buffer(4 * Integer.BYTES);
        buf.setInt(0, 0);
        buf.setInt(Integer.BYTES, 100_000);
        buf.setInt(2L * Integer.BYTES, Integer.MAX_VALUE);
        buf.setInt(3L * Integer.BYTES, 0xFFFFFFFF); // unsigned: 4294967295

        try (var vector = new UInt32ArrowBufVector(buf, 4, blockFactory)) {
            assertEquals(4, vector.getPositionCount());
            assertEquals(ElementType.LONG, vector.elementType());
            assertEquals(0L, vector.getLong(0));
            assertEquals(100_000L, vector.getLong(1));
            assertEquals((long) Integer.MAX_VALUE, vector.getLong(2));
            assertEquals(4294967295L, vector.getLong(3));
        }
    }

    public void testUInt32VectorFilter() {
        ArrowBuf buf = allocator.buffer(3 * Integer.BYTES);
        buf.setInt(0, 42);
        buf.setInt(Integer.BYTES, 0xFFFFFFFF);
        buf.setInt(2L * Integer.BYTES, 1000);

        try (var vector = new UInt32ArrowBufVector(buf, 3, blockFactory)) {
            try (var filtered = vector.filter(false, 1, 2)) {
                assertEquals(2, filtered.getPositionCount());
                assertEquals(4294967295L, filtered.getLong(0));
                assertEquals(1000L, filtered.getLong(1));
            }
        }
    }

    public void testUInt32BlockWithNulls() {
        ArrowBuf valueBuf = allocator.buffer(3 * Integer.BYTES);
        valueBuf.setInt(0, 42);
        valueBuf.setInt(Integer.BYTES, 0);
        valueBuf.setInt(2L * Integer.BYTES, 0xFFFFFFFF);

        ArrowBuf validityBuf = allocator.buffer(8);
        validityBuf.setZero(0, 8);
        validityBuf.setByte(0, 0b00000101); // positions 0 and 2 valid

        try (var block = new UInt32ArrowBufBlock(valueBuf, validityBuf, null, 3, 0, blockFactory)) {
            assertEquals(3, block.getPositionCount());
            assertTrue(block.mayHaveNulls());

            assertFalse(block.isNull(0));
            assertEquals(42L, block.getLong(0));

            assertTrue(block.isNull(1));

            assertFalse(block.isNull(2));
            assertEquals(4294967295L, block.getLong(2));
        }
    }

    public void testUInt32BlockFilter() {
        ArrowBuf valueBuf = allocator.buffer(4 * Integer.BYTES);
        valueBuf.setInt(0, 10);
        valueBuf.setInt(Integer.BYTES, 0x80000000);
        valueBuf.setInt(2L * Integer.BYTES, 0xFFFFFFFF);
        valueBuf.setInt(3L * Integer.BYTES, 100);

        try (var block = new UInt32ArrowBufBlock(valueBuf, null, null, 4, 0, blockFactory)) {
            try (LongBlock filtered = block.filter(false, 0, 2, 3)) {
                assertEquals(3, filtered.getPositionCount());
                assertEquals(10L, filtered.getLong(0));
                assertEquals(4294967295L, filtered.getLong(1));
                assertEquals(100L, filtered.getLong(2));
            }
        }
    }

    // ----- Timestamp1k (long * 1000 -> Long) -----

    public void testTimestamp1kVectorBasics() {
        ArrowBuf buf = allocator.buffer(4 * Long.BYTES);
        buf.setLong(0, 0L);
        buf.setLong(Long.BYTES, 1L);
        buf.setLong(2L * Long.BYTES, 1000L);
        buf.setLong(3L * Long.BYTES, 1_700_000_000L); // a Unix timestamp in seconds

        try (var vector = new LongMul1kArrowBufVector(buf, 4, blockFactory)) {
            assertEquals(4, vector.getPositionCount());
            assertEquals(ElementType.LONG, vector.elementType());
            assertEquals(0L, vector.getLong(0));
            assertEquals(1000L, vector.getLong(1));
            assertEquals(1_000_000L, vector.getLong(2));
            assertEquals(1_700_000_000_000L, vector.getLong(3));
        }
    }

    public void testTimestamp1kVectorFilter() {
        ArrowBuf buf = allocator.buffer(3 * Long.BYTES);
        buf.setLong(0, 100L);
        buf.setLong(Long.BYTES, 200L);
        buf.setLong(2L * Long.BYTES, 300L);

        try (var vector = new LongMul1kArrowBufVector(buf, 3, blockFactory)) {
            try (var filtered = vector.filter(false, 0, 2)) {
                assertEquals(2, filtered.getPositionCount());
                assertEquals(100_000L, filtered.getLong(0));
                assertEquals(300_000L, filtered.getLong(1));
            }
        }
    }

    public void testTimestamp1kBlockWithNulls() {
        ArrowBuf valueBuf = allocator.buffer(3 * Long.BYTES);
        valueBuf.setLong(0, 1_700_000_000L);
        valueBuf.setLong(Long.BYTES, 0L);
        valueBuf.setLong(2L * Long.BYTES, 42L);

        ArrowBuf validityBuf = allocator.buffer(8);
        validityBuf.setZero(0, 8);
        validityBuf.setByte(0, 0b00000101); // positions 0 and 2 valid

        try (var block = new LongMul1kArrowBufBlock(valueBuf, validityBuf, null, 3, 0, blockFactory)) {
            assertFalse(block.isNull(0));
            assertEquals(1_700_000_000_000L, block.getLong(0));

            assertTrue(block.isNull(1));

            assertFalse(block.isNull(2));
            assertEquals(42_000L, block.getLong(2));
        }
    }

    public void testTimestamp1kBlockFilter() {
        ArrowBuf valueBuf = allocator.buffer(3 * Long.BYTES);
        valueBuf.setLong(0, 10L);
        valueBuf.setLong(Long.BYTES, 20L);
        valueBuf.setLong(2L * Long.BYTES, 30L);

        try (var block = new LongMul1kArrowBufBlock(valueBuf, null, null, 3, 0, blockFactory)) {
            try (LongBlock filtered = block.filter(false, 1, 2)) {
                assertEquals(2, filtered.getPositionCount());
                assertEquals(20_000L, filtered.getLong(0));
                assertEquals(30_000L, filtered.getLong(1));
            }
        }
    }

    // ----- Cross-cutting: keepMask and lookup -----

    public void testUInt8BlockKeepMask() {
        ArrowBuf valueBuf = allocator.buffer(4);
        valueBuf.setByte(0, (byte) 200);
        valueBuf.setByte(1, (byte) 100);
        valueBuf.setByte(2, (byte) 255);
        valueBuf.setByte(3, (byte) 50);

        try (var block = new UInt8ArrowBufBlock(valueBuf, null, null, 4, 0, blockFactory)) {
            try (var maskBuilder = blockFactory.newBooleanVectorFixedBuilder(4)) {
                maskBuilder.appendBoolean(true);
                maskBuilder.appendBoolean(false);
                maskBuilder.appendBoolean(true);
                maskBuilder.appendBoolean(false);
                try (BooleanVector mask = maskBuilder.build()) {
                    try (IntBlock kept = block.keepMask(mask)) {
                        assertEquals(4, kept.getPositionCount());
                        assertFalse(kept.isNull(0));
                        assertEquals(200, kept.getInt(kept.getFirstValueIndex(0)));
                        assertTrue(kept.isNull(1));
                        assertFalse(kept.isNull(2));
                        assertEquals(255, kept.getInt(kept.getFirstValueIndex(2)));
                        assertTrue(kept.isNull(3));
                    }
                }
            }
        }
    }

    public void testTimestamp1kVectorLookup() {
        ArrowBuf buf = allocator.buffer(4 * Long.BYTES);
        buf.setLong(0, 10L);
        buf.setLong(Long.BYTES, 20L);
        buf.setLong(2L * Long.BYTES, 30L);
        buf.setLong(3L * Long.BYTES, 40L);

        try (var vector = new LongMul1kArrowBufVector(buf, 4, blockFactory)) {
            try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(2)) {
                posBuilder.appendInt(1);
                posBuilder.appendInt(3);
                try (IntBlock positions = posBuilder.build()) {
                    try (ReleasableIterator<? extends LongBlock> iter = vector.lookup(positions, ByteSizeValue.ofMb(1))) {
                        assertTrue(iter.hasNext());
                        try (LongBlock result = iter.next()) {
                            assertEquals(2, result.getPositionCount());
                            assertEquals(20_000L, result.getLong(result.getFirstValueIndex(0)));
                            assertEquals(40_000L, result.getLong(result.getFirstValueIndex(1)));
                        }
                        assertFalse(iter.hasNext());
                    }
                }
            }
        }
    }

    // ----- Float16 (16-bit float -> Double) -----

    private static short f16(float value) {
        return Float.floatToFloat16(value);
    }

    public void testFloat16VectorBasics() {
        ArrowBuf buf = allocator.buffer(4 * Short.BYTES);
        buf.setShort(0, f16(0.0f));
        buf.setShort(Short.BYTES, f16(1.0f));
        buf.setShort(2L * Short.BYTES, f16(-3.5f));
        buf.setShort(3L * Short.BYTES, f16(65504.0f)); // max finite float16

        try (var vector = new Float16ArrowBufVector(buf, 4, blockFactory)) {
            assertEquals(4, vector.getPositionCount());
            assertEquals(ElementType.DOUBLE, vector.elementType());
            assertEquals(0.0, vector.getDouble(0), 0.0);
            assertEquals(1.0, vector.getDouble(1), 0.001);
            assertEquals(-3.5, vector.getDouble(2), 0.001);
            assertEquals(65504.0, vector.getDouble(3), 0.0);
        }
    }

    public void testFloat16VectorFilter() {
        ArrowBuf buf = allocator.buffer(3 * Short.BYTES);
        buf.setShort(0, f16(1.5f));
        buf.setShort(Short.BYTES, f16(2.5f));
        buf.setShort(2L * Short.BYTES, f16(3.5f));

        try (var vector = new Float16ArrowBufVector(buf, 3, blockFactory)) {
            try (var filtered = vector.filter(false, 0, 2)) {
                assertEquals(2, filtered.getPositionCount());
                assertEquals(1.5, filtered.getDouble(0), 0.001);
                assertEquals(3.5, filtered.getDouble(1), 0.001);
            }
        }
    }

    public void testFloat16BlockWithNulls() {
        ArrowBuf valueBuf = allocator.buffer(3 * Short.BYTES);
        valueBuf.setShort(0, f16(42.0f));
        valueBuf.setShort(Short.BYTES, (short) 0);
        valueBuf.setShort(2L * Short.BYTES, f16(-1.0f));

        ArrowBuf validityBuf = allocator.buffer(8);
        validityBuf.setZero(0, 8);
        validityBuf.setByte(0, 0b00000101); // positions 0 and 2 valid

        try (var block = new Float16ArrowBufBlock(valueBuf, validityBuf, null, 3, 0, blockFactory)) {
            assertEquals(3, block.getPositionCount());
            assertTrue(block.mayHaveNulls());

            assertFalse(block.isNull(0));
            assertEquals(42.0, block.getDouble(0), 0.001);

            assertTrue(block.isNull(1));

            assertFalse(block.isNull(2));
            assertEquals(-1.0, block.getDouble(2), 0.001);
        }
    }

    public void testFloat16BlockFilter() {
        ArrowBuf valueBuf = allocator.buffer(4 * Short.BYTES);
        valueBuf.setShort(0, f16(10.0f));
        valueBuf.setShort(Short.BYTES, f16(20.0f));
        valueBuf.setShort(2L * Short.BYTES, f16(30.0f));
        valueBuf.setShort(3L * Short.BYTES, f16(40.0f));

        try (var block = new Float16ArrowBufBlock(valueBuf, null, null, 4, 0, blockFactory)) {
            try (DoubleBlock filtered = block.filter(false, 1, 3)) {
                assertEquals(2, filtered.getPositionCount());
                assertEquals(20.0, filtered.getDouble(0), 0.001);
                assertEquals(40.0, filtered.getDouble(1), 0.001);
            }
        }
    }

    public void testFloat16BlockKeepMask() {
        ArrowBuf valueBuf = allocator.buffer(3 * Short.BYTES);
        valueBuf.setShort(0, f16(1.0f));
        valueBuf.setShort(Short.BYTES, f16(2.0f));
        valueBuf.setShort(2L * Short.BYTES, f16(3.0f));

        try (var block = new Float16ArrowBufBlock(valueBuf, null, null, 3, 0, blockFactory)) {
            try (var maskBuilder = blockFactory.newBooleanVectorFixedBuilder(3)) {
                maskBuilder.appendBoolean(false);
                maskBuilder.appendBoolean(true);
                maskBuilder.appendBoolean(true);
                try (BooleanVector mask = maskBuilder.build()) {
                    try (DoubleBlock kept = block.keepMask(mask)) {
                        assertEquals(3, kept.getPositionCount());
                        assertTrue(kept.isNull(0));
                        assertFalse(kept.isNull(1));
                        assertEquals(2.0, kept.getDouble(kept.getFirstValueIndex(1)), 0.001);
                        assertFalse(kept.isNull(2));
                        assertEquals(3.0, kept.getDouble(kept.getFirstValueIndex(2)), 0.001);
                    }
                }
            }
        }
    }
}
