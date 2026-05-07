/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.FieldType;
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

/**
 * End-to-end constant-detection coverage for the {@code *ArrowBufBlock.of(...)} factories
 * across every supported fixed-width Arrow type. Verifies that uniform vectors yield constant
 * blocks (or {@code ConstantNullBlock} when all-null) and that non-uniform vectors fall
 * through to the regular zero-copy {@code *ArrowBufBlock} path.
 */
public class ArrowBufConstantTests extends ESTestCase {

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

    // ---- INT ----

    public void testIntConstant() {
        try (IntVector v = new IntVector("v", allocator)) {
            fillInt(v, new int[] { 42, 42, 42, 42 });
            try (Block block = IntArrowBufBlock.of(v, blockFactory)) {
                assertTrue(block.asVector() != null && block.asVector().isConstant());
                IntBlock ib = (IntBlock) block;
                assertEquals(4, ib.getPositionCount());
                for (int i = 0; i < 4; i++) {
                    assertEquals(42, ib.getInt(i));
                }
            }
        }
    }

    public void testIntNotConstant() {
        try (IntVector v = new IntVector("v", allocator)) {
            fillInt(v, new int[] { 1, 1, 1, 2 });
            try (Block block = IntArrowBufBlock.of(v, blockFactory)) {
                assertFalse(block.asVector() != null && block.asVector().isConstant());
                assertTrue(block instanceof IntArrowBufBlock);
            }
        }
    }

    public void testIntAllNull() {
        try (IntVector v = new IntVector("v", allocator)) {
            v.allocateNew(3);
            v.setNull(0);
            v.setNull(1);
            v.setNull(2);
            v.setValueCount(3);
            try (Block block = IntArrowBufBlock.of(v, blockFactory)) {
                assertTrue(block.areAllValuesNull());
                assertEquals(3, block.getPositionCount());
            }
        }
    }

    // ---- BIGINT ----

    public void testLongConstant() {
        try (BigIntVector v = new BigIntVector("v", allocator)) {
            fillLong(v, new long[] { Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE });
            try (Block block = LongArrowBufBlock.of(v, blockFactory)) {
                LongBlock lb = (LongBlock) block;
                assertTrue(lb.asVector() != null && lb.asVector().isConstant());
                assertEquals(Long.MAX_VALUE, lb.getLong(0));
            }
        }
    }

    // ---- FLOAT8 (double) ----

    public void testDoubleConstant() {
        try (Float8Vector v = new Float8Vector("v", allocator)) {
            v.allocateNew(4);
            for (int i = 0; i < 4; i++) {
                v.set(i, 3.14);
            }
            v.setValueCount(4);
            try (Block block = DoubleArrowBufBlock.of(v, blockFactory)) {
                DoubleBlock db = (DoubleBlock) block;
                assertTrue(db.asVector() != null && db.asVector().isConstant());
                assertEquals(3.14, db.getDouble(0), 0.0);
            }
        }
    }

    public void testDoubleMixedNaNNotConstant() {
        // Two distinct NaN bit patterns must NOT be detected as constant
        try (Float8Vector v = new Float8Vector("v", allocator)) {
            v.allocateNew(2);
            v.getDataBuffer().setLong(0, 0x7FF8000000000001L);
            v.getDataBuffer().setLong(Long.BYTES, 0x7FF8000000000002L);
            v.setValueCount(2);
            try (Block block = DoubleArrowBufBlock.of(v, blockFactory)) {
                assertFalse(block.asVector() != null && block.asVector().isConstant());
            }
        }
    }

    // ---- FLOAT4 (widened to double) ----

    public void testFloat4Constant() {
        try (Float4Vector v = new Float4Vector("v", allocator)) {
            v.allocateNew(5);
            for (int i = 0; i < 5; i++) {
                v.set(i, 1.5f);
            }
            v.setValueCount(5);
            try (Block block = FloatToDoubleArrowBlock.of(v, blockFactory)) {
                DoubleBlock db = (DoubleBlock) block;
                assertTrue(db.asVector() != null && db.asVector().isConstant());
                assertEquals(1.5, db.getDouble(0), 0.0);
            }
        }
    }

    public void testFloat4NotConstant() {
        try (Float4Vector v = new Float4Vector("v", allocator)) {
            v.allocateNew(3);
            v.set(0, 1.0f);
            v.set(1, 1.0f);
            v.set(2, 2.0f);
            v.setValueCount(3);
            try (Block block = FloatToDoubleArrowBlock.of(v, blockFactory)) {
                assertFalse(block.asVector() != null && block.asVector().isConstant());
            }
        }
    }

    // ---- FLOAT2 (Float16, widened to double) ----

    public void testFloat16Constant() {
        try (Float2Vector v = new Float2Vector("v", allocator)) {
            v.allocateNew(4);
            short halfTwoPointFive = Float.floatToFloat16(2.5f);
            for (int i = 0; i < 4; i++) {
                v.set(i, halfTwoPointFive);
            }
            v.setValueCount(4);
            try (Block block = Float16ArrowBufBlock.of(v, blockFactory)) {
                DoubleBlock db = (DoubleBlock) block;
                assertTrue(db.asVector() != null && db.asVector().isConstant());
                assertEquals(2.5, db.getDouble(0), 0.0);
            }
        }
    }

    // ---- TINYINT (Int8 → IntBlock) ----

    public void testInt8Constant() {
        try (TinyIntVector v = new TinyIntVector("v", allocator)) {
            v.allocateNew(8);
            for (int i = 0; i < 8; i++) {
                v.set(i, (byte) -7);
            }
            v.setValueCount(8);
            try (Block block = Int8ArrowBufBlock.of(v, blockFactory)) {
                IntBlock ib = (IntBlock) block;
                assertTrue(ib.asVector() != null && ib.asVector().isConstant());
                assertEquals(-7, ib.getInt(0));
            }
        }
    }

    // ---- SMALLINT (Int16 → IntBlock) ----

    public void testInt16Constant() {
        try (SmallIntVector v = new SmallIntVector("v", allocator)) {
            v.allocateNew(4);
            for (int i = 0; i < 4; i++) {
                v.set(i, (short) 4321);
            }
            v.setValueCount(4);
            try (Block block = Int16ArrowBufBlock.of(v, blockFactory)) {
                IntBlock ib = (IntBlock) block;
                assertTrue(ib.asVector() != null && ib.asVector().isConstant());
                assertEquals(4321, ib.getInt(0));
            }
        }
    }

    // ---- UINT1 (UInt8 → IntBlock) ----

    public void testUInt8Constant() {
        try (UInt1Vector v = new UInt1Vector("v", allocator)) {
            v.allocateNew(4);
            for (int i = 0; i < 4; i++) {
                v.set(i, (byte) 0xFF);
            }
            v.setValueCount(4);
            try (Block block = UInt8ArrowBufBlock.of(v, blockFactory)) {
                IntBlock ib = (IntBlock) block;
                assertTrue(ib.asVector() != null && ib.asVector().isConstant());
                assertEquals(255, ib.getInt(0));
            }
        }
    }

    // ---- UINT2 (UInt16 → IntBlock) ----

    public void testUInt16Constant() {
        try (UInt2Vector v = new UInt2Vector("v", allocator)) {
            v.allocateNew(3);
            for (int i = 0; i < 3; i++) {
                v.set(i, '\u8001');
            }
            v.setValueCount(3);
            try (Block block = UInt16ArrowBufBlock.of(v, blockFactory)) {
                IntBlock ib = (IntBlock) block;
                assertTrue(ib.asVector() != null && ib.asVector().isConstant());
                assertEquals(0x8001, ib.getInt(0));
            }
        }
    }

    // ---- UINT4 (UInt32 → LongBlock with toUnsignedLong transform) ----

    public void testUInt32Constant() {
        try (UInt4Vector v = new UInt4Vector("v", allocator)) {
            v.allocateNew(3);
            for (int i = 0; i < 3; i++) {
                v.set(i, 0xFFFFFFFF);
            }
            v.setValueCount(3);
            try (Block block = UInt32ArrowBufBlock.of(v, blockFactory)) {
                LongBlock lb = (LongBlock) block;
                assertTrue(lb.asVector() != null && lb.asVector().isConstant());
                assertEquals(0xFFFFFFFFL, lb.getLong(0));
            }
        }
    }

    // ---- LongMul1k (TIMESTAMPSEC → LongBlock with × 1000 transform) ----

    public void testLongMul1kConstant() {
        try (
            TimeStampSecVector v = new TimeStampSecVector(
                "v",
                FieldType.nullable(
                    new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.SECOND, null)
                ),
                allocator
            )
        ) {
            v.allocateNew(2);
            v.set(0, 1700000000L);
            v.set(1, 1700000000L);
            v.setValueCount(2);
            try (Block block = LongMul1kArrowBufBlock.of(v, blockFactory)) {
                LongBlock lb = (LongBlock) block;
                assertTrue(lb.asVector() != null && lb.asVector().isConstant());
                assertEquals(1700000000L * 1000, lb.getLong(0));
            }
        }
    }

    // ---- TIMESTAMPMILLI (LongArrowBufBlock pass-through) ----

    public void testTimestampMilliConstant() {
        try (
            TimeStampMilliVector v = new TimeStampMilliVector(
                "v",
                FieldType.nullable(
                    new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null)
                ),
                allocator
            )
        ) {
            v.allocateNew(3);
            for (int i = 0; i < 3; i++) {
                v.set(i, 1700000000123L);
            }
            v.setValueCount(3);
            try (Block block = LongArrowBufBlock.of(v, blockFactory)) {
                LongBlock lb = (LongBlock) block;
                assertTrue(lb.asVector() != null && lb.asVector().isConstant());
                assertEquals(1700000000123L, lb.getLong(0));
            }
        }
    }

    // ---- BIT (BooleanArrowBufBlock — bit-packed) ----

    public void testBitConstantTrue() {
        try (BitVector v = new BitVector("v", allocator)) {
            v.allocateNew(9);
            for (int i = 0; i < 9; i++) {
                v.set(i, 1);
            }
            v.setValueCount(9);
            try (Block block = BooleanArrowBufBlock.of(v, blockFactory)) {
                BooleanBlock bb = (BooleanBlock) block;
                assertTrue(bb.asVector() != null && bb.asVector().isConstant());
                for (int i = 0; i < 9; i++) {
                    assertTrue(bb.getBoolean(i));
                }
            }
        }
    }

    public void testBitConstantFalse() {
        try (BitVector v = new BitVector("v", allocator)) {
            v.allocateNew(16);
            for (int i = 0; i < 16; i++) {
                v.set(i, 0);
            }
            v.setValueCount(16);
            try (Block block = BooleanArrowBufBlock.of(v, blockFactory)) {
                BooleanBlock bb = (BooleanBlock) block;
                assertTrue(bb.asVector() != null && bb.asVector().isConstant());
                for (int i = 0; i < 16; i++) {
                    assertFalse(bb.getBoolean(i));
                }
            }
        }
    }

    public void testBitNotConstant() {
        try (BitVector v = new BitVector("v", allocator)) {
            v.allocateNew(4);
            v.set(0, 1);
            v.set(1, 1);
            v.set(2, 0);
            v.set(3, 1);
            v.setValueCount(4);
            try (Block block = BooleanArrowBufBlock.of(v, blockFactory)) {
                assertFalse(block.asVector() != null && block.asVector().isConstant());
                assertTrue(block instanceof BooleanArrowBufBlock);
            }
        }
    }

    // ---- VARCHAR ----

    public void testVarCharConstant() {
        try (VarCharVector v = new VarCharVector("v", allocator)) {
            v.allocateNew(4);
            byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < 4; i++) {
                v.setSafe(i, bytes);
            }
            v.setValueCount(4);
            try (Block block = BytesRefArrowBufBlock.of(v, blockFactory)) {
                BytesRefBlock bb = (BytesRefBlock) block;
                assertTrue(bb.asVector() != null && bb.asVector().isConstant());
                BytesRef scratch = new BytesRef();
                assertEquals(new BytesRef("hello"), bb.getBytesRef(0, scratch));
            }
        }
    }

    public void testVarCharNotConstantSameLength() {
        try (VarCharVector v = new VarCharVector("v", allocator)) {
            v.allocateNew(3);
            v.setSafe(0, "abc".getBytes(StandardCharsets.UTF_8));
            v.setSafe(1, "abc".getBytes(StandardCharsets.UTF_8));
            v.setSafe(2, "abd".getBytes(StandardCharsets.UTF_8));
            v.setValueCount(3);
            try (Block block = BytesRefArrowBufBlock.of(v, blockFactory)) {
                assertFalse(block.asVector() != null && block.asVector().isConstant());
                assertTrue(block instanceof BytesRefArrowBufBlock);
            }
        }
    }

    public void testVarCharNotConstantDifferentLength() {
        try (VarCharVector v = new VarCharVector("v", allocator)) {
            v.allocateNew(3);
            v.setSafe(0, "abc".getBytes(StandardCharsets.UTF_8));
            v.setSafe(1, "abc".getBytes(StandardCharsets.UTF_8));
            v.setSafe(2, "abcd".getBytes(StandardCharsets.UTF_8));
            v.setValueCount(3);
            try (Block block = BytesRefArrowBufBlock.of(v, blockFactory)) {
                assertFalse(block.asVector() != null && block.asVector().isConstant());
            }
        }
    }

    public void testVarBinaryConstant() {
        try (VarBinaryVector v = new VarBinaryVector("v", allocator)) {
            v.allocateNew(3);
            byte[] bytes = new byte[] { 1, 2, 3, 4 };
            for (int i = 0; i < 3; i++) {
                v.setSafe(i, bytes);
            }
            v.setValueCount(3);
            try (Block block = BytesRefArrowBufBlock.of(v, blockFactory)) {
                BytesRefBlock bb = (BytesRefBlock) block;
                assertTrue(bb.asVector() != null && bb.asVector().isConstant());
                BytesRef scratch = new BytesRef();
                assertEquals(new BytesRef(bytes), bb.getBytesRef(0, scratch));
            }
        }
    }

    public void testVarCharEmptyStrings() {
        try (VarCharVector v = new VarCharVector("v", allocator)) {
            v.allocateNew(5);
            byte[] empty = new byte[0];
            for (int i = 0; i < 5; i++) {
                v.setSafe(i, empty);
            }
            v.setValueCount(5);
            try (Block block = BytesRefArrowBufBlock.of(v, blockFactory)) {
                BytesRefBlock bb = (BytesRefBlock) block;
                assertTrue(bb.asVector() != null && bb.asVector().isConstant());
                BytesRef scratch = new BytesRef();
                assertEquals(0, bb.getBytesRef(0, scratch).length);
            }
        }
    }

    // ---- ListVector → no constant detection (zero-copy retained) ----

    public void testListVectorSkipsConstantDetection() {
        try (
            ListVector listVector = ListVector.empty("listInt", allocator);
            IntVector child = (IntVector) listVector.addOrGetVector(
                FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true))
            ).getVector()
        ) {
            // 2 rows, each with one value: [42], [42] — would be "constant" if flat,
            // but constant detection deliberately skips ListVectors.
            listVector.setInitialCapacity(2);
            listVector.allocateNew();
            child.allocateNew(2);
            child.set(0, 42);
            child.set(1, 42);
            child.setValueCount(2);
            listVector.getOffsetBuffer().setInt(0, 0);
            listVector.getOffsetBuffer().setInt(Integer.BYTES, 1);
            listVector.getOffsetBuffer().setInt(2L * Integer.BYTES, 2);
            org.apache.arrow.vector.BitVectorHelper.setBit(listVector.getValidityBuffer(), 0);
            org.apache.arrow.vector.BitVectorHelper.setBit(listVector.getValidityBuffer(), 1);
            listVector.setLastSet(1);
            listVector.setValueCount(2);

            try (Block block = IntArrowBufBlock.of(listVector, blockFactory)) {
                assertTrue(block instanceof IntArrowBufBlock);
            }
        }
    }

    // ---- helpers ----

    private static void fillInt(IntVector v, int[] values) {
        v.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            v.set(i, values[i]);
        }
        v.setValueCount(values.length);
    }

    private static void fillLong(BigIntVector v, long[] values) {
        v.allocateNew(values.length);
        for (int i = 0; i < values.length; i++) {
            v.set(i, values[i]);
        }
        v.setValueCount(values.length);
    }

    @SuppressWarnings("unused")
    private FieldVector unused() {
        // Keeps the FieldVector import live for cross-method type references in tests
        return null;
    }
}
