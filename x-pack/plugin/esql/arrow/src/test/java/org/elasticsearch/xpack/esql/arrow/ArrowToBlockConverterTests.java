/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
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

public class ArrowToBlockConverterTests extends ESTestCase {

    private RootAllocator allocator;
    private BlockFactory blockFactory;

    @Before
    public void setup() {
        allocator = new RootAllocator();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
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

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
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

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
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

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
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

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
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

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
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

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
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

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
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

    public void testFromFloat16() {
        try (Float2Vector vector = new Float2Vector("test", allocator)) {
            vector.allocateNew(4);
            vector.set(0, Float.floatToFloat16(1.5f));
            vector.set(1, Float.floatToFloat16(-3.0f));
            vector.setNull(2);
            vector.set(3, Float.floatToFloat16(65504.0f)); // max finite float16
            vector.setValueCount(4);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof DoubleBlock);
                DoubleBlock db = (DoubleBlock) block;
                assertEquals(4, db.getPositionCount());
                assertEquals(1.5, db.getDouble(0), 0.001);
                assertEquals(-3.0, db.getDouble(1), 0.001);
                assertTrue(db.isNull(2));
                assertEquals(65504.0, db.getDouble(3), 0.0);
            }
        }
    }

    public void testFromTinyInt() {
        try (TinyIntVector vector = new TinyIntVector("test", allocator)) {
            vector.allocateNew(4);
            vector.set(0, 0);
            vector.set(1, 127);
            vector.setNull(2);
            vector.set(3, -128);
            vector.setValueCount(4);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof IntBlock);
                IntBlock ib = (IntBlock) block;
                assertEquals(4, ib.getPositionCount());
                assertEquals(0, ib.getInt(0));
                assertEquals(127, ib.getInt(1));
                assertTrue(ib.isNull(2));
                assertEquals(-128, ib.getInt(3));
            }
        }
    }

    public void testFromSmallInt() {
        try (SmallIntVector vector = new SmallIntVector("test", allocator)) {
            vector.allocateNew(4);
            vector.set(0, 0);
            vector.set(1, 32767);
            vector.setNull(2);
            vector.set(3, -32768);
            vector.setValueCount(4);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof IntBlock);
                IntBlock ib = (IntBlock) block;
                assertEquals(4, ib.getPositionCount());
                assertEquals(0, ib.getInt(0));
                assertEquals(32767, ib.getInt(1));
                assertTrue(ib.isNull(2));
                assertEquals(-32768, ib.getInt(3));
            }
        }
    }

    public void testFromUInt1() {
        try (UInt1Vector vector = new UInt1Vector("test", allocator)) {
            vector.allocateNew(4);
            vector.set(0, 0);
            vector.set(1, 127);
            vector.setNull(2);
            vector.set(3, 255);
            vector.setValueCount(4);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof IntBlock);
                IntBlock ib = (IntBlock) block;
                assertEquals(4, ib.getPositionCount());
                assertEquals(0, ib.getInt(0));
                assertEquals(127, ib.getInt(1));
                assertTrue(ib.isNull(2));
                assertEquals(255, ib.getInt(3));
            }
        }
    }

    public void testFromUInt2() {
        try (UInt2Vector vector = new UInt2Vector("test", allocator)) {
            vector.allocateNew(4);
            vector.set(0, 0);
            vector.set(1, 1000);
            vector.setNull(2);
            vector.set(3, 65535);
            vector.setValueCount(4);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof IntBlock);
                IntBlock ib = (IntBlock) block;
                assertEquals(4, ib.getPositionCount());
                assertEquals(0, ib.getInt(0));
                assertEquals(1000, ib.getInt(1));
                assertTrue(ib.isNull(2));
                assertEquals(65535, ib.getInt(3));
            }
        }
    }

    public void testFromUInt4() {
        try (UInt4Vector vector = new UInt4Vector("test", allocator)) {
            vector.allocateNew(4);
            vector.set(0, 0);
            vector.set(1, 100_000);
            vector.setNull(2);
            vector.set(3, 0xFFFFFFFF); // unsigned: 4294967295
            vector.setValueCount(4);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof LongBlock);
                LongBlock lb = (LongBlock) block;
                assertEquals(4, lb.getPositionCount());
                assertEquals(0L, lb.getLong(0));
                assertEquals(100_000L, lb.getLong(1));
                assertTrue(lb.isNull(2));
                assertEquals(4294967295L, lb.getLong(3));
            }
        }
    }

    public void testFromTimestampSec() {
        try (TimeStampSecVector vector = new TimeStampSecVector("test", allocator)) {
            vector.allocateNew(3);
            vector.set(0, 1_700_000_000L); // seconds since epoch
            vector.setNull(1);
            vector.set(2, 0L);
            vector.setValueCount(3);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof LongBlock);
                LongBlock lb = (LongBlock) block;
                assertEquals(3, lb.getPositionCount());
                assertEquals(1_700_000_000_000L, lb.getLong(0)); // seconds * 1000
                assertTrue(lb.isNull(1));
                assertEquals(0L, lb.getLong(2));
            }
        }
    }

    public void testFromTimestampMilli() {
        try (TimeStampMilliVector vector = new TimeStampMilliVector("test", allocator)) {
            vector.allocateNew(3);
            vector.set(0, 1_700_000_000_000L); // millis since epoch
            vector.setNull(1);
            vector.set(2, 42L);
            vector.setValueCount(3);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof LongBlock);
                LongBlock lb = (LongBlock) block;
                assertEquals(3, lb.getPositionCount());
                assertEquals(1_700_000_000_000L, lb.getLong(0)); // direct, no conversion
                assertTrue(lb.isNull(1));
                assertEquals(42L, lb.getLong(2));
            }
        }
    }

    public void testFromTimestampMicro() {
        try (TimeStampMicroVector vector = new TimeStampMicroVector("test", allocator)) {
            vector.allocateNew(3);
            vector.set(0, 1_700_000_000_000_000L); // micros since epoch
            vector.setNull(1);
            vector.set(2, 42L);
            vector.setValueCount(3);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof LongBlock);
                LongBlock lb = (LongBlock) block;
                assertEquals(3, lb.getPositionCount());
                assertEquals(1_700_000_000_000_000_000L, lb.getLong(0)); // micros * 1000
                assertTrue(lb.isNull(1));
                assertEquals(42_000L, lb.getLong(2));
            }
        }
    }

    public void testFromTimestampNano() {
        try (TimeStampNanoVector vector = new TimeStampNanoVector("test", allocator)) {
            vector.allocateNew(3);
            vector.set(0, 1_700_000_000_000_000_000L); // nanos since epoch
            vector.setNull(1);
            vector.set(2, 42_000L);
            vector.setValueCount(3);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof LongBlock);
                LongBlock lb = (LongBlock) block;
                assertEquals(3, lb.getPositionCount());
                assertEquals(1_700_000_000_000_000_000L, lb.getLong(0)); // direct, no conversion
                assertTrue(lb.isNull(1));
                assertEquals(42_000L, lb.getLong(2));
            }
        }
    }

    public void testForTypeFactory() {
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.FLOAT2));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.FLOAT4));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.FLOAT8));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.TINYINT));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.SMALLINT));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.INT));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.BIGINT));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.UINT1));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.UINT2));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.UINT4));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.BIT));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.VARCHAR));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.VARBINARY));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.TIMESTAMPSEC));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.TIMESTAMPSECTZ));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.TIMESTAMPMILLI));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.TIMESTAMPMILLITZ));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.TIMESTAMPMICRO));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.TIMESTAMPMICROTZ));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.TIMESTAMPNANO));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.TIMESTAMPNANOTZ));
        assertNotNull(ArrowToBlockConverter.forType(Types.MinorType.LIST));
        assertNull(ArrowToBlockConverter.forType(Types.MinorType.NULL));
        assertNull(ArrowToBlockConverter.forType(Types.MinorType.STRUCT));
    }

    public void testFromFloat32() {
        try (Float4Vector vector = new Float4Vector("test", allocator)) {
            vector.allocateNew(4);
            vector.set(0, 1.5f);
            vector.set(1, -3.0f);
            vector.setNull(2);
            vector.set(3, 0.125f);
            vector.setValueCount(4);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
            try (Block block = converter.convert(vector, blockFactory)) {
                assertTrue(block instanceof DoubleBlock);
                DoubleBlock db = (DoubleBlock) block;
                assertEquals(4, db.getPositionCount());
                assertEquals(1.5, db.getDouble(0), 0.0);
                assertEquals(-3.0, db.getDouble(1), 0.0);
                assertTrue(db.isNull(2));
                assertEquals(0.125, db.getDouble(3), 0.0);
            }
        }
    }

    /**
     * LIST&lt;FLOAT4&gt; is rejected at conversion time, mirroring LIST&lt;BIGINT&gt;/LIST&lt;INT&gt;:
     * the registered {@code Float32ArrowBufBlock} converter inherits the zero-copy
     * {@code AbstractArrowBufBlock} list path which throws on null children and yields malformed
     * blocks for empty lists in non-null positions, so the LIST registry refuses to dispatch to
     * it. See {@code ArrowToBlockConverters#SUPPORTED_LIST_CHILD_TYPES}.
     */
    public void testConvertListOfFloat32ThrowsUnsupported() {
        ListVector listVector = ListVector.empty("test", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.FLOAT4.getType()));
        listVector.setValueCount(0);

        try (listVector) {
            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(Types.MinorType.LIST);
            expectThrows(UnsupportedOperationException.class, () -> converter.convert(listVector, blockFactory));
        }
    }

    public void testFromListOfBoolean() {
        ListVector listVector = ListVector.empty("test", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.BIT.getType()));
        listVector.allocateNew();
        BitVector child = (BitVector) listVector.getDataVector();
        child.allocateNew(2);
        child.set(0, 1);
        child.set(1, 0);
        child.setValueCount(2);

        // pos 0: [true, false], pos 1: null list
        ArrowBuf offsetBuf = listVector.getOffsetBuffer();
        offsetBuf.setInt(0, 0);
        offsetBuf.setInt(4, 2);
        offsetBuf.setInt(8, 2);

        ArrowBuf validityBuf = listVector.getValidityBuffer();
        validityBuf.setZero(0, validityBuf.capacity());
        BitVectorHelper.setBit(validityBuf, 0);

        listVector.setLastSet(1);
        listVector.setValueCount(2);

        try (listVector) {
            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(Types.MinorType.LIST);
            try (Block block = converter.convert(listVector, blockFactory)) {
                assertTrue(block instanceof BooleanBlock);
                BooleanBlock bb = (BooleanBlock) block;
                assertEquals(2, bb.getPositionCount());
                assertEquals(2, bb.getValueCount(0));
                int p0 = bb.getFirstValueIndex(0);
                assertTrue(bb.getBoolean(p0));
                assertFalse(bb.getBoolean(p0 + 1));
                assertTrue(bb.isNull(1));
            }
        }
    }

    public void testFromListOfVarChar() {
        ListVector listVector = ListVector.empty("test", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.VARCHAR.getType()));
        listVector.allocateNew();
        VarCharVector child = (VarCharVector) listVector.getDataVector();
        child.allocateNew();
        child.set(0, "hello".getBytes(StandardCharsets.UTF_8));
        child.set(1, "world".getBytes(StandardCharsets.UTF_8));
        child.setValueCount(2);

        // pos 0: ["hello", "world"], pos 1: null list
        ArrowBuf offsetBuf = listVector.getOffsetBuffer();
        offsetBuf.setInt(0, 0);
        offsetBuf.setInt(4, 2);
        offsetBuf.setInt(8, 2);

        ArrowBuf validityBuf = listVector.getValidityBuffer();
        validityBuf.setZero(0, validityBuf.capacity());
        BitVectorHelper.setBit(validityBuf, 0);

        listVector.setLastSet(1);
        listVector.setValueCount(2);

        try (listVector) {
            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(Types.MinorType.LIST);
            try (Block block = converter.convert(listVector, blockFactory)) {
                assertTrue(block instanceof BytesRefBlock);
                BytesRefBlock bb = (BytesRefBlock) block;
                assertEquals(2, bb.getPositionCount());
                assertEquals(2, bb.getValueCount(0));
                int p0 = bb.getFirstValueIndex(0);
                assertEquals(new BytesRef("hello"), bb.getBytesRef(p0, new BytesRef()));
                assertEquals(new BytesRef("world"), bb.getBytesRef(p0 + 1, new BytesRef()));
                assertTrue(bb.isNull(1));
            }
        }
    }

    public void testConvertListUnsupportedElementTypeThrows() {
        ListVector listVector = ListVector.empty("test", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.STRUCT.getType()));
        listVector.setValueCount(0);

        try (listVector) {
            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(Types.MinorType.LIST);
            expectThrows(UnsupportedOperationException.class, () -> converter.convert(listVector, blockFactory));
        }
    }

    /**
     * LIST&lt;BIGINT&gt; is rejected even though BIGINT itself has a registered (zero-copy) converter:
     * the zero-copy {@code AbstractArrowBufBlock} list path throws on null children and produces
     * malformed blocks for empty lists in non-null positions, so we don't dispatch to it. See
     * {@code ArrowToBlockConverters#SUPPORTED_LIST_CHILD_TYPES}.
     */
    public void testConvertListOfBigIntThrowsUnsupported() {
        ListVector listVector = ListVector.empty("test", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.BIGINT.getType()));
        listVector.setValueCount(0);

        try (listVector) {
            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(Types.MinorType.LIST);
            expectThrows(UnsupportedOperationException.class, () -> converter.convert(listVector, blockFactory));
        }
    }

    public void testIsListChildTypeSupported() {
        assertTrue(ArrowToBlockConverter.isListChildTypeSupported(Types.MinorType.BIT));
        assertTrue(ArrowToBlockConverter.isListChildTypeSupported(Types.MinorType.VARCHAR));
        assertTrue(ArrowToBlockConverter.isListChildTypeSupported(Types.MinorType.VARBINARY));

        // Registered as a flat converter but not yet list-safe.
        assertFalse(ArrowToBlockConverter.isListChildTypeSupported(Types.MinorType.BIGINT));
        assertFalse(ArrowToBlockConverter.isListChildTypeSupported(Types.MinorType.INT));
        assertFalse(ArrowToBlockConverter.isListChildTypeSupported(Types.MinorType.FLOAT4));
        assertFalse(ArrowToBlockConverter.isListChildTypeSupported(Types.MinorType.FLOAT8));
        assertFalse(ArrowToBlockConverter.isListChildTypeSupported(Types.MinorType.TIMESTAMPMILLI));

        // Not registered at all.
        assertFalse(ArrowToBlockConverter.isListChildTypeSupported(Types.MinorType.STRUCT));
        assertFalse(ArrowToBlockConverter.isListChildTypeSupported(Types.MinorType.LIST));
    }

    public void testFromFloat64EmptyVector() {
        try (Float8Vector vector = new Float8Vector("test", allocator)) {
            vector.allocateNew(0);
            vector.setValueCount(0);

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
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

            ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
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
                    ArrowToBlockConverter converter = ArrowToBlockConverter.forType(vector.getMinorType());
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
