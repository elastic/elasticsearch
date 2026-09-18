/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

/**
 * Tests for {@link FloatToDoubleArrowBlock#of} on {@link ListVector} input. Verifies the
 * Arrow-list -> ESQL multi-value mapping rules documented on {@link ArrowListSupport}:
 * null lists, empty lists, and lists whose children are all null all collapse to an
 * ESQL null position; mixed lists drop their null children.
 */
public class FloatToDoubleArrowListTests extends ESTestCase {

    private static final double EPSILON = 0.0;

    private RootAllocator allocator;
    private BlockFactory blockFactory;

    @Before
    public void setup() {
        allocator = new RootAllocator();
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
    }

    @After
    public void cleanup() {
        allocator.close();
    }

    /**
     * Builds a {@link ListVector} with a {@link Float4Vector} child and the following layout:
     * <pre>
     *   Position 0: [1.0f, 2.0f]
     *   Position 1: null
     *   Position 2: [3.0f]
     *   Position 3: []
     *   Position 4: [null]
     *   Position 5: [null, null]
     *   Position 6: [4.0f, null, 5.0f]
     *   Position 7: [null, 6.0f, null]
     * </pre>
     */
    private ListVector createMixedListVector() {
        ListVector listVector = ListVector.empty("test", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.FLOAT4.getType()));

        listVector.allocateNew();
        Float4Vector child = (Float4Vector) listVector.getDataVector();
        child.allocateNew(12);

        child.set(0, 1.0f);
        child.set(1, 2.0f);
        child.set(2, 3.0f);
        child.setNull(3);
        child.setNull(4);
        child.setNull(5);
        child.set(6, 4.0f);
        child.setNull(7);
        child.set(8, 5.0f);
        child.setNull(9);
        child.set(10, 6.0f);
        child.setNull(11);
        child.setValueCount(12);

        ArrowBuf offsetBuf = listVector.getOffsetBuffer();
        offsetBuf.setInt(0, 0);
        offsetBuf.setInt(4, 2);
        offsetBuf.setInt(8, 2);
        offsetBuf.setInt(12, 3);
        offsetBuf.setInt(16, 3);
        offsetBuf.setInt(20, 4);
        offsetBuf.setInt(24, 6);
        offsetBuf.setInt(28, 9);
        offsetBuf.setInt(32, 12);

        ArrowBuf validityBuf = listVector.getValidityBuffer();
        validityBuf.setZero(0, validityBuf.capacity());
        BitVectorHelper.setBit(validityBuf, 0);
        BitVectorHelper.setBit(validityBuf, 2);
        BitVectorHelper.setBit(validityBuf, 3);
        BitVectorHelper.setBit(validityBuf, 4);
        BitVectorHelper.setBit(validityBuf, 5);
        BitVectorHelper.setBit(validityBuf, 6);
        BitVectorHelper.setBit(validityBuf, 7);

        listVector.setLastSet(7);
        listVector.setValueCount(8);
        return listVector;
    }

    public void testListMappingMatchesEsqlSemantics() {
        try (ListVector listVector = createMixedListVector()) {
            try (Block raw = FloatToDoubleArrowBlock.of(listVector, blockFactory)) {
                DoubleBlock block = (DoubleBlock) raw;
                assertEquals(8, block.getPositionCount());
                assertTrue(block.mayHaveNulls());
                assertTrue(block.mayHaveMultivaluedFields());

                assertFalse(block.isNull(0));
                assertEquals(2, block.getValueCount(0));
                int p0 = block.getFirstValueIndex(0);
                assertEquals(1.0, block.getDouble(p0), EPSILON);
                assertEquals(2.0, block.getDouble(p0 + 1), EPSILON);

                assertTrue(block.isNull(1));
                assertEquals(0, block.getValueCount(1));

                assertFalse(block.isNull(2));
                assertEquals(1, block.getValueCount(2));
                assertEquals(3.0, block.getDouble(block.getFirstValueIndex(2)), EPSILON);

                assertTrue(block.isNull(3));
                assertEquals(0, block.getValueCount(3));

                assertTrue(block.isNull(4));
                assertEquals(0, block.getValueCount(4));

                assertTrue(block.isNull(5));
                assertEquals(0, block.getValueCount(5));

                assertFalse(block.isNull(6));
                assertEquals(2, block.getValueCount(6));
                int p6 = block.getFirstValueIndex(6);
                assertEquals(4.0, block.getDouble(p6), EPSILON);
                assertEquals(5.0, block.getDouble(p6 + 1), EPSILON);

                assertFalse(block.isNull(7));
                assertEquals(1, block.getValueCount(7));
                assertEquals(6.0, block.getDouble(block.getFirstValueIndex(7)), EPSILON);

                assertEquals(6, block.getTotalValueCount());
            }
        }
    }
}
