/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

/**
 * Tests for {@link BooleanArrowBlock#of} on {@link ListVector} input. Verifies the
 * Arrow-list -> ESQL multi-value mapping rules documented on {@link ArrowListSupport}:
 * null lists, empty lists, and lists whose children are all null all collapse to an
 * ESQL null position; mixed lists drop their null children.
 */
public class BooleanArrowListTests extends ESTestCase {

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
     * Builds a {@link ListVector} with a {@link BitVector} child and the following layout:
     * <pre>
     *   Position 0: [true, false]            -- normal multi-value
     *   Position 1: null                     -- null list
     *   Position 2: [true]                   -- single non-null
     *   Position 3: []                       -- empty list
     *   Position 4: [null]                   -- single null child
     *   Position 5: [null, null]             -- all-null children
     *   Position 6: [false, null, true]      -- mixed (drops null in the middle)
     *   Position 7: [null, false, null]      -- single non-null among nulls
     * </pre>
     */
    private ListVector createMixedListVector() {
        ListVector listVector = ListVector.empty("test", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.BIT.getType()));

        listVector.allocateNew();
        BitVector child = (BitVector) listVector.getDataVector();
        child.allocateNew(12);

        child.set(0, 1);          // pos 0: true
        child.set(1, 0);          // pos 0: false
        child.set(2, 1);          // pos 2: true
        child.setNull(3);         // pos 4: null
        child.setNull(4);         // pos 5: null
        child.setNull(5);         // pos 5: null
        child.set(6, 0);          // pos 6: false
        child.setNull(7);         // pos 6: null (dropped)
        child.set(8, 1);          // pos 6: true
        child.setNull(9);         // pos 7: null (dropped)
        child.set(10, 0);         // pos 7: false
        child.setNull(11);        // pos 7: null (dropped)
        child.setValueCount(12);

        // Offsets: position p occupies child indices [offset[p], offset[p+1]).
        ArrowBuf offsetBuf = listVector.getOffsetBuffer();
        offsetBuf.setInt(0, 0);   // pos 0 start
        offsetBuf.setInt(4, 2);   // pos 1 start (also pos 0 end)
        offsetBuf.setInt(8, 2);   // pos 2 start (pos 1 is null, zero width)
        offsetBuf.setInt(12, 3);  // pos 3 start
        offsetBuf.setInt(16, 3);  // pos 4 start (pos 3 is empty, zero width)
        offsetBuf.setInt(20, 4);  // pos 5 start
        offsetBuf.setInt(24, 6);  // pos 6 start
        offsetBuf.setInt(28, 9);  // pos 7 start
        offsetBuf.setInt(32, 12); // end of pos 7

        ArrowBuf validityBuf = listVector.getValidityBuffer();
        validityBuf.setZero(0, validityBuf.capacity());
        BitVectorHelper.setBit(validityBuf, 0);
        // pos 1 stays unset -> null list
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
            try (Block raw = BooleanArrowBlock.of(listVector, blockFactory)) {
                BooleanBlock block = (BooleanBlock) raw;
                assertEquals(8, block.getPositionCount());
                assertTrue(block.mayHaveNulls());
                assertTrue(block.mayHaveMultivaluedFields());

                // Position 0: [true, false] -> multi-value of 2
                assertFalse(block.isNull(0));
                assertEquals(2, block.getValueCount(0));
                int p0 = block.getFirstValueIndex(0);
                assertTrue(block.getBoolean(p0));
                assertFalse(block.getBoolean(p0 + 1));

                // Position 1: null list -> ESQL null
                assertTrue(block.isNull(1));
                assertEquals(0, block.getValueCount(1));

                // Position 2: [true] -> single value
                assertFalse(block.isNull(2));
                assertEquals(1, block.getValueCount(2));
                assertTrue(block.getBoolean(block.getFirstValueIndex(2)));

                // Position 3: [] -> ESQL null (empty multi-value not representable)
                assertTrue(block.isNull(3));
                assertEquals(0, block.getValueCount(3));

                // Position 4: [null] -> ESQL null
                assertTrue(block.isNull(4));
                assertEquals(0, block.getValueCount(4));

                // Position 5: [null, null] -> ESQL null
                assertTrue(block.isNull(5));
                assertEquals(0, block.getValueCount(5));

                // Position 6: [false, null, true] -> multi-value [false, true]
                assertFalse(block.isNull(6));
                assertEquals(2, block.getValueCount(6));
                int p6 = block.getFirstValueIndex(6);
                assertFalse(block.getBoolean(p6));
                assertTrue(block.getBoolean(p6 + 1));

                // Position 7: [null, false, null] -> single value [false]
                assertFalse(block.isNull(7));
                assertEquals(1, block.getValueCount(7));
                assertFalse(block.getBoolean(block.getFirstValueIndex(7)));

                // Total values across all positions: 2 + 1 + 2 + 1 = 6
                assertEquals(6, block.getTotalValueCount());
            }
        }
    }
}
