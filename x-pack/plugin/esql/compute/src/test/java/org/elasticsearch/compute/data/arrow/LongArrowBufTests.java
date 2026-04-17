/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.compute.data.BasicBlockTests.assertDeepCopy;
import static org.elasticsearch.compute.data.BasicBlockTests.assertFilter;
import static org.elasticsearch.compute.data.BasicBlockTests.assertKeepMask;
import static org.elasticsearch.compute.data.BasicBlockTests.assertSlice;

/**
 * Tests for LongArrowBufVector and LongArrowBufBlock. These tests also apply to other blocks for fixed-length values:
 * int, float and double.
 */
public class LongArrowBufTests extends ESTestCase {

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

    public void testInvalidSize() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(1);
            arrowVec.set(0, 10L);
            arrowVec.setValueCount(1);

            assertThrows(IllegalArgumentException.class, () -> IntArrowBufVector.of(arrowVec, blockFactory));
            assertThrows(IllegalArgumentException.class, () -> IntArrowBufBlock.of(arrowVec, blockFactory));
        }
    }

    // -- Vector tests (from BigIntVector without nulls) --

    public void testVectorBasics() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(5);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.set(2, 30L);
            arrowVec.set(3, 40L);
            arrowVec.set(4, 50L);
            arrowVec.setValueCount(5);

            try (var vector = LongArrowBufVector.of(arrowVec, blockFactory)) {
                assertEquals(5, vector.getPositionCount());
                assertEquals(ElementType.LONG, vector.elementType());
                assertFalse(vector.isConstant());
                assertEquals(10L, vector.getLong(0));
                assertEquals(20L, vector.getLong(1));
                assertEquals(30L, vector.getLong(2));
                assertEquals(40L, vector.getLong(3));
                assertEquals(50L, vector.getLong(4));

                // assertKeepMask(vector); TODO doesn't return "this"
                assertFilter(vector);
                assertSlice(vector);
            }
        }
    }

    public void testVectorAsBlock() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 100L);
            arrowVec.set(1, 200L);
            arrowVec.set(2, 300L);
            arrowVec.setValueCount(3);

            // Do not close vector, asBlock() transfers ownership
            var vector = LongArrowBufVector.of(arrowVec, blockFactory);
            try (LongBlock block = vector.asBlock()) {
                assertEquals(3, block.getPositionCount());
                assertEquals(100L, block.getLong(0));
                assertEquals(200L, block.getLong(1));
                assertEquals(300L, block.getLong(2));
                assertFalse(block.mayHaveNulls());
                assertFalse(block.mayHaveMultivaluedFields());

                assertKeepMask(block);
                assertFilter(block);
                assertSlice(block);
                assertDeepCopy(block);
            }
        }
    }

    public void testVectorKeepMaskConstantTrue() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.set(2, 30L);
            arrowVec.setValueCount(3);

            try (var vector = LongArrowBufVector.of(arrowVec, blockFactory)) {
                try (BooleanVector mask = blockFactory.newConstantBooleanVector(true, 3)) {
                    try (LongBlock kept = vector.keepMask(mask)) {
                        assertEquals(3, kept.getPositionCount());
                        assertFalse(kept.mayHaveNulls());
                        assertEquals(10L, kept.getLong(kept.getFirstValueIndex(0)));
                        assertEquals(20L, kept.getLong(kept.getFirstValueIndex(1)));
                        assertEquals(30L, kept.getLong(kept.getFirstValueIndex(2)));
                    }
                }
            }
        }
    }

    public void testVectorKeepMaskConstantFalse() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.set(2, 30L);
            arrowVec.setValueCount(3);

            try (var vector = LongArrowBufVector.of(arrowVec, blockFactory)) {
                try (BooleanVector mask = blockFactory.newConstantBooleanVector(false, 3)) {
                    try (LongBlock kept = vector.keepMask(mask)) {
                        assertEquals(3, kept.getPositionCount());
                        assertTrue(kept.areAllValuesNull());
                    }
                }
            }
        }
    }

    public void testVectorKeepMaskMixed() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.set(2, 30L);
            arrowVec.set(3, 40L);
            arrowVec.setValueCount(4);

            try (var vector = LongArrowBufVector.of(arrowVec, blockFactory)) {
                try (var maskBuilder = blockFactory.newBooleanVectorFixedBuilder(4)) {
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(false);
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(false);
                    try (BooleanVector mask = maskBuilder.build()) {
                        try (LongBlock kept = vector.keepMask(mask)) {
                            assertEquals(4, kept.getPositionCount());
                            assertFalse(kept.isNull(0));
                            assertEquals(10L, kept.getLong(kept.getFirstValueIndex(0)));
                            assertTrue(kept.isNull(1));
                            assertFalse(kept.isNull(2));
                            assertEquals(30L, kept.getLong(kept.getFirstValueIndex(2)));
                            assertTrue(kept.isNull(3));
                        }
                    }
                }
            }
        }
    }

    public void testVectorLookup() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.set(2, 30L);
            arrowVec.set(3, 40L);
            arrowVec.setValueCount(4);

            try (var vector = LongArrowBufVector.of(arrowVec, blockFactory)) {
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(3)) {
                    posBuilder.appendInt(0);
                    posBuilder.appendInt(2);
                    posBuilder.appendInt(3);
                    try (IntBlock positions = posBuilder.build()) {
                        try (ReleasableIterator<? extends LongBlock> iter = vector.lookup(positions, ByteSizeValue.ofMb(1))) {
                            assertTrue(iter.hasNext());
                            try (LongBlock result = iter.next()) {
                                assertEquals(3, result.getPositionCount());
                                assertFalse(result.isNull(0));
                                assertEquals(10L, result.getLong(result.getFirstValueIndex(0)));
                                assertFalse(result.isNull(1));
                                assertEquals(30L, result.getLong(result.getFirstValueIndex(1)));
                                assertFalse(result.isNull(2));
                                assertEquals(40L, result.getLong(result.getFirstValueIndex(2)));
                            }
                            assertFalse(iter.hasNext());
                        }
                    }
                }
            }
        }
    }

    // -- Single-valued block tests (from BigIntVector) --

    public void testBlockWithNulls() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(5);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.setNull(2);
            arrowVec.set(3, 30L);
            arrowVec.set(4, 40L);
            arrowVec.setValueCount(5);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                assertEquals(5, block.getPositionCount());
                assertEquals(ElementType.LONG, block.elementType());
                assertTrue(block.mayHaveNulls());
                assertFalse(block.mayHaveMultivaluedFields());

                assertFalse(block.isNull(0));
                assertEquals(1, block.getValueCount(0));
                assertEquals(10L, block.getLong(0));

                assertFalse(block.isNull(1));
                assertEquals(20L, block.getLong(1));

                assertTrue(block.isNull(2));
                assertEquals(0, block.getValueCount(2));

                assertFalse(block.isNull(3));
                assertEquals(30L, block.getLong(3));

                assertFalse(block.isNull(4));
                assertEquals(40L, block.getLong(4));
            }
        }
    }

    public void testBlockNoNulls() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 100L);
            arrowVec.set(1, 200L);
            arrowVec.set(2, 300L);
            arrowVec.setValueCount(3);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                assertFalse(block.mayHaveNulls());
                assertFalse(block.areAllValuesNull());
                assertEquals(3, block.getTotalValueCount());

                // asVector() provides a view on the block and shouldn't be closed
                LongVector vec = block.asVector();
                assertNotNull(vec);
                assertEquals(3, vec.getPositionCount());
                assertEquals(100L, vec.getLong(0));
                assertEquals(200L, vec.getLong(1));
                assertEquals(300L, vec.getLong(2));
            }
        }
    }

    public void testBlockAllNulls() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.setNull(0);
            arrowVec.setNull(1);
            arrowVec.setNull(2);
            arrowVec.setValueCount(3);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                assertTrue(block.mayHaveNulls());
                assertTrue(block.areAllValuesNull());
                assertEquals(0, block.getTotalValueCount());
                assertNull(block.asVector());
            }
        }
    }

    public void testBlockGetTotalValueCount() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(5);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.setNull(2);
            arrowVec.set(3, 30L);
            arrowVec.setNull(4);
            arrowVec.setValueCount(5);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                assertEquals(3, block.getTotalValueCount());
            }
        }
    }

    public void testBlockFilter() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(5);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.setNull(2);
            arrowVec.set(3, 30L);
            arrowVec.set(4, 40L);
            arrowVec.setValueCount(5);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                try (LongBlock filtered = block.filter(false, 0, 2, 3)) {
                    assertEquals(3, filtered.getPositionCount());
                    assertFalse(filtered.isNull(0));
                    assertEquals(10L, filtered.getLong(0));
                    assertTrue(filtered.isNull(1));
                    assertFalse(filtered.isNull(2));
                    assertEquals(30L, filtered.getLong(2));
                }
            }
        }
    }

    public void testBlockFilterNoNulls() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.set(2, 30L);
            arrowVec.set(3, 40L);
            arrowVec.setValueCount(4);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                try (LongBlock filtered = block.filter(false, 1, 3)) {
                    assertEquals(2, filtered.getPositionCount());
                    assertEquals(20L, filtered.getLong(0));
                    assertEquals(40L, filtered.getLong(1));
                    assertFalse(filtered.mayHaveNulls());
                }
            }
        }
    }

    public void testBlockKeepMaskConstantTrue() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.set(2, 30L);
            arrowVec.setValueCount(3);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                try (BooleanVector mask = blockFactory.newConstantBooleanVector(true, 3)) {
                    try (LongBlock kept = block.keepMask(mask)) {
                        assertSame(block, kept);
                    }
                }
            }
        }
    }

    public void testBlockKeepMaskConstantFalse() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.set(2, 30L);
            arrowVec.setValueCount(3);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                try (BooleanVector mask = blockFactory.newConstantBooleanVector(false, 3)) {
                    try (Block kept = block.keepMask(mask)) {
                        assertEquals(3, kept.getPositionCount());
                        assertTrue(kept.areAllValuesNull());
                    }
                }
            }
        }
    }

    public void testBlockKeepMaskMixed() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.setNull(2);
            arrowVec.set(3, 30L);
            arrowVec.setValueCount(4);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                try (var maskBuilder = blockFactory.newBooleanVectorFixedBuilder(4)) {
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(false);
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(true);
                    try (BooleanVector mask = maskBuilder.build()) {
                        try (LongBlock kept = block.keepMask(mask)) {
                            assertEquals(4, kept.getPositionCount());
                            assertFalse(kept.isNull(0));
                            assertEquals(10L, kept.getLong(0));
                            assertTrue(kept.isNull(1));
                            assertTrue(kept.isNull(2));
                            assertFalse(kept.isNull(3));
                            assertEquals(30L, kept.getLong(3));
                        }
                    }
                }
            }
        }
    }

    public void testBlockExpandSingleValued() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.set(2, 30L);
            arrowVec.setValueCount(3);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                try (LongBlock expanded = block.expand()) {
                    assertSame(block, expanded);
                }
            }
        }
    }

    public void testBlockLookupSingleValued() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.setNull(2);
            arrowVec.set(3, 30L);
            arrowVec.setValueCount(4);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(3)) {
                    posBuilder.appendInt(0);
                    posBuilder.appendInt(2);
                    posBuilder.appendInt(3);
                    try (IntBlock positions = posBuilder.build()) {
                        try (ReleasableIterator<? extends LongBlock> iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                            assertTrue(iter.hasNext());
                            try (LongBlock result = iter.next()) {
                                assertEquals(3, result.getPositionCount());
                                assertFalse(result.isNull(0));
                                assertEquals(10L, result.getLong(result.getFirstValueIndex(0)));
                                assertTrue(result.isNull(1));
                                assertFalse(result.isNull(2));
                                assertEquals(30L, result.getLong(result.getFirstValueIndex(2)));
                            }
                            assertFalse(iter.hasNext());
                        }
                    }
                }
            }
        }
    }

    // -- Multi-valued block tests (from ListVector with BigIntVector child) --

    /**
     * Creates a ListVector with a BigIntVector child and the following layout:
     * <pre>
     *   Position 0: [100, 200, 300]
     *   Position 1: null
     *   Position 2: [400]
     *   Position 3: [500, 600]
     * </pre>
     */
    private ListVector createMultiValuedListVector() {
        ListVector listVector = ListVector.empty("test", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.BIGINT.getType()));

        // Allocate before populating to avoid buffer reallocation
        listVector.allocateNew();
        BigIntVector childVector = (BigIntVector) listVector.getDataVector();
        childVector.allocateNew(6);

        childVector.set(0, 100L);
        childVector.set(1, 200L);
        childVector.set(2, 300L);
        childVector.set(3, 400L);
        childVector.set(4, 500L);
        childVector.set(5, 600L);
        childVector.setValueCount(6);

        ArrowBuf offsetBuf = listVector.getOffsetBuffer();
        offsetBuf.setInt(0, 0);
        offsetBuf.setInt(4, 3);
        offsetBuf.setInt(8, 3);
        offsetBuf.setInt(12, 4);
        offsetBuf.setInt(16, 6);

        ArrowBuf validityBuf = listVector.getValidityBuffer();
        validityBuf.setZero(0, validityBuf.capacity());
        BitVectorHelper.setBit(validityBuf, 0);
        BitVectorHelper.setBit(validityBuf, 2);
        BitVectorHelper.setBit(validityBuf, 3);

        listVector.setLastSet(3);
        listVector.setValueCount(4);
        return listVector;
    }

    private LongArrowBufBlock blockFromListVector(ListVector listVector) {
        return LongArrowBufBlock.of(listVector, blockFactory);
    }

    public void testMultiValuedBlockBasics() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (LongArrowBufBlock block = blockFromListVector(listVector)) {
                assertEquals(4, block.getPositionCount());
                assertTrue(block.mayHaveNulls());
                assertTrue(block.mayHaveMultivaluedFields());
                assertNull(block.asVector());

                assertFalse(block.isNull(0));
                assertEquals(3, block.getValueCount(0));
                assertEquals(0, block.getFirstValueIndex(0));
                assertEquals(100L, block.getLong(0));
                assertEquals(200L, block.getLong(1));
                assertEquals(300L, block.getLong(2));

                assertTrue(block.isNull(1));
                assertEquals(0, block.getValueCount(1));

                assertFalse(block.isNull(2));
                assertEquals(1, block.getValueCount(2));
                assertEquals(400L, block.getLong(block.getFirstValueIndex(2)));

                assertFalse(block.isNull(3));
                assertEquals(2, block.getValueCount(3));
                int start3 = block.getFirstValueIndex(3);
                assertEquals(500L, block.getLong(start3));
                assertEquals(600L, block.getLong(start3 + 1));

                assertEquals(6, block.getTotalValueCount());
            }
        }
    }

    public void testMultiValuedFilter() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (LongArrowBufBlock block = blockFromListVector(listVector)) {
                try (LongBlock filtered = block.filter(false, 0, 2)) {
                    assertEquals(2, filtered.getPositionCount());

                    assertFalse(filtered.isNull(0));
                    assertEquals(3, filtered.getValueCount(0));
                    int start0 = filtered.getFirstValueIndex(0);
                    assertEquals(100L, filtered.getLong(start0));
                    assertEquals(200L, filtered.getLong(start0 + 1));
                    assertEquals(300L, filtered.getLong(start0 + 2));

                    assertFalse(filtered.isNull(1));
                    assertEquals(1, filtered.getValueCount(1));
                    assertEquals(400L, filtered.getLong(filtered.getFirstValueIndex(1)));
                }
            }
        }
    }

    public void testMultiValuedFilterIncludingNull() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (LongArrowBufBlock block = blockFromListVector(listVector)) {
                try (LongBlock filtered = block.filter(false, 1, 3)) {
                    assertEquals(2, filtered.getPositionCount());

                    assertTrue(filtered.isNull(0));

                    assertFalse(filtered.isNull(1));
                    assertEquals(2, filtered.getValueCount(1));
                    int start1 = filtered.getFirstValueIndex(1);
                    assertEquals(500L, filtered.getLong(start1));
                    assertEquals(600L, filtered.getLong(start1 + 1));
                }
            }
        }
    }

    public void testMultiValuedExpand() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (LongArrowBufBlock block = blockFromListVector(listVector)) {
                try (LongBlock expanded = block.expand()) {
                    assertNotSame(block, expanded);
                    // 6 total values become 6 positions (null position 1 maps to expanded pos 3)
                    assertEquals(6, expanded.getPositionCount());
                    assertFalse(expanded.mayHaveMultivaluedFields());

                    assertEquals(100L, expanded.getLong(0));
                    assertEquals(200L, expanded.getLong(1));
                    assertEquals(300L, expanded.getLong(2));
                    // Position 3 was null in the original (mapped from position 1 offset=3)
                    assertTrue(expanded.isNull(3));
                    assertEquals(500L, expanded.getLong(4));
                    assertEquals(600L, expanded.getLong(5));
                }
            }
        }
    }

    public void testMultiValuedExpandNoNulls() {
        try (ListVector listVector = ListVector.empty("test", allocator)) {
            listVector.addOrGetVector(FieldType.nullable(Types.MinorType.BIGINT.getType()));

            listVector.allocateNew();
            BigIntVector childVector = (BigIntVector) listVector.getDataVector();
            childVector.allocateNew(4);

            childVector.set(0, 10L);
            childVector.set(1, 20L);
            childVector.set(2, 30L);
            childVector.set(3, 40L);
            childVector.setValueCount(4);

            ArrowBuf offsetBuf = listVector.getOffsetBuffer();
            offsetBuf.setInt(0, 0);
            offsetBuf.setInt(4, 2);
            offsetBuf.setInt(8, 4);

            ArrowBuf validityBuf = listVector.getValidityBuffer();
            validityBuf.setZero(0, validityBuf.capacity());
            BitVectorHelper.setBit(validityBuf, 0);
            BitVectorHelper.setBit(validityBuf, 1);

            listVector.setLastSet(1);
            listVector.setValueCount(2);

            // No validity buffer passed → no nulls in the block
            try (var block = LongArrowBufBlock.of(listVector, blockFactory)) {
                try (LongBlock expanded = block.expand()) {
                    assertEquals(4, expanded.getPositionCount());
                    assertFalse(expanded.mayHaveNulls());
                    assertEquals(10L, expanded.getLong(0));
                    assertEquals(20L, expanded.getLong(1));
                    assertEquals(30L, expanded.getLong(2));
                    assertEquals(40L, expanded.getLong(3));
                }
            }
        }
    }

    public void testMultiValuedKeepMask() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (LongArrowBufBlock block = blockFromListVector(listVector)) {
                // Mask: [true, true, false, true] — position 2 becomes null
                try (var maskBuilder = blockFactory.newBooleanVectorFixedBuilder(4)) {
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(false);
                    maskBuilder.appendBoolean(true);
                    try (BooleanVector mask = maskBuilder.build()) {
                        try (LongBlock kept = block.keepMask(mask)) {
                            assertEquals(4, kept.getPositionCount());

                            assertFalse(kept.isNull(0));
                            assertEquals(3, kept.getValueCount(0));

                            // Was already null, stays null
                            assertTrue(kept.isNull(1));

                            // Was [400] but masked off
                            assertTrue(kept.isNull(2));

                            assertFalse(kept.isNull(3));
                            assertEquals(2, kept.getValueCount(3));
                        }
                    }
                }
            }
        }
    }

    public void testMultiValuedLookup() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (LongArrowBufBlock block = blockFromListVector(listVector)) {
                // Lookup positions: [0, 2, 3]
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(3)) {
                    posBuilder.appendInt(0);
                    posBuilder.appendInt(2);
                    posBuilder.appendInt(3);
                    try (IntBlock positions = posBuilder.build()) {
                        try (ReleasableIterator<? extends LongBlock> iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                            assertTrue(iter.hasNext());
                            try (LongBlock result = iter.next()) {
                                assertEquals(3, result.getPositionCount());

                                // Position 0 looked up [100, 200, 300]
                                assertFalse(result.isNull(0));
                                assertEquals(3, result.getValueCount(0));
                                int s0 = result.getFirstValueIndex(0);
                                assertEquals(100L, result.getLong(s0));
                                assertEquals(200L, result.getLong(s0 + 1));
                                assertEquals(300L, result.getLong(s0 + 2));

                                // Position 1 looked up [400]
                                assertFalse(result.isNull(1));
                                assertEquals(1, result.getValueCount(1));
                                assertEquals(400L, result.getLong(result.getFirstValueIndex(1)));

                                // Position 2 looked up [500, 600]
                                assertFalse(result.isNull(2));
                                assertEquals(2, result.getValueCount(2));
                                int s2 = result.getFirstValueIndex(2);
                                assertEquals(500L, result.getLong(s2));
                                assertEquals(600L, result.getLong(s2 + 1));
                            }
                            assertFalse(iter.hasNext());
                        }
                    }
                }
            }
        }
    }

    public void testMultiValuedLookupWithNullPosition() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (LongArrowBufBlock block = blockFromListVector(listVector)) {
                // Lookup position 1 which is null in the source block
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(2)) {
                    posBuilder.appendInt(1);
                    posBuilder.appendInt(0);
                    try (IntBlock positions = posBuilder.build()) {
                        try (ReleasableIterator<? extends LongBlock> iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                            assertTrue(iter.hasNext());
                            try (LongBlock result = iter.next()) {
                                assertEquals(2, result.getPositionCount());
                                assertTrue(result.isNull(0));
                                assertFalse(result.isNull(1));
                                assertEquals(3, result.getValueCount(1));
                            }
                            assertFalse(iter.hasNext());
                        }
                    }
                }
            }
        }
    }

    public void testMultiValuedGetTotalValueCount() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (LongArrowBufBlock block = blockFromListVector(listVector)) {
                // 3 (pos 0) + 0 (pos 1 null) + 1 (pos 2) + 2 (pos 3) = 6
                assertEquals(6, block.getTotalValueCount());
            }
        }
    }
}
