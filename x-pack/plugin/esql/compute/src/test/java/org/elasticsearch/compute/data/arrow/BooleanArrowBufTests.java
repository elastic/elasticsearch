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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.compute.data.BasicBlockTests.assertDeepCopy;
import static org.elasticsearch.compute.data.BasicBlockTests.assertFilter;
import static org.elasticsearch.compute.data.BasicBlockTests.assertKeepMask;
import static org.elasticsearch.compute.data.BasicBlockTests.assertSlice;

/**
 * Tests for {@link BooleanArrowBufVector} and {@link BooleanArrowBufBlock}. Booleans are bit-packed in Arrow,
 * so these tests verify the specialized bit-level implementations.
 */
public class BooleanArrowBufTests extends ESTestCase {

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

    // -- Vector tests (from BitVector without nulls) --

    public void testVectorBasics() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(5);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.set(2, 1);
            arrowVec.set(3, 1);
            arrowVec.set(4, 0);
            arrowVec.setValueCount(5);

            try (var vector = BooleanArrowBufVector.of(arrowVec, blockFactory)) {
                assertEquals(5, vector.getPositionCount());
                assertEquals(ElementType.BOOLEAN, vector.elementType());
                assertFalse(vector.isConstant());
                assertTrue(vector.getBoolean(0));
                assertFalse(vector.getBoolean(1));
                assertTrue(vector.getBoolean(2));
                assertTrue(vector.getBoolean(3));
                assertFalse(vector.getBoolean(4));

                // assertKeepMask(vector); TODO doesn't return "this"
                assertFilter(vector);
                assertSlice(vector);
            }
        }
    }

    public void testVectorAllTrue() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 1);
            arrowVec.set(1, 1);
            arrowVec.set(2, 1);
            arrowVec.setValueCount(3);

            try (var vector = BooleanArrowBufVector.of(arrowVec, blockFactory)) {
                assertTrue(vector.allTrue());
                assertFalse(vector.allFalse());
            }
        }
    }

    public void testVectorAllFalse() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 0);
            arrowVec.set(1, 0);
            arrowVec.set(2, 0);
            arrowVec.setValueCount(3);

            try (var vector = BooleanArrowBufVector.of(arrowVec, blockFactory)) {
                assertFalse(vector.allTrue());
                assertTrue(vector.allFalse());
            }
        }
    }

    public void testVectorAsBlock() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.set(2, 1);
            arrowVec.setValueCount(3);

            // Don't close vector, asBlock() takes ownership
            var vector = BooleanArrowBufVector.of(arrowVec, blockFactory);
            try (BooleanBlock block = vector.asBlock()) {
                assertEquals(3, block.getPositionCount());
                assertTrue(block.getBoolean(0));
                assertFalse(block.getBoolean(1));
                assertTrue(block.getBoolean(2));
                assertFalse(block.mayHaveNulls());
                assertFalse(block.mayHaveMultivaluedFields());

                assertKeepMask(block);
                assertFilter(block);
                assertSlice(block);
                assertDeepCopy(block);
            }
        }
    }

    public void testVectorFilter() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(5);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.set(2, 1);
            arrowVec.set(3, 0);
            arrowVec.set(4, 1);
            arrowVec.setValueCount(5);

            try (var vector = BooleanArrowBufVector.of(arrowVec, blockFactory)) {
                try (BooleanVector filtered = vector.filter(false, 0, 2, 4)) {
                    assertEquals(3, filtered.getPositionCount());
                    assertTrue(filtered.getBoolean(0));
                    assertTrue(filtered.getBoolean(1));
                    assertTrue(filtered.getBoolean(2));
                }
            }
        }
    }

    // -- Single-valued block tests (from BitVector) --

    public void testBlockWithNulls() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(5);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.setNull(2);
            arrowVec.set(3, 1);
            arrowVec.set(4, 0);
            arrowVec.setValueCount(5);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
                assertEquals(5, block.getPositionCount());
                assertEquals(ElementType.BOOLEAN, block.elementType());
                assertTrue(block.mayHaveNulls());
                assertFalse(block.mayHaveMultivaluedFields());

                assertFalse(block.isNull(0));
                assertEquals(1, block.getValueCount(0));
                assertTrue(block.getBoolean(0));

                assertFalse(block.isNull(1));
                assertFalse(block.getBoolean(1));

                assertTrue(block.isNull(2));
                assertEquals(0, block.getValueCount(2));

                assertFalse(block.isNull(3));
                assertTrue(block.getBoolean(3));

                assertFalse(block.isNull(4));
                assertFalse(block.getBoolean(4));
            }
        }
    }

    public void testBlockNoNulls() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.set(2, 1);
            arrowVec.setValueCount(3);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
                assertFalse(block.mayHaveNulls());
                assertFalse(block.areAllValuesNull());
                assertEquals(3, block.getTotalValueCount());

                BooleanVector vec = block.asVector();
                assertNotNull(vec);
                assertEquals(3, vec.getPositionCount());
                assertTrue(vec.getBoolean(0));
                assertFalse(vec.getBoolean(1));
                assertTrue(vec.getBoolean(2));
            }
        }
    }

    public void testBlockAllNulls() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.setNull(0);
            arrowVec.setNull(1);
            arrowVec.setNull(2);
            arrowVec.setValueCount(3);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
                assertTrue(block.mayHaveNulls());
                assertTrue(block.areAllValuesNull());
                assertEquals(0, block.getTotalValueCount());
                assertNull(block.asVector());
            }
        }
    }

    public void testBlockGetTotalValueCount() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(5);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.setNull(2);
            arrowVec.set(3, 1);
            arrowVec.setNull(4);
            arrowVec.setValueCount(5);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
                assertEquals(3, block.getTotalValueCount());
            }
        }
    }

    public void testBlockFilter() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(5);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.setNull(2);
            arrowVec.set(3, 1);
            arrowVec.set(4, 0);
            arrowVec.setValueCount(5);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
                try (BooleanBlock filtered = block.filter(false, 0, 2, 3)) {
                    assertEquals(3, filtered.getPositionCount());
                    assertFalse(filtered.isNull(0));
                    assertTrue(filtered.getBoolean(0));
                    assertTrue(filtered.isNull(1));
                    assertFalse(filtered.isNull(2));
                    assertTrue(filtered.getBoolean(2));
                }
            }
        }
    }

    public void testBlockFilterNoNulls() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.set(2, 1);
            arrowVec.set(3, 0);
            arrowVec.setValueCount(4);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
                try (BooleanBlock filtered = block.filter(false, 1, 3)) {
                    assertEquals(2, filtered.getPositionCount());
                    assertFalse(filtered.getBoolean(0));
                    assertFalse(filtered.getBoolean(1));
                    assertFalse(filtered.mayHaveNulls());
                }
            }
        }
    }

    public void testBlockKeepMaskConstantTrue() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.set(2, 1);
            arrowVec.setValueCount(3);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
                try (BooleanVector mask = blockFactory.newConstantBooleanVector(true, 3)) {
                    try (BooleanBlock kept = block.keepMask(mask)) {
                        assertSame(block, kept);
                    }
                }
            }
        }
    }

    public void testBlockKeepMaskConstantFalse() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.set(2, 1);
            arrowVec.setValueCount(3);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
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
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.setNull(2);
            arrowVec.set(3, 1);
            arrowVec.setValueCount(4);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
                try (var maskBuilder = blockFactory.newBooleanVectorFixedBuilder(4)) {
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(false);
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(true);
                    try (BooleanVector mask = maskBuilder.build()) {
                        try (BooleanBlock kept = block.keepMask(mask)) {
                            assertEquals(4, kept.getPositionCount());
                            assertFalse(kept.isNull(0));
                            assertTrue(kept.getBoolean(0));
                            assertTrue(kept.isNull(1));
                            assertTrue(kept.isNull(2));
                            assertFalse(kept.isNull(3));
                            assertTrue(kept.getBoolean(3));
                        }
                    }
                }
            }
        }
    }

    public void testBlockExpandSingleValued() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.set(2, 1);
            arrowVec.setValueCount(3);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
                try (BooleanBlock expanded = block.expand()) {
                    assertSame(block, expanded);
                }
            }
        }
    }

    public void testBlockLookupSingleValued() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.setNull(2);
            arrowVec.set(3, 1);
            arrowVec.setValueCount(4);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(3)) {
                    posBuilder.appendInt(0);
                    posBuilder.appendInt(2);
                    posBuilder.appendInt(3);
                    try (IntBlock positions = posBuilder.build()) {
                        try (ReleasableIterator<? extends BooleanBlock> iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                            assertTrue(iter.hasNext());
                            try (BooleanBlock result = iter.next()) {
                                assertEquals(3, result.getPositionCount());
                                assertFalse(result.isNull(0));
                                assertTrue(result.getBoolean(result.getFirstValueIndex(0)));
                                assertTrue(result.isNull(1));
                                assertFalse(result.isNull(2));
                                assertTrue(result.getBoolean(result.getFirstValueIndex(2)));
                            }
                            assertFalse(iter.hasNext());
                        }
                    }
                }
            }
        }
    }

    public void testBlockToMask() {
        try (BitVector arrowVec = new BitVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 1);
            arrowVec.set(1, 0);
            arrowVec.setNull(2);
            arrowVec.set(3, 1);
            arrowVec.setValueCount(4);

            try (var block = BooleanArrowBufBlock.of(arrowVec, blockFactory)) {
                try (var toMask = block.toMask()) {
                    assertFalse(toMask.hadMultivaluedFields());
                    BooleanVector mask = toMask.mask();
                    assertEquals(4, mask.getPositionCount());
                    assertTrue(mask.getBoolean(0));
                    assertFalse(mask.getBoolean(1));
                    assertFalse(mask.getBoolean(2));
                    assertTrue(mask.getBoolean(3));
                }
            }
        }
    }

    // -- Multi-valued block tests (from ListVector with BitVector child) --

    /**
     * Creates a ListVector with a BitVector child and the following layout:
     * <pre>
     *   Position 0: [true, false, true]
     *   Position 1: null
     *   Position 2: [false]
     *   Position 3: [true, true]
     * </pre>
     */
    private ListVector createMultiValuedListVector() {
        ListVector listVector = ListVector.empty("test", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.BIT.getType()));

        listVector.allocateNew();
        BitVector childVector = (BitVector) listVector.getDataVector();
        childVector.allocateNew(6);

        childVector.set(0, 1);  // true
        childVector.set(1, 0);  // false
        childVector.set(2, 1);  // true
        childVector.set(3, 0);  // false
        childVector.set(4, 1);  // true
        childVector.set(5, 1);  // true
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

    private BooleanArrowBufBlock blockFromListVector(ListVector listVector) {
        BitVector childVector = (BitVector) listVector.getDataVector();
        ArrowUtils.retainBuffers(childVector.getDataBuffer(), listVector.getValidityBuffer(), listVector.getOffsetBuffer());
        return new BooleanArrowBufBlock(
            childVector.getDataBuffer(),
            listVector.getValidityBuffer(),
            listVector.getOffsetBuffer(),
            listVector.getValueCount(),
            listVector.getValueCount() + 1,
            blockFactory
        );
    }

    public void testMultiValuedBlockBasics() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (BooleanArrowBufBlock block = blockFromListVector(listVector)) {
                assertEquals(4, block.getPositionCount());
                assertTrue(block.mayHaveNulls());
                assertTrue(block.mayHaveMultivaluedFields());
                assertNull(block.asVector());

                assertFalse(block.isNull(0));
                assertEquals(3, block.getValueCount(0));
                assertEquals(0, block.getFirstValueIndex(0));
                assertTrue(block.getBoolean(0));
                assertFalse(block.getBoolean(1));
                assertTrue(block.getBoolean(2));

                assertTrue(block.isNull(1));
                assertEquals(0, block.getValueCount(1));

                assertFalse(block.isNull(2));
                assertEquals(1, block.getValueCount(2));
                assertFalse(block.getBoolean(block.getFirstValueIndex(2)));

                assertFalse(block.isNull(3));
                assertEquals(2, block.getValueCount(3));
                int start3 = block.getFirstValueIndex(3);
                assertTrue(block.getBoolean(start3));
                assertTrue(block.getBoolean(start3 + 1));

                assertEquals(6, block.getTotalValueCount());
            }
        }
    }

    public void testMultiValuedFilter() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (BooleanArrowBufBlock block = blockFromListVector(listVector)) {
                try (BooleanBlock filtered = block.filter(false, 0, 2)) {
                    assertEquals(2, filtered.getPositionCount());

                    assertFalse(filtered.isNull(0));
                    assertEquals(3, filtered.getValueCount(0));
                    int start0 = filtered.getFirstValueIndex(0);
                    assertTrue(filtered.getBoolean(start0));
                    assertFalse(filtered.getBoolean(start0 + 1));
                    assertTrue(filtered.getBoolean(start0 + 2));

                    assertFalse(filtered.isNull(1));
                    assertEquals(1, filtered.getValueCount(1));
                    assertFalse(filtered.getBoolean(filtered.getFirstValueIndex(1)));
                }
            }
        }
    }

    public void testMultiValuedFilterIncludingNull() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (BooleanArrowBufBlock block = blockFromListVector(listVector)) {
                try (BooleanBlock filtered = block.filter(false, 1, 3)) {
                    assertEquals(2, filtered.getPositionCount());

                    assertTrue(filtered.isNull(0));

                    assertFalse(filtered.isNull(1));
                    assertEquals(2, filtered.getValueCount(1));
                    int start1 = filtered.getFirstValueIndex(1);
                    assertTrue(filtered.getBoolean(start1));
                    assertTrue(filtered.getBoolean(start1 + 1));
                }
            }
        }
    }

    public void testMultiValuedExpand() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (BooleanArrowBufBlock block = blockFromListVector(listVector)) {
                try (BooleanBlock expanded = block.expand()) {
                    assertNotSame(block, expanded);
                    assertEquals(6, expanded.getPositionCount());
                    assertFalse(expanded.mayHaveMultivaluedFields());

                    assertTrue(expanded.getBoolean(0));
                    assertFalse(expanded.getBoolean(1));
                    assertTrue(expanded.getBoolean(2));
                    assertTrue(expanded.isNull(3));
                    assertTrue(expanded.getBoolean(4));
                    assertTrue(expanded.getBoolean(5));
                }
            }
        }
    }

    public void testMultiValuedExpandNoNulls() {
        try (ListVector listVector = ListVector.empty("test", allocator)) {
            listVector.addOrGetVector(FieldType.nullable(Types.MinorType.BIT.getType()));

            listVector.allocateNew();
            BitVector childVector = (BitVector) listVector.getDataVector();
            childVector.allocateNew(4);

            childVector.set(0, 1);
            childVector.set(1, 0);
            childVector.set(2, 1);
            childVector.set(3, 1);
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

            try (
                var block = new BooleanArrowBufBlock(childVector.getDataBuffer(), null, listVector.getOffsetBuffer(), 2, 3, blockFactory)
            ) {
                block.retainBuffers();
                try (BooleanBlock expanded = block.expand()) {
                    assertEquals(4, expanded.getPositionCount());
                    assertFalse(expanded.mayHaveNulls());
                    assertTrue(expanded.getBoolean(0));
                    assertFalse(expanded.getBoolean(1));
                    assertTrue(expanded.getBoolean(2));
                    assertTrue(expanded.getBoolean(3));
                }
            }
        }
    }

    public void testMultiValuedKeepMask() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (BooleanArrowBufBlock block = blockFromListVector(listVector)) {
                try (var maskBuilder = blockFactory.newBooleanVectorFixedBuilder(4)) {
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(false);
                    maskBuilder.appendBoolean(true);
                    try (BooleanVector mask = maskBuilder.build()) {
                        try (BooleanBlock kept = block.keepMask(mask)) {
                            assertEquals(4, kept.getPositionCount());

                            assertFalse(kept.isNull(0));
                            assertEquals(3, kept.getValueCount(0));

                            assertTrue(kept.isNull(1));

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
            try (BooleanArrowBufBlock block = blockFromListVector(listVector)) {
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(3)) {
                    posBuilder.appendInt(0);
                    posBuilder.appendInt(2);
                    posBuilder.appendInt(3);
                    try (IntBlock positions = posBuilder.build()) {
                        try (ReleasableIterator<? extends BooleanBlock> iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                            assertTrue(iter.hasNext());
                            try (BooleanBlock result = iter.next()) {
                                assertEquals(3, result.getPositionCount());

                                assertFalse(result.isNull(0));
                                assertEquals(3, result.getValueCount(0));
                                int s0 = result.getFirstValueIndex(0);
                                assertTrue(result.getBoolean(s0));
                                assertFalse(result.getBoolean(s0 + 1));
                                assertTrue(result.getBoolean(s0 + 2));

                                assertFalse(result.isNull(1));
                                assertEquals(1, result.getValueCount(1));
                                assertFalse(result.getBoolean(result.getFirstValueIndex(1)));

                                assertFalse(result.isNull(2));
                                assertEquals(2, result.getValueCount(2));
                                int s2 = result.getFirstValueIndex(2);
                                assertTrue(result.getBoolean(s2));
                                assertTrue(result.getBoolean(s2 + 1));
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
            try (BooleanArrowBufBlock block = blockFromListVector(listVector)) {
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(2)) {
                    posBuilder.appendInt(1);
                    posBuilder.appendInt(0);
                    try (IntBlock positions = posBuilder.build()) {
                        try (ReleasableIterator<? extends BooleanBlock> iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                            assertTrue(iter.hasNext());
                            try (BooleanBlock result = iter.next()) {
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
            try (BooleanArrowBufBlock block = blockFromListVector(listVector)) {
                assertEquals(6, block.getTotalValueCount());
            }
        }
    }

    public void testMultiValuedToMask() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (BooleanArrowBufBlock block = blockFromListVector(listVector)) {
                try (var toMask = block.toMask()) {
                    assertTrue(toMask.hadMultivaluedFields());
                    BooleanVector mask = toMask.mask();
                    assertEquals(4, mask.getPositionCount());
                    assertFalse(mask.getBoolean(0));
                    assertFalse(mask.getBoolean(1));
                    assertFalse(mask.getBoolean(2));
                    assertFalse(mask.getBoolean(3));
                }
            }
        }
    }
}
