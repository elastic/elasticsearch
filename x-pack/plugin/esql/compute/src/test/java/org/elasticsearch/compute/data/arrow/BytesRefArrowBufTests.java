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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

import static org.elasticsearch.compute.data.BasicBlockTests.assertDeepCopy;
import static org.elasticsearch.compute.data.BasicBlockTests.assertFilter;
import static org.elasticsearch.compute.data.BasicBlockTests.assertKeepMask;
import static org.elasticsearch.compute.data.BasicBlockTests.assertSlice;

/**
 * Tests for {@link BytesRefArrowBufVector} and {@link BytesRefArrowBufBlock}, backed by Arrow's
 * variable-length binary layout (VarCharVector for single-valued, ListVector with VarCharVector child
 * for multi-valued).
 */
public class BytesRefArrowBufTests extends ESTestCase {

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

    private static void assertBytesRef(String expected, BytesRefBlock block, int valueIndex, BytesRef scratch) {
        BytesRef actual = block.getBytesRef(valueIndex, scratch);
        assertEquals(expected, actual.utf8ToString());
    }

    private static void assertBytesRef(String expected, BytesRefVector vector, int position, BytesRef scratch) {
        BytesRef actual = vector.getBytesRef(position, scratch);
        assertEquals(expected, actual.utf8ToString());
    }

    private static void assertCursorBytes(String expected, PagedBytesCursor cursor) {
        byte[] actual = new byte[cursor.remaining()];
        for (int i = 0; i < actual.length; i++) {
            actual[i] = cursor.readByte();
        }
        assertEquals(expected, new String(actual, StandardCharsets.UTF_8));
    }

    // -- Vector tests (from VarCharVector without nulls) --

    public void testVectorBasics() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "hello".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "world".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(2, "".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(3);

            try (var vector = BytesRefArrowBufVector.of(arrowVec, blockFactory)) {
                assertEquals(3, vector.getPositionCount());
                assertEquals(ElementType.BYTES_REF, vector.elementType());
                assertFalse(vector.isConstant());
                BytesRef scratch = new BytesRef();
                assertBytesRef("hello", vector, 0, scratch);
                assertBytesRef("world", vector, 1, scratch);
                assertBytesRef("", vector, 2, scratch);

                // assertKeepMask(vector); TODO doesn't return "this"
                assertFilter(vector);
                assertSlice(vector);
            }
        }
    }

    public void testVectorAsBlock() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "abc".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "def".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(2);

            // Don't release vector, asBlock() takes ownership
            var vector = BytesRefArrowBufVector.of(arrowVec, blockFactory);
            try (BytesRefBlock block = vector.asBlock()) {
                assertEquals(2, block.getPositionCount());
                BytesRef scratch = new BytesRef();
                assertBytesRef("abc", block, 0, scratch);
                assertBytesRef("def", block, 1, scratch);
                assertFalse(block.mayHaveNulls());
                assertFalse(block.mayHaveMultivaluedFields());

                assertKeepMask(block);
                assertFilter(block);
                assertSlice(block);
                assertDeepCopy(block);
            }
        }
    }

    public void testVectorGetCursor() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "hello".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "world".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(2, "".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(3);

            try (var vector = BytesRefArrowBufVector.of(arrowVec, blockFactory)) {
                PagedBytesCursor scratch = new PagedBytesCursor();
                assertCursorBytes("hello", vector.get(0, scratch));
                assertCursorBytes("world", vector.get(1, scratch));
                assertCursorBytes("", vector.get(2, scratch));
            }
        }
    }

    public void testBlockGetCursor() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "foo".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "bar".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(2);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                PagedBytesCursor scratch = new PagedBytesCursor();
                assertCursorBytes("foo", block.get(0, scratch));
                assertCursorBytes("bar", block.get(1, scratch));
            }
        }
    }

    public void testVectorFilter() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "aaa".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "bbb".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(2, "ccc".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(3, "ddd".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(4);

            try (var vector = BytesRefArrowBufVector.of(arrowVec, blockFactory)) {
                try (BytesRefVector filtered = vector.filter(false, 0, 2, 3)) {
                    assertEquals(3, filtered.getPositionCount());
                    BytesRef scratch = new BytesRef();
                    assertBytesRef("aaa", filtered, 0, scratch);
                    assertBytesRef("ccc", filtered, 1, scratch);
                    assertBytesRef("ddd", filtered, 2, scratch);
                }
            }
        }
    }

    // -- Single-valued block tests (from VarCharVector) --

    public void testBlockWithNulls() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "foo".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "bar".getBytes(StandardCharsets.UTF_8));
            arrowVec.setNull(2);
            arrowVec.set(3, "baz".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(4);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                assertEquals(4, block.getPositionCount());
                assertEquals(ElementType.BYTES_REF, block.elementType());
                assertTrue(block.mayHaveNulls());
                assertFalse(block.mayHaveMultivaluedFields());

                BytesRef scratch = new BytesRef();
                assertFalse(block.isNull(0));
                assertBytesRef("foo", block, 0, scratch);
                assertFalse(block.isNull(1));
                assertBytesRef("bar", block, 1, scratch);
                assertTrue(block.isNull(2));
                assertEquals(0, block.getValueCount(2));
                assertFalse(block.isNull(3));
                assertBytesRef("baz", block, 3, scratch);
            }
        }
    }

    public void testBlockNoNulls() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "one".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "two".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(2);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                assertFalse(block.mayHaveNulls());
                assertFalse(block.areAllValuesNull());
                assertEquals(2, block.getTotalValueCount());

                // Don't release vec, it's a view on the block
                BytesRefVector vec = block.asVector();
                assertNotNull(vec);
                assertEquals(2, vec.getPositionCount());
                BytesRef scratch = new BytesRef();
                assertBytesRef("one", vec, 0, scratch);
                assertBytesRef("two", vec, 1, scratch);
            }
        }
    }

    public void testBlockAllNulls() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.setNull(0);
            arrowVec.setNull(1);
            arrowVec.setValueCount(2);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                assertTrue(block.mayHaveNulls());
                assertTrue(block.areAllValuesNull());
                assertEquals(0, block.getTotalValueCount());
                assertNull(block.asVector());
            }
        }
    }

    public void testBlockFilter() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "alpha".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "beta".getBytes(StandardCharsets.UTF_8));
            arrowVec.setNull(2);
            arrowVec.set(3, "gamma".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(4);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                try (BytesRefBlock filtered = block.filter(false, 0, 2, 3)) {
                    assertEquals(3, filtered.getPositionCount());
                    BytesRef scratch = new BytesRef();
                    assertFalse(filtered.isNull(0));
                    assertBytesRef("alpha", filtered, 0, scratch);
                    assertTrue(filtered.isNull(1));
                    assertFalse(filtered.isNull(2));
                    assertBytesRef("gamma", filtered, 2, scratch);
                }
            }
        }
    }

    public void testBlockKeepMaskMixed() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "a".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "b".getBytes(StandardCharsets.UTF_8));
            arrowVec.setNull(2);
            arrowVec.set(3, "d".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(4);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                try (var maskBuilder = blockFactory.newBooleanVectorFixedBuilder(4)) {
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(false);
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(true);
                    try (BooleanVector mask = maskBuilder.build()) {
                        try (BytesRefBlock kept = block.keepMask(mask)) {
                            assertEquals(4, kept.getPositionCount());
                            BytesRef scratch = new BytesRef();
                            assertFalse(kept.isNull(0));
                            assertBytesRef("a", kept, 0, scratch);
                            assertTrue(kept.isNull(1));
                            assertTrue(kept.isNull(2));
                            assertFalse(kept.isNull(3));
                            assertBytesRef("d", kept, 3, scratch);
                        }
                    }
                }
            }
        }
    }

    public void testBlockKeepMaskConstantTrue() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "x".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(1);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                try (BooleanVector mask = blockFactory.newConstantBooleanVector(true, 1)) {
                    try (BytesRefBlock kept = block.keepMask(mask)) {
                        assertSame(block, kept);
                    }
                }
            }
        }
    }

    public void testBlockKeepMaskConstantFalse() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "x".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(1);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                try (BooleanVector mask = blockFactory.newConstantBooleanVector(false, 1)) {
                    try (Block kept = block.keepMask(mask)) {
                        assertEquals(1, kept.getPositionCount());
                        assertTrue(kept.areAllValuesNull());
                    }
                }
            }
        }
    }

    public void testBlockExpandSingleValued() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "x".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(1);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                try (BytesRefBlock expanded = block.expand()) {
                    assertSame(block, expanded);
                }
            }
        }
    }

    public void testBlockLookupSingleValued() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew();
            arrowVec.set(0, "aaa".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "bbb".getBytes(StandardCharsets.UTF_8));
            arrowVec.setNull(2);
            arrowVec.set(3, "ddd".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(4);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(3)) {
                    posBuilder.appendInt(0);
                    posBuilder.appendInt(2);
                    posBuilder.appendInt(3);
                    try (IntBlock positions = posBuilder.build()) {
                        try (ReleasableIterator<? extends BytesRefBlock> iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                            assertTrue(iter.hasNext());
                            try (BytesRefBlock result = iter.next()) {
                                assertEquals(3, result.getPositionCount());
                                BytesRef scratch = new BytesRef();
                                assertFalse(result.isNull(0));
                                assertBytesRef("aaa", result, result.getFirstValueIndex(0), scratch);
                                assertTrue(result.isNull(1));
                                assertFalse(result.isNull(2));
                                assertBytesRef("ddd", result, result.getFirstValueIndex(2), scratch);
                            }
                            assertFalse(iter.hasNext());
                        }
                    }
                }
            }
        }
    }

    // -- Multi-valued block tests (from ListVector with VarCharVector child) --

    /**
     * Creates a ListVector with a VarCharVector child and the following layout:
     * <pre>
     *   Position 0: ["hello", "world"]
     *   Position 1: null
     *   Position 2: ["foo"]
     *   Position 3: ["bar", "baz"]
     * </pre>
     */
    private ListVector createMultiValuedListVector() {
        ListVector listVector = ListVector.empty("test", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.VARCHAR.getType()));

        listVector.allocateNew();
        VarCharVector childVector = (VarCharVector) listVector.getDataVector();
        childVector.allocateNew();

        childVector.set(0, "hello".getBytes(StandardCharsets.UTF_8));
        childVector.set(1, "world".getBytes(StandardCharsets.UTF_8));
        childVector.set(2, "foo".getBytes(StandardCharsets.UTF_8));
        childVector.set(3, "bar".getBytes(StandardCharsets.UTF_8));
        childVector.set(4, "baz".getBytes(StandardCharsets.UTF_8));
        childVector.setValueCount(5);

        ArrowBuf offsetBuf = listVector.getOffsetBuffer();
        offsetBuf.setInt(0, 0);
        offsetBuf.setInt(4, 2);
        offsetBuf.setInt(8, 2);
        offsetBuf.setInt(12, 3);
        offsetBuf.setInt(16, 5);

        ArrowBuf validityBuf = listVector.getValidityBuffer();
        validityBuf.setZero(0, validityBuf.capacity());
        BitVectorHelper.setBit(validityBuf, 0);
        BitVectorHelper.setBit(validityBuf, 2);
        BitVectorHelper.setBit(validityBuf, 3);

        listVector.setLastSet(3);
        listVector.setValueCount(4);
        return listVector;
    }

    private BytesRefArrowBufBlock blockFromListVector(ListVector listVector) {
        var result = BytesRefArrowBufBlock.of(listVector, blockFactory);
        return result;
    }

    public void testMultiValuedBlockBasics() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (BytesRefArrowBufBlock block = blockFromListVector(listVector)) {
                assertEquals(4, block.getPositionCount());
                assertTrue(block.mayHaveNulls());
                assertTrue(block.mayHaveMultivaluedFields());
                assertNull(block.asVector());

                BytesRef scratch = new BytesRef();

                assertFalse(block.isNull(0));
                assertEquals(2, block.getValueCount(0));
                assertBytesRef("hello", block, block.getFirstValueIndex(0), scratch);
                assertBytesRef("world", block, block.getFirstValueIndex(0) + 1, scratch);

                assertTrue(block.isNull(1));
                assertEquals(0, block.getValueCount(1));

                assertFalse(block.isNull(2));
                assertEquals(1, block.getValueCount(2));
                assertBytesRef("foo", block, block.getFirstValueIndex(2), scratch);

                assertFalse(block.isNull(3));
                assertEquals(2, block.getValueCount(3));
                assertBytesRef("bar", block, block.getFirstValueIndex(3), scratch);
                assertBytesRef("baz", block, block.getFirstValueIndex(3) + 1, scratch);

                assertEquals(5, block.getTotalValueCount());
            }
        }
    }

    public void testMultiValuedFilter() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (BytesRefArrowBufBlock block = blockFromListVector(listVector)) {
                try (BytesRefBlock filtered = block.filter(false, 0, 2)) {
                    assertEquals(2, filtered.getPositionCount());
                    BytesRef scratch = new BytesRef();

                    assertFalse(filtered.isNull(0));
                    assertEquals(2, filtered.getValueCount(0));
                    assertBytesRef("hello", filtered, filtered.getFirstValueIndex(0), scratch);
                    assertBytesRef("world", filtered, filtered.getFirstValueIndex(0) + 1, scratch);

                    assertFalse(filtered.isNull(1));
                    assertEquals(1, filtered.getValueCount(1));
                    assertBytesRef("foo", filtered, filtered.getFirstValueIndex(1), scratch);
                }
            }
        }
    }

    public void testMultiValuedFilterIncludingNull() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (BytesRefArrowBufBlock block = blockFromListVector(listVector)) {
                try (BytesRefBlock filtered = block.filter(false, 1, 3)) {
                    assertEquals(2, filtered.getPositionCount());
                    BytesRef scratch = new BytesRef();

                    assertTrue(filtered.isNull(0));

                    assertFalse(filtered.isNull(1));
                    assertEquals(2, filtered.getValueCount(1));
                    assertBytesRef("bar", filtered, filtered.getFirstValueIndex(1), scratch);
                    assertBytesRef("baz", filtered, filtered.getFirstValueIndex(1) + 1, scratch);
                }
            }
        }
    }

    public void testMultiValuedExpand() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (BytesRefArrowBufBlock block = blockFromListVector(listVector)) {
                try (BytesRefBlock expanded = block.expand()) {
                    assertNotSame(block, expanded);
                    assertEquals(5, expanded.getPositionCount());
                    assertFalse(expanded.mayHaveMultivaluedFields());

                    BytesRef scratch = new BytesRef();
                    assertBytesRef("hello", expanded, 0, scratch);
                    assertBytesRef("world", expanded, 1, scratch);
                    assertTrue(expanded.isNull(2));
                    assertBytesRef("bar", expanded, 3, scratch);
                    assertBytesRef("baz", expanded, 4, scratch);
                }
            }
        }
    }

    public void testMultiValuedKeepMask() {
        try (ListVector listVector = createMultiValuedListVector()) {
            try (BytesRefArrowBufBlock block = blockFromListVector(listVector)) {
                try (var maskBuilder = blockFactory.newBooleanVectorFixedBuilder(4)) {
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(false);
                    maskBuilder.appendBoolean(true);
                    try (BooleanVector mask = maskBuilder.build()) {
                        try (BytesRefBlock kept = block.keepMask(mask)) {
                            assertEquals(4, kept.getPositionCount());

                            assertFalse(kept.isNull(0));
                            assertEquals(2, kept.getValueCount(0));

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
            try (BytesRefArrowBufBlock block = blockFromListVector(listVector)) {
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(3)) {
                    posBuilder.appendInt(0);
                    posBuilder.appendInt(2);
                    posBuilder.appendInt(3);
                    try (IntBlock positions = posBuilder.build()) {
                        try (ReleasableIterator<? extends BytesRefBlock> iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                            assertTrue(iter.hasNext());
                            try (BytesRefBlock result = iter.next()) {
                                assertEquals(3, result.getPositionCount());
                                BytesRef scratch = new BytesRef();

                                assertFalse(result.isNull(0));
                                assertEquals(2, result.getValueCount(0));
                                assertBytesRef("hello", result, result.getFirstValueIndex(0), scratch);
                                assertBytesRef("world", result, result.getFirstValueIndex(0) + 1, scratch);

                                assertFalse(result.isNull(1));
                                assertEquals(1, result.getValueCount(1));
                                assertBytesRef("foo", result, result.getFirstValueIndex(1), scratch);

                                assertFalse(result.isNull(2));
                                assertEquals(2, result.getValueCount(2));
                                assertBytesRef("bar", result, result.getFirstValueIndex(2), scratch);
                                assertBytesRef("baz", result, result.getFirstValueIndex(2) + 1, scratch);
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
            try (BytesRefArrowBufBlock block = blockFromListVector(listVector)) {
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(2)) {
                    posBuilder.appendInt(1);
                    posBuilder.appendInt(0);
                    try (IntBlock positions = posBuilder.build()) {
                        try (ReleasableIterator<? extends BytesRefBlock> iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                            assertTrue(iter.hasNext());
                            try (BytesRefBlock result = iter.next()) {
                                assertEquals(2, result.getPositionCount());
                                assertTrue(result.isNull(0));
                                assertFalse(result.isNull(1));
                                assertEquals(2, result.getValueCount(1));
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
            try (BytesRefArrowBufBlock block = blockFromListVector(listVector)) {
                assertEquals(5, block.getTotalValueCount());
            }
        }
    }
}
