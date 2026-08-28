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
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

/**
 * Verifies that Arrow block/vector operations release their freshly allocated buffers when an
 * exception is thrown while populating them, i.e. after allocation but before the resulting
 * block or vector takes ownership. Failures are triggered either by corrupting the Arrow offset
 * buffers shared with the block (modeling malformed or hostile file data) or by violating call
 * invariants (out-of-range positions, an undersized mask). Without try/finally guards covering
 * the population phase, each scenario leaks the new buffers: the memory stays accounted in the
 * allocator (and thus charged to the circuit breaker) for the lifetime of the
 * {@link BlockFactory}.
 */
public class ArrowBufLeakTests extends ESTestCase {

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

    /**
     * Runs {@code op}, expecting it to throw {@link IndexOutOfBoundsException} mid-population,
     * and asserts that the allocator's outstanding memory is unchanged, i.e. that the buffers
     * allocated by the failed operation were released.
     */
    private void assertNoLeak(ThrowingRunnable op) {
        long before = allocator.getAllocatedMemory();
        expectThrows(IndexOutOfBoundsException.class, op);
        assertEquals("ArrowBuf leaked by failed operation", before, allocator.getAllocatedMemory());
    }

    // -- Long (exercises the AbstractArrowBufBlock / AbstractArrowBufVector implementations) --

    public void testLongBlockExpandCorruptOffsets() {
        try (ListVector listVector = createLongListVector()) {
            try (var block = LongArrowBufBlock.of(listVector, blockFactory)) {
                // Point the null position's offset far past the expanded position count: expand()
                // sizes the new validity buffer from the last offset (6) but indexes it with this one.
                listVector.getOffsetBuffer().setInt(1 * Integer.BYTES, 500_000);
                assertNoLeak(block::expand);
            }
        }
    }

    public void testLongBlockFilterOutOfRangePosition() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 10L);
            arrowVec.setNull(1);
            arrowVec.set(2, 30L);
            arrowVec.set(3, 40L);
            arrowVec.setValueCount(4);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                // Far out of range so the value copy reads the value buffer way past its capacity;
                // barely-out-of-range positions can land in the buffer's rounded-up padding.
                assertNoLeak(() -> block.filter(false, 1_000_000));
            }
        }
    }

    public void testLongBlockFilterMultiValuedCorruptOffsets() {
        try (ListVector listVector = createLongListVector()) {
            try (var block = LongArrowBufBlock.of(listVector, blockFactory)) {
                // Non-monotonic offsets: position 2 now claims 4 values and position 3 claims -1,
                // so the pre-scan undersizes the new buffers and the value copy overflows them.
                listVector.getOffsetBuffer().setInt(3 * Integer.BYTES, 7);
                assertNoLeak(() -> block.filter(false, 2, 3));
            }
        }
    }

    public void testLongBlockKeepMaskShortMask() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.set(2, 30L);
            arrowVec.set(3, 40L);
            arrowVec.setValueCount(4);

            try (var block = LongArrowBufBlock.of(arrowVec, blockFactory)) {
                // Undersized mask: keepMask reads one boolean per block position, and the mask's
                // backing array is shorter, throwing after the new validity buffer is allocated.
                try (var maskBuilder = blockFactory.newBooleanVectorFixedBuilder(2)) {
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(false);
                    try (BooleanVector mask = maskBuilder.build()) {
                        assertNoLeak(() -> block.keepMask(mask));
                    }
                }
            }
        }
    }

    public void testLongBlockLookupCorruptOffsets() {
        try (ListVector listVector = createLongListVector()) {
            try (var block = LongArrowBufBlock.of(listVector, blockFactory)) {
                // Same corruption as the multivalued filter case: the batch pre-scan undersizes
                // the new buffers and the value copy overflows them.
                listVector.getOffsetBuffer().setInt(3 * Integer.BYTES, 7);
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(2)) {
                    posBuilder.appendInt(2);
                    posBuilder.appendInt(3);
                    try (IntBlock positions = posBuilder.build()) {
                        assertNoLeak(() -> {
                            try (var iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                                iter.next();
                            }
                        });
                    }
                }
            }
        }
    }

    public void testLongVectorFilterOutOfRangePosition() {
        try (BigIntVector arrowVec = new BigIntVector("test", allocator)) {
            arrowVec.allocateNew(4);
            arrowVec.set(0, 10L);
            arrowVec.set(1, 20L);
            arrowVec.set(2, 30L);
            arrowVec.set(3, 40L);
            arrowVec.setValueCount(4);

            try (var vector = LongArrowBufVector.of(arrowVec, blockFactory)) {
                assertNoLeak(() -> vector.filter(false, 1_000_000));
            }
        }
    }

    // -- BytesRef (exercises the hand-written BytesRefArrowBufBlock / BytesRefArrowBufVector overrides) --

    public void testBytesRefBlockFilterCorruptValueOffsets() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, "aaaa".getBytes(StandardCharsets.UTF_8));
            arrowVec.setNull(1);
            arrowVec.set(2, "bb".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(3);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                // Keep the value's length (4) but point its bytes far past the data buffer, so
                // the pre-scan sizes the buffers fine and the byte copy reads out of bounds.
                ArrowBuf valueOffsets = arrowVec.getOffsetBuffer();
                valueOffsets.setInt(2 * Integer.BYTES, 1_000_000);
                valueOffsets.setInt(3 * Integer.BYTES, 1_000_004);
                assertNoLeak(() -> block.filter(false, 2));
            }
        }
    }

    public void testBytesRefBlockFilterMultiValuedCorruptOffsets() {
        try (ListVector listVector = createVarCharListVector()) {
            try (var block = BytesRefArrowBufBlock.of(listVector, blockFactory)) {
                listVector.getOffsetBuffer().setInt(3 * Integer.BYTES, 7);
                assertNoLeak(() -> block.filter(false, 2, 3));
            }
        }
    }

    public void testBytesRefBlockKeepMaskShortMask() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, "aaaa".getBytes(StandardCharsets.UTF_8));
            arrowVec.setNull(1);
            arrowVec.set(2, "bb".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(3);

            try (var block = BytesRefArrowBufBlock.of(arrowVec, blockFactory)) {
                try (var maskBuilder = blockFactory.newBooleanVectorFixedBuilder(2)) {
                    maskBuilder.appendBoolean(true);
                    maskBuilder.appendBoolean(false);
                    try (BooleanVector mask = maskBuilder.build()) {
                        assertNoLeak(() -> block.keepMask(mask));
                    }
                }
            }
        }
    }

    public void testBytesRefBlockExpandCorruptOffsets() {
        try (ListVector listVector = createVarCharListVector()) {
            try (var block = BytesRefArrowBufBlock.of(listVector, blockFactory)) {
                listVector.getOffsetBuffer().setInt(1 * Integer.BYTES, 500_000);
                assertNoLeak(block::expand);
            }
        }
    }

    public void testBytesRefBlockLookupCorruptOffsets() {
        try (ListVector listVector = createVarCharListVector()) {
            try (var block = BytesRefArrowBufBlock.of(listVector, blockFactory)) {
                listVector.getOffsetBuffer().setInt(3 * Integer.BYTES, 7);
                try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(2)) {
                    posBuilder.appendInt(2);
                    posBuilder.appendInt(3);
                    try (IntBlock positions = posBuilder.build()) {
                        assertNoLeak(() -> {
                            try (var iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                                iter.next();
                            }
                        });
                    }
                }
            }
        }
    }

    public void testBytesRefVectorFilterCorruptValueOffsets() {
        try (VarCharVector arrowVec = new VarCharVector("test", allocator)) {
            arrowVec.allocateNew(3);
            arrowVec.set(0, "aaaa".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(1, "bb".getBytes(StandardCharsets.UTF_8));
            arrowVec.set(2, "c".getBytes(StandardCharsets.UTF_8));
            arrowVec.setValueCount(3);

            try (var vector = BytesRefArrowBufVector.of(arrowVec, blockFactory)) {
                ArrowBuf valueOffsets = arrowVec.getOffsetBuffer();
                valueOffsets.setInt(1 * Integer.BYTES, 1_000_000);
                valueOffsets.setInt(2 * Integer.BYTES, 1_000_010);
                assertNoLeak(() -> vector.filter(false, 1));
            }
        }
    }

    // -- Boolean (exercises the hand-written BooleanArrowBufBlock / BooleanArrowBufVector overrides) --

    public void testBooleanBlockFilterOutOfRangePosition() {
        try (BitVector bitVector = new BitVector("test", allocator)) {
            bitVector.allocateNew(3);
            bitVector.set(0, 1);
            bitVector.setNull(1);
            bitVector.set(2, 0);
            bitVector.setValueCount(3);

            try (var block = BooleanArrowBufBlock.of(bitVector, blockFactory)) {
                // Bit-packed: position 1,000,000 reads value buffer byte 125,000.
                assertNoLeak(() -> block.filter(false, 1_000_000));
            }
        }
    }

    public void testBooleanBlockFilterMultiValuedCorruptOffsets() {
        try (var block = createCorruptMultiValuedBooleanBlock()) {
            assertNoLeak(() -> block.filter(false, 2, 3));
        }
    }

    public void testBooleanBlockLookupCorruptOffsets() {
        try (var block = createCorruptMultiValuedBooleanBlock()) {
            try (IntBlock.Builder posBuilder = blockFactory.newIntBlockBuilder(2)) {
                posBuilder.appendInt(2);
                posBuilder.appendInt(3);
                try (IntBlock positions = posBuilder.build()) {
                    assertNoLeak(() -> {
                        try (var iter = block.lookup(positions, ByteSizeValue.ofMb(1))) {
                            iter.next();
                        }
                    });
                }
            }
        }
    }

    public void testBooleanVectorFilterOutOfRangePosition() {
        try (BitVector bitVector = new BitVector("test", allocator)) {
            bitVector.allocateNew(3);
            bitVector.set(0, 1);
            bitVector.set(1, 0);
            bitVector.set(2, 1);
            bitVector.setValueCount(3);

            try (var vector = BooleanArrowBufVector.of(bitVector, blockFactory)) {
                assertNoLeak(() -> vector.filter(false, 1_000_000));
            }
        }
    }

    // -- Fixtures --

    /**
     * Creates a ListVector with a BigIntVector child and the following layout (same layout as
     * {@code LongArrowBufTests#createMultiValuedListVector}):
     * <pre>
     *   Position 0: [100, 200, 300]
     *   Position 1: null
     *   Position 2: [400]
     *   Position 3: [500, 600]
     * </pre>
     * Tests corrupt the list's offset buffer after building a block over it, modeling malformed
     * file-supplied data reaching the zero-copy block.
     */
    private ListVector createLongListVector() {
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

    /**
     * Creates a ListVector with a VarCharVector child and the following layout:
     * <pre>
     *   Position 0: ["a", "b", "c"]
     *   Position 1: null
     *   Position 2: ["d"]
     *   Position 3: ["e", "f"]
     * </pre>
     */
    private ListVector createVarCharListVector() {
        ListVector listVector = ListVector.empty("test", allocator);
        listVector.addOrGetVector(FieldType.nullable(Types.MinorType.VARCHAR.getType()));

        // Allocate before populating to avoid buffer reallocation
        listVector.allocateNew();
        VarCharVector childVector = (VarCharVector) listVector.getDataVector();
        childVector.allocateNew(6);

        childVector.set(0, "a".getBytes(StandardCharsets.UTF_8));
        childVector.set(1, "b".getBytes(StandardCharsets.UTF_8));
        childVector.set(2, "c".getBytes(StandardCharsets.UTF_8));
        childVector.set(3, "d".getBytes(StandardCharsets.UTF_8));
        childVector.set(4, "e".getBytes(StandardCharsets.UTF_8));
        childVector.set(5, "f".getBytes(StandardCharsets.UTF_8));
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

    /**
     * Hand-builds a multivalued boolean block (there is no boolean ListVector factory on
     * {@code BooleanArrowBufBlock}) over 6 bit-packed values with the layout of
     * {@link #createLongListVector} and an already corrupted last offset: position 3 claims
     * values [4, 600), so value reads run far past the 8-byte value buffer while the counts
     * stay under the lookup batch limit. The block takes ownership of the fresh buffers.
     */
    private BooleanArrowBufBlock createCorruptMultiValuedBooleanBlock() {
        ArrowBuf valueBuf = allocator.buffer(8);
        valueBuf.setZero(0, valueBuf.capacity());
        valueBuf.setByte(0, 0b0010_1101);

        ArrowBuf validityBuf = allocator.buffer(8);
        validityBuf.setZero(0, validityBuf.capacity());
        BitVectorHelper.setBit(validityBuf, 0);
        BitVectorHelper.setBit(validityBuf, 2);
        BitVectorHelper.setBit(validityBuf, 3);

        ArrowBuf offsetBuf = allocator.buffer(5 * Integer.BYTES);
        offsetBuf.setInt(0, 0);
        offsetBuf.setInt(4, 3);
        offsetBuf.setInt(8, 3);
        offsetBuf.setInt(12, 4);
        offsetBuf.setInt(16, 600);

        return new BooleanArrowBufBlock(valueBuf, validityBuf, offsetBuf, 4, 5, blockFactory);
    }
}
