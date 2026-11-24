/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.arrow.IntArrowBufVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.List;

/**
 * Parity tests for the bulk Arrow aggregation path: foldable aggregators reduce off-heap
 * {@link IntArrowBufVector} buffers via {@code AbstractArrowBufVector#valuesSegment()} (a MemorySegment
 * loop) instead of per-element {@code getInt}. These feed a real Arrow vector through the generated
 * aggregator and assert the result matches a hand-computed reference, exercising both the segment
 * reduction and the restricted native-access path (which the standard aggregation test framework, which
 * only generates heap blocks, never reaches). The test runs under Arrow's debug allocator, so any
 * buffer leak fails it.
 */
public class BulkArrowAggregationTests extends ESTestCase {

    private BlockFactory blockFactory;
    private BufferAllocator allocator;

    @Before
    public void initBlockFactory() {
        blockFactory = new BlockFactory(NoopCircuitBreaker.INSTANCE, BigArrays.NON_RECYCLING_INSTANCE);
        allocator = blockFactory.arrowAllocator();
    }

    @After
    public void closeAllocator() throws Exception {
        allocator.close();
    }

    public void testSumIntOverArrowBuffer() {
        int positions = between(1, 5000);
        int[] values = new int[positions];
        long expected = 0;
        for (int i = 0; i < positions; i++) {
            values[i] = between(-1000, 1000);
            expected += values[i];
        }
        assertEquals(expected, sumIntArrow(values));
    }

    public void testSumIntOverArrowBufferMasked() {
        int positions = between(2, 5000);
        int[] values = new int[positions];
        boolean[] mask = new boolean[positions];
        boolean seen = false;
        long expected = 0;
        for (int i = 0; i < positions; i++) {
            values[i] = between(-1000, 1000);
            mask[i] = randomBoolean();
            if (mask[i]) {
                seen = true;
                expected += values[i];
            }
        }
        Long actual = sumIntArrowMasked(values, mask);
        if (seen) {
            assertNotNull(actual);
            assertEquals(expected, actual.longValue());
        } else {
            // SUM of no unmasked values is null, not 0
            assertNull(actual);
        }
    }

    /**
     * Feeds a deliberately misaligned int buffer through the real bulk Arrow segment path.
     * <p>
     * External Arrow producers are only <em>recommended</em> (not required) to pad each buffer to its
     * element width, and the Arrow IPC reader slices the shared record-batch body at whatever byte
     * offsets the producer wrote, so a vector's data buffer can start on an address that is not a
     * multiple of the element size. The JDK enforces {@link java.lang.foreign.ValueLayout} alignment in
     * software, so the segment reduction must read through an <em>unaligned</em> layout; with an aligned
     * layout the very first {@code getAtIndex} throws {@link IllegalArgumentException}. This is the
     * regression that {@code JAVA_INT_UNALIGNED} (vs {@code JAVA_INT}) guards against.
     */
    public void testSumIntOverMisalignedArrowBuffer() {
        int positions = between(1, 1000);
        long expected = 0;
        // One element of slack so the data can start one byte into the allocation.
        try (ArrowBuf base = allocator.buffer((long) (positions + 1) * Integer.BYTES)) {
            final long misalignByteOffset = 1; // not a multiple of 4: base + 1 is not int-aligned
            for (int i = 0; i < positions; i++) {
                int v = between(-1000, 1000);
                base.setInt(misalignByteOffset + (long) i * Integer.BYTES, v);
                expected += v;
            }
            ArrowBuf misaligned = base.slice(misalignByteOffset, (long) positions * Integer.BYTES);
            assertNotEquals("buffer must be misaligned for this test to be meaningful", 0, misaligned.memoryAddress() % Integer.BYTES);
            // The vector/block releases this reference once on close; balance it against the try-with-resources base.
            misaligned.getReferenceManager().retain();
            DriverContext ctx = driverContext();
            try (
                IntBlock block = new IntArrowBufVector(misaligned, positions, blockFactory).asBlock();
                BooleanVector mask = blockFactory.newConstantBooleanVector(true, positions);
                SumIntAggregatorFunction agg = new SumIntAggregatorFunctionSupplier().aggregator(ctx, List.of(0))
            ) {
                assertTrue("expected the bulk segment path", block.asVector() instanceof IntArrowBufVector);
                agg.addRawInput(new Page(block), mask);
                assertEquals(expected, evaluateLong(agg, ctx));
            }
        }
    }

    /** Extreme values must not wrap inside the unchecked per-buffer accumulation. */
    public void testSumIntOverArrowBufferExtremes() {
        int positions = between(1, 5000);
        int[] max = new int[positions];
        int[] min = new int[positions];
        Arrays.fill(max, Integer.MAX_VALUE);
        Arrays.fill(min, Integer.MIN_VALUE);
        assertEquals((long) positions * Integer.MAX_VALUE, sumIntArrow(max));
        assertEquals((long) positions * Integer.MIN_VALUE, sumIntArrow(min));
    }

    public void testSumIntOverArrowBufferFullRange() {
        int[] values = new int[between(1, 5000)];
        long expected = 0;
        for (int i = 0; i < values.length; i++) {
            values[i] = randomInt();
            expected += values[i];
        }
        assertEquals(expected, sumIntArrow(values));
    }

    /**
     * Only the first {@code count} ints of the segment are summed, even if the buffer holds more. {@code count} is always
     * strictly below the buffer length and every value past it is {@code Integer.MAX_VALUE}, so a loop that read the whole
     * segment would change the sum.
     */
    public void testCombineBulkSegmentIgnoresValuesPastCount() {
        int[] values = new int[between(2, 100)];
        int count = between(0, values.length - 1);
        long expected = 0;
        for (int i = 0; i < values.length; i++) {
            if (i < count) {
                values[i] = between(-1000, 1000);
                expected += values[i];
            } else {
                values[i] = Integer.MAX_VALUE;
            }
        }
        long current = randomLongBetween(-1_000_000_000_000L, 1_000_000_000_000L);
        try (IntBlock block = arrowBlock(values)) {
            MemorySegment segment = ((IntArrowBufVector) block.asVector()).valuesSegment();
            assertEquals(current + expected, SumIntAggregator.combineBulk(current, segment, count));
        }
    }

    /** The single overflow check per buffer must still fire when adding the buffer's sum to {@code current} leaves the long range. */
    public void testCombineBulkSegmentOverflow() {
        try (IntBlock block = arrowBlock(new int[] { 1 })) {
            MemorySegment segment = ((IntArrowBufVector) block.asVector()).valuesSegment();
            expectThrows(ArithmeticException.class, () -> SumIntAggregator.combineBulk(Long.MAX_VALUE, segment, 1));
        }
        try (IntBlock block = arrowBlock(new int[] { -1 })) {
            MemorySegment segment = ((IntArrowBufVector) block.asVector()).valuesSegment();
            expectThrows(ArithmeticException.class, () -> SumIntAggregator.combineBulk(Long.MIN_VALUE, segment, 1));
        }
    }

    /**
     * Overflow is checked once per buffer, so a transient overflow that cancels <em>within one</em> unmasked 32-bit Arrow
     * buffer does not throw, while a sum that leaves the {@code long} range after the buffer is added still does. The
     * guarantee is per buffer only; see {@link #testRunningSumCarriesAcrossPages} for state carried across pages.
     */
    public void testAggregatorOverflowOverArrowBuffer() {
        DriverContext ctx = driverContext();
        try (
            IntBlock block = arrowBlock(new int[] { 10, -10 });
            BooleanVector mask = blockFactory.newConstantBooleanVector(true, 2);
            SumIntAggregatorFunction agg = new SumIntAggregatorFunctionSupplier().aggregator(ctx, List.of(0, 1))
        ) {
            addIntermediate(agg, Long.MAX_VALUE - 5);
            agg.addRawInput(new Page(block), mask);
            assertEquals(Long.MAX_VALUE - 5, evaluateLong(agg, ctx));
        }
        try (
            IntBlock block = arrowBlock(new int[] { 10 });
            BooleanVector mask = blockFactory.newConstantBooleanVector(true, 1);
            SumIntAggregatorFunction agg = new SumIntAggregatorFunctionSupplier().aggregator(ctx, List.of(0, 1))
        ) {
            addIntermediate(agg, Long.MAX_VALUE - 5);
            Page page = new Page(block);
            expectThrows(ArithmeticException.class, () -> agg.addRawInput(page, mask));
        }
    }

    /**
     * The running sum is carried across raw-input pages and every page is still checked once. The first page lands the
     * sum exactly on {@code Long.MAX_VALUE}, which must not throw; the second page pushes it out of range and must.
     */
    public void testRunningSumCarriesAcrossPages() {
        DriverContext ctx = driverContext();
        try (
            IntBlock first = arrowBlock(new int[] { 5 });
            IntBlock second = arrowBlock(new int[] { 1 });
            BooleanVector mask = blockFactory.newConstantBooleanVector(true, 1);
            SumIntAggregatorFunction agg = new SumIntAggregatorFunctionSupplier().aggregator(ctx, List.of(0, 1))
        ) {
            addIntermediate(agg, Long.MAX_VALUE - 5);
            agg.addRawInput(new Page(first), mask);
            Page secondPage = new Page(second);
            expectThrows(ArithmeticException.class, () -> agg.addRawInput(secondPage, mask));
        }
    }

    private void addIntermediate(SumIntAggregatorFunction agg, long initial) {
        try (
            Page intermediate = new Page(
                blockFactory.newConstantLongBlockWith(initial, 1),
                blockFactory.newConstantBooleanBlockWith(true, 1)
            )
        ) {
            agg.addIntermediateInput(intermediate);
        }
    }

    private long sumIntArrow(int[] values) {
        DriverContext ctx = driverContext();
        try (
            IntBlock block = arrowBlock(values);
            BooleanVector mask = blockFactory.newConstantBooleanVector(true, values.length);
            SumIntAggregatorFunction agg = new SumIntAggregatorFunctionSupplier().aggregator(ctx, List.of(0))
        ) {
            agg.addRawInput(new Page(block), mask);
            return evaluateLong(agg, ctx);
        }
    }

    private Long sumIntArrowMasked(int[] values, boolean[] mask) {
        DriverContext ctx = driverContext();
        try (
            IntBlock block = arrowBlock(values);
            BooleanVector maskVector = maskVector(mask);
            SumIntAggregatorFunction agg = new SumIntAggregatorFunctionSupplier().aggregator(ctx, List.of(0))
        ) {
            agg.addRawInput(new Page(block), maskVector);
            return evaluateLongOrNull(agg, ctx);
        }
    }

    private long evaluateLong(SumIntAggregatorFunction agg, DriverContext ctx) {
        Long value = evaluateLongOrNull(agg, ctx);
        assertNotNull(value);
        return value;
    }

    private Long evaluateLongOrNull(SumIntAggregatorFunction agg, DriverContext ctx) {
        Block[] out = new Block[1];
        try {
            agg.evaluateFinal(out, 0, ctx);
            LongBlock block = (LongBlock) out[0];
            return block.isNull(0) ? null : block.getLong(0);
        } finally {
            Releasables.closeExpectNoException(out);
        }
    }

    /**
     * Builds a heap-free {@link IntArrowBufVector} and returns it as a block. Per {@code asBlock()}'s
     * contract the returned block takes sole ownership of the off-heap buffer's single reference, so
     * callers close the block (and must not also close a vector handle).
     */
    private IntBlock arrowBlock(int[] values) {
        try (org.apache.arrow.vector.IntVector arrowVec = new org.apache.arrow.vector.IntVector("test", allocator)) {
            arrowVec.allocateNew(values.length);
            for (int i = 0; i < values.length; i++) {
                arrowVec.set(i, values[i]);
            }
            arrowVec.setValueCount(values.length);
            return IntArrowBufVector.of(arrowVec, blockFactory).asBlock();
        }
    }

    private BooleanVector maskVector(boolean[] mask) {
        try (BooleanVector.Builder builder = blockFactory.newBooleanVectorBuilder(mask.length)) {
            for (boolean b : mask) {
                builder.appendBoolean(b);
            }
            return builder.build();
        }
    }

    private DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }

    // Sanity: confirm the block's vector is really an IntArrowBufVector so we exercise the bulk arm.
    public void testInputIsArrowVector() {
        try (IntBlock block = arrowBlock(new int[] { 1, 2, 3 })) {
            assertTrue("expected the bulk path to see an IntArrowBufVector", block.asVector() instanceof IntArrowBufVector);
        }
    }
}
