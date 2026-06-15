/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);
        allocator = blockFactory.arrowAllocator();
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
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
        long expected = 0;
        for (int i = 0; i < positions; i++) {
            values[i] = between(-1000, 1000);
            mask[i] = randomBoolean();
            if (mask[i]) {
                expected += values[i];
            }
        }
        assertEquals(expected, sumIntArrowMasked(values, mask));
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

    private long sumIntArrowMasked(int[] values, boolean[] mask) {
        DriverContext ctx = driverContext();
        try (
            IntBlock block = arrowBlock(values);
            BooleanVector maskVector = maskVector(mask);
            SumIntAggregatorFunction agg = new SumIntAggregatorFunctionSupplier().aggregator(ctx, List.of(0))
        ) {
            agg.addRawInput(new Page(block), maskVector);
            return evaluateLong(agg, ctx);
        }
    }

    private long evaluateLong(SumIntAggregatorFunction agg, DriverContext ctx) {
        Block[] out = new Block[1];
        try {
            agg.evaluateFinal(out, 0, ctx);
            return ((LongBlock) out[0]).getLong(0);
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
