/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for the unmasked {@link IntArrayVector} fast path of ungrouped {@code SUM(int)}: the vector's
 * {@link IntArrayVector#sum()} and the {@link SumIntAggregator#combineBulk} hook that the generated
 * aggregator calls once per vector instead of once per element.
 */
public class SumIntArrayVectorBulkTests extends ESTestCase {
    private final BlockFactory blockFactory = new BlockFactory(new NoopCircuitBreaker("test"), BigArrays.NON_RECYCLING_INSTANCE);

    public void testSumLengths() {
        for (int length = 0; length <= 40; length++) {
            int[] values = new int[length];
            for (int i = 0; i < length; i++) {
                values[i] = randomInt();
            }
            assertSum(values, length);
        }
    }

    public void testSumLarge() {
        int length = between(1000, 20_000);
        int[] values = new int[length];
        for (int i = 0; i < length; i++) {
            values[i] = randomInt();
        }
        assertSum(values, length);
    }

    public void testSumExtremes() {
        int length = between(1, 5000);
        int[] max = new int[length];
        int[] min = new int[length];
        int[] mixed = new int[length];
        Arrays.fill(max, Integer.MAX_VALUE);
        Arrays.fill(min, Integer.MIN_VALUE);
        for (int i = 0; i < length; i++) {
            mixed[i] = i % 2 == 0 ? Integer.MAX_VALUE : Integer.MIN_VALUE;
        }
        assertSum(max, length);
        assertSum(min, length);
        assertSum(mixed, length);
    }

    /**
     * The backing array can be longer than the position count. Everything past the count must be ignored.
     */
    public void testSumIgnoresValuesPastPositionCount() {
        int count = between(0, 100);
        int[] values = new int[count + between(1, 20)];
        for (int i = 0; i < values.length; i++) {
            values[i] = i < count ? randomIntBetween(-1000, 1000) : Integer.MAX_VALUE;
        }
        assertSum(values, count);
    }

    public void testCombineBulkAddsToCurrent() {
        int[] values = new int[between(0, 100)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomInt();
        }
        long current = randomLongBetween(-1_000_000_000_000L, 1_000_000_000_000L);
        try (IntVector vector = blockFactory.newIntArrayVector(values, values.length)) {
            assertThat(
                SumIntAggregator.combineBulk(current, (IntArrayVector) vector),
                equalTo(current + referenceSum(values, values.length))
            );
        }
    }

    /**
     * The one overflow check per vector must still fire when the final result does not fit in a long.
     */
    public void testCombineBulkOverflow() {
        try (IntVector vector = blockFactory.newIntArrayVector(new int[] { 1 }, 1)) {
            expectThrows(ArithmeticException.class, () -> SumIntAggregator.combineBulk(Long.MAX_VALUE, (IntArrayVector) vector));
        }
        try (IntVector vector = blockFactory.newIntArrayVector(new int[] { -1 }, 1)) {
            expectThrows(ArithmeticException.class, () -> SumIntAggregator.combineBulk(Long.MIN_VALUE, (IntArrayVector) vector));
        }
    }

    public void testAggregatorOverArrayVector() {
        int[] values = new int[between(1, 5000)];
        for (int i = 0; i < values.length; i++) {
            values[i] = randomInt();
        }
        assertThat(evaluate(null, values), equalTo(referenceSum(values, values.length)));
    }

    /**
     * Per element accumulation threw on a transient overflow even when the final sum was representable.
     * Summing a vector first and checking once only fails when the final result is out of range.
     */
    public void testAggregatorTransientOverflowIsAllowed() {
        assertThat(evaluate(Long.MAX_VALUE - 5, new int[] { 10, -10 }), equalTo(Long.MAX_VALUE - 5));
    }

    public void testAggregatorFinalOverflowThrows() {
        expectThrows(ArithmeticException.class, () -> evaluate(Long.MAX_VALUE - 5, new int[] { 10 }));
    }

    public void testAggregatorEmptyVector() {
        assertThat(evaluate(7L, new int[0]), equalTo(7L));
    }

    private void assertSum(int[] values, int count) {
        try (IntVector vector = blockFactory.newIntArrayVector(values, count)) {
            assertThat(vector, instanceOf(IntArrayVector.class));
            assertThat(((IntArrayVector) vector).sum(), equalTo(referenceSum(values, count)));
        }
    }

    private static long referenceSum(int[] values, int count) {
        long sum = 0;
        for (int i = 0; i < count; i++) {
            sum += values[i];
        }
        return sum;
    }

    /**
     * Runs the generated aggregator over a single unmasked {@link IntArrayVector} page, optionally starting
     * from {@code initial} (fed through the intermediate state, hence the two channels: sum and seen), and returns the final sum.
     */
    private long evaluate(Long initial, int[] values) {
        DriverContext ctx = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
        try (
            IntBlock block = blockFactory.newIntArrayVector(values, values.length).asBlock();
            BooleanVector mask = blockFactory.newConstantBooleanVector(true, values.length);
            SumIntAggregatorFunction agg = new SumIntAggregatorFunctionSupplier().aggregator(ctx, List.of(0, 1))
        ) {
            assertThat(block.asVector(), instanceOf(IntArrayVector.class));
            if (initial != null) {
                try (
                    Page intermediate = new Page(
                        blockFactory.newConstantLongBlockWith(initial, 1),
                        blockFactory.newConstantBooleanBlockWith(true, 1)
                    )
                ) {
                    agg.addIntermediateInput(intermediate);
                }
            }
            agg.addRawInput(new Page(block.asVector().asBlock()), mask);
            Block[] out = new Block[1];
            try {
                agg.evaluateFinal(out, 0, ctx);
                return ((LongBlock) out[0]).getLong(0);
            } finally {
                Releasables.closeExpectNoException(out);
            }
        }
    }
}
