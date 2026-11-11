/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.elasticsearch.core.Releasables;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class SumLongAsDoubleAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        // The full range of longs, so the sums exceed what a long, and what a double, can hold exactly.
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomLong()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SumLongAsDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of longs as doubles";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        BigInteger exact = input.stream()
            .flatMapToLong(p -> allLongs(p.getBlock(0)))
            .mapToObj(BigInteger::valueOf)
            .reduce(BigInteger.ZERO, BigInteger::add);
        double expected = exact.doubleValue();
        // The partial results of the drivers are merged as doubles, so allow for the rounding of that merge.
        assertThat(((DoubleBlock) result).getDouble(0), closeTo(expected, Math.abs(expected) * 1e-12));
    }

    /**
     * The intermediate state is the contract with nodes that don't know this function, they must see exactly what
     * {@code SUM(TO_DOUBLE(x))} would send.
     */
    public void testIntermediateStateIsTheOneOfSumDouble() {
        AggregatorFunctionSupplier sumDouble = new SumDoubleAggregatorFunctionSupplier();
        assertThat(aggregatorFunction().nonGroupingIntermediateStateDesc(), equalTo(sumDouble.nonGroupingIntermediateStateDesc()));
        assertThat(aggregatorFunction().groupingIntermediateStateDesc(), equalTo(sumDouble.groupingIntermediateStateDesc()));
    }

    public void testExactSumOfArrayVectors() {
        // Lengths around the 4 value unrolling of the kernel, and a long one, with random and with extreme values.
        for (int length : new int[] { 0, 1, 2, 3, 4, 5, 7, 8, 127, 128, 129, 1000, between(1, 20_000) }) {
            long[] values = new long[length];
            for (int i = 0; i < length; i++) {
                values[i] = randomBoolean() ? randomLong() : randomFrom(Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0L, 1L);
            }
            assertExactSum(values, null);
        }
    }

    public void testExtremeValuesDoNotOverflowTheLanes() {
        long[] min = new long[100_000];
        Arrays.fill(min, Long.MIN_VALUE);
        assertExactSum(min, null);
        long[] max = new long[100_000];
        Arrays.fill(max, Long.MAX_VALUE);
        assertExactSum(max, null);
    }

    public void testCarriesAcrossTheLowWord() {
        assertExactSum(new long[] { Long.MAX_VALUE, Long.MAX_VALUE, 2 }, null);
        assertExactSum(new long[] { Long.MIN_VALUE, Long.MIN_VALUE, -2 }, null);
        assertExactSum(new long[] { Long.MAX_VALUE, 1, Long.MAX_VALUE, 1, Long.MAX_VALUE, 1, Long.MAX_VALUE, 1 }, null);
        assertExactSum(new long[] { 0xFFFF_FFFFL, 0xFFFF_FFFFL, 0xFFFF_FFFFL, 0xFFFF_FFFFL, 1, 1, 1, 1 }, null);
    }

    public void testMaskedArrayVector() {
        int length = between(1, 5000);
        long[] values = new long[length];
        boolean[] mask = new boolean[length];
        for (int i = 0; i < length; i++) {
            values[i] = randomLong();
            mask[i] = randomBoolean();
        }
        assertExactSum(values, mask);
    }

    public void testNullsAndMultiValues() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        BigInteger expected = BigInteger.ZERO;
        int positions = between(1, 500);
        try (LongBlock.Builder builder = blockFactory.newLongBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                switch (between(0, 2)) {
                    case 0 -> builder.appendNull();
                    case 1 -> {
                        long v = randomLong();
                        builder.appendLong(v);
                        expected = expected.add(BigInteger.valueOf(v));
                    }
                    case 2 -> {
                        builder.beginPositionEntry();
                        for (int i = between(2, 5); i > 0; i--) {
                            long v = randomLong();
                            builder.appendLong(v);
                            expected = expected.add(BigInteger.valueOf(v));
                        }
                        builder.endPositionEntry();
                    }
                    default -> throw new AssertionError();
                }
            }
            try (
                LongBlock block = builder.build();
                BooleanVector mask = blockFactory.newConstantBooleanVector(true, positions);
                SumLongAsDoubleAggregatorFunction function = new SumLongAsDoubleAggregatorFunction(List.of(0))
            ) {
                function.addRawInput(new Page(block), mask);
                assertThat(function.exactSum(), equalTo(expected));
            }
        }
        driverContext.finish();
        assertDriverContext(driverContext);
    }

    public void testNothingSeenIsNull() {
        DriverContext driverContext = driverContext();
        try (SumLongAsDoubleAggregatorFunction function = new SumLongAsDoubleAggregatorFunction(List.of(0))) {
            Block[] result = new Block[1];
            function.evaluateFinal(result, 0, driverContext);
            try (Block r = result[0]) {
                assertTrue(r.isNull(0));
            }

            Block[] intermediate = new Block[3];
            function.evaluateIntermediate(intermediate, 0, driverContext);
            try {
                assertThat(((DoubleBlock) intermediate[0]).getDouble(0), equalTo(0.0));
                assertThat(((DoubleBlock) intermediate[1]).getDouble(0), equalTo(0.0));
                assertThat(((BooleanBlock) intermediate[2]).getBoolean(0), equalTo(false));
            } finally {
                Releasables.close(intermediate);
            }
        }
        driverContext.finish();
        assertDriverContext(driverContext);
    }

    /**
     * The part of the exact sum that doesn't fit into the {@code double} travels in {@code delta}, so that
     * {@code value + delta} is the exact sum.
     */
    public void testIntermediateCarriesTheRoundingError() {
        DriverContext driverContext = driverContext();
        // 2^62 + 1 isn't representable as a double, 2^62 is.
        long[] values = new long[] { 1L << 62, 1 };
        try (SumLongAsDoubleAggregatorFunction function = new SumLongAsDoubleAggregatorFunction(List.of(0))) {
            addArray(driverContext.blockFactory(), function, values, null);
            Block[] intermediate = new Block[3];
            function.evaluateIntermediate(intermediate, 0, driverContext);
            try {
                assertThat(((DoubleBlock) intermediate[0]).getDouble(0), equalTo((double) (1L << 62)));
                assertThat(((DoubleBlock) intermediate[1]).getDouble(0), equalTo(1.0));
                assertThat(((BooleanBlock) intermediate[2]).getBoolean(0), equalTo(true));
            } finally {
                Releasables.close(intermediate);
            }
        }
        driverContext.finish();
        assertDriverContext(driverContext);
    }

    /** Partial results from two functions merge into the same total as one function adding all the input. */
    public void testMergeOfPartials() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        long[] a = randomLongs(between(1, 3000));
        long[] b = randomLongs(between(1, 3000));
        BigInteger expected = sum(a).add(sum(b));
        try (
            SumLongAsDoubleAggregatorFunction partialA = new SumLongAsDoubleAggregatorFunction(List.of(0));
            SumLongAsDoubleAggregatorFunction partialB = new SumLongAsDoubleAggregatorFunction(List.of(0));
            SumLongAsDoubleAggregatorFunction merged = new SumLongAsDoubleAggregatorFunction(List.of(0, 1, 2))
        ) {
            addArray(blockFactory, partialA, a, null);
            addArray(blockFactory, partialB, b, null);
            for (SumLongAsDoubleAggregatorFunction partial : List.of(partialA, partialB)) {
                Block[] intermediate = new Block[3];
                partial.evaluateIntermediate(intermediate, 0, driverContext);
                Page page = new Page(intermediate);
                try {
                    merged.addIntermediateInput(page);
                } finally {
                    page.releaseBlocks();
                }
            }
            Block[] result = new Block[1];
            merged.evaluateFinal(result, 0, driverContext);
            try (Block r = result[0]) {
                // The partial sums are merged as doubles, so the result can be off by a couple of ulps of the partial sums, which can be
                // far larger than the result when they nearly cancel out.
                double partials = Math.max(Math.abs(sum(a).doubleValue()), Math.abs(sum(b).doubleValue()));
                assertThat(((DoubleBlock) r).getDouble(0), closeTo(expected.doubleValue(), 2 * Math.ulp(partials)));
            }
        }
        driverContext.finish();
        assertDriverContext(driverContext);
    }

    public void testMergeIgnoresPartialsThatSawNothing() {
        DriverContext driverContext = driverContext();
        try (
            SumLongAsDoubleAggregatorFunction empty = new SumLongAsDoubleAggregatorFunction(List.of(0));
            SumLongAsDoubleAggregatorFunction merged = new SumLongAsDoubleAggregatorFunction(List.of(0, 1, 2))
        ) {
            Block[] intermediate = new Block[3];
            empty.evaluateIntermediate(intermediate, 0, driverContext);
            Page page = new Page(intermediate);
            try {
                merged.addIntermediateInput(page);
            } finally {
                page.releaseBlocks();
            }
            Block[] result = new Block[1];
            merged.evaluateFinal(result, 0, driverContext);
            try (Block r = result[0]) {
                assertTrue(r.isNull(0));
            }
        }
        driverContext.finish();
        assertDriverContext(driverContext);
    }

    public void testGroupingIsNotSupported() {
        expectThrows(UnsupportedOperationException.class, () -> aggregatorFunction().groupingAggregator(driverContext(), List.of(0)));
    }

    private void assertExactSum(long[] values, boolean[] mask) {
        DriverContext driverContext = driverContext();
        BigInteger expected = BigInteger.ZERO;
        for (int i = 0; i < values.length; i++) {
            if (mask == null || mask[i]) {
                expected = expected.add(BigInteger.valueOf(values[i]));
            }
        }
        try (SumLongAsDoubleAggregatorFunction function = new SumLongAsDoubleAggregatorFunction(List.of(0))) {
            addArray(driverContext.blockFactory(), function, values, mask);
            assertThat(function.exactSum(), equalTo(expected));

            Block[] result = new Block[1];
            function.evaluateFinal(result, 0, driverContext);
            try (Block r = result[0]) {
                boolean anySeen = mask == null ? values.length > 0 : anyTrue(mask);
                if (anySeen) {
                    assertThat(((DoubleBlock) r).getDouble(0), equalTo(expected.doubleValue()));
                } else {
                    assertTrue(r.isNull(0));
                }
            }
        }
        driverContext.finish();
        assertDriverContext(driverContext);
    }

    private static boolean anyTrue(boolean[] mask) {
        for (boolean b : mask) {
            if (b) {
                return true;
            }
        }
        return false;
    }

    private static void addArray(BlockFactory blockFactory, SumLongAsDoubleAggregatorFunction function, long[] values, boolean[] mask) {
        BooleanVector maskVector = null;
        try (LongBlock block = blockFactory.newLongArrayVector(values.clone(), values.length).asBlock()) {
            if (mask == null) {
                maskVector = blockFactory.newConstantBooleanVector(true, values.length);
            } else {
                maskVector = blockFactory.newBooleanArrayVector(mask.clone(), mask.length);
            }
            function.addRawInput(new Page(block), maskVector);
        } finally {
            Releasables.close(maskVector);
        }
    }

    private static long[] randomLongs(int length) {
        long[] values = new long[length];
        for (int i = 0; i < length; i++) {
            values[i] = randomLong();
        }
        return values;
    }

    private static BigInteger sum(long[] values) {
        BigInteger sum = BigInteger.ZERO;
        for (long v : values) {
            sum = sum.add(BigInteger.valueOf(v));
        }
        return sum;
    }
}
