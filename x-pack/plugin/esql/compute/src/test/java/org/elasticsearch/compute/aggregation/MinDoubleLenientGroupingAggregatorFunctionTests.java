/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

/**
 * Grouping tests for the PromQL-only lenient {@code min} aggregator, {@link MinDoubleLenientAggregator}.
 * <p>
 *     Applies IEEE-754/Prometheus non-finite semantics per group: {@code NaN} values are skipped whenever a
 *     non-{@code NaN} value is present (so a group is {@code NaN} only when all of its values are {@code NaN}),
 *     while {@code ±Infinity} participate as ordinary ordered values. A group with no values at all remains
 *     {@code null}. The random {@link #simpleInput} mixes in {@code NaN} and {@code ±Infinity} so the full
 *     multi-mode grouping matrix asserts the lenient reduction, and the dedicated single-group tests below pin
 *     the all-{@code NaN} / all-{@code Infinity} / mixed corner cases.
 * </p>
 */
public class MinDoubleLenientGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new LongDoubleTupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, end).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomLenientDouble()))
        );
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MinDoubleLenientAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min_double of lenients";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        double[] values = input.stream().flatMapToDouble(p -> allDoubles(p, group)).toArray();
        if (values.length == 0) {
            assertTrue(result.isNull(position));
            return;
        }
        assertFalse(result.isNull(position));
        assertEquals(lenientMin(values), ((DoubleBlock) result).getDouble(position), 0.0);
    }

    public void testAllNaNProducesNaN() {
        assertSingleGroupMin(List.of(Double.NaN, Double.NaN, Double.NaN), Double.NaN);
    }

    public void testNaNSkippedWhenFinitePresent() {
        assertSingleGroupMin(List.of(Double.NaN, 3.0, Double.NaN, 1.0), 1.0);
    }

    public void testAllPositiveInfinity() {
        assertSingleGroupMin(List.of(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
    }

    public void testNegativeInfinityDominates() {
        assertSingleGroupMin(List.of(2.0, Double.NEGATIVE_INFINITY, Double.NaN, -100.0), Double.NEGATIVE_INFINITY);
    }

    public void testMixedInfinities() {
        assertSingleGroupMin(List.of(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN), Double.NEGATIVE_INFINITY);
    }

    /**
     * Feeds all values into a single group ({@code 0}) and asserts the aggregated value for that group.
     */
    private void assertSingleGroupMin(List<Double> values, double expected) {
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(new LongDoubleTupleBlockSourceOperator(runner.blockFactory(), values.stream().map(v -> Tuple.tuple(0L, v))));
        List<Page> results = runner.run(simple());
        boolean found = false;
        for (Page page : results) {
            LongBlock groups = page.getBlock(0);
            DoubleBlock result = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                if (groups.isNull(p) == false && groups.getLong(p) == 0L) {
                    found = true;
                    assertFalse(result.isNull(p));
                    assertEquals(expected, result.getDouble(p), 0.0);
                }
            }
        }
        assertTrue(found);
    }

    private double randomLenientDouble() {
        if (randomIntBetween(0, 7) == 0) {
            return randomFrom(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        }
        return randomDouble();
    }

    /**
     * Reference reduction mirroring {@link MinDoubleLenientAggregator#combine}: fold the values, seeded with
     * {@code NaN}, keeping the running value unless the incoming one is strictly smaller (which skips {@code NaN}
     * once a real value has been adopted). The result is order-independent, matching how the aggregator merges
     * across pages and partial states.
     */
    private static double lenientMin(double[] values) {
        double acc = Double.NaN;
        for (double v : values) {
            acc = Double.isNaN(acc) || acc > v ? v : acc;
        }
        return acc;
    }
}
