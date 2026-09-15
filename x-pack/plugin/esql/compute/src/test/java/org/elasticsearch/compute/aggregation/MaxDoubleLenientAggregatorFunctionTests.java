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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceDoubleBlockSourceOperator;

import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for the PromQL-only lenient {@code max} aggregator, {@link MaxDoubleLenientAggregator}.
 * <p>
 *     Unlike the strict {@code max} aggregator, this one applies IEEE-754/Prometheus non-finite semantics:
 *     {@code NaN} inputs are skipped whenever a non-{@code NaN} value is present (so the result is {@code NaN}
 *     only when every input is {@code NaN}), while {@code ±Infinity} participate as ordinary ordered values.
 *     The random {@link #simpleInput} deliberately mixes in {@code NaN} and {@code ±Infinity} so that the whole
 *     inherited {@code SINGLE / INITIAL / INTERMEDIATE / FINAL} matrix asserts the lenient reduction, and the
 *     dedicated tests below pin the specific all-{@code NaN} / all-{@code Infinity} / mixed corner cases.
 * </p>
 */
public class MaxDoubleLenientAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceDoubleBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToDouble(l -> randomLenientDouble()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MaxDoubleLenientAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "max_double of lenients";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        double max = lenientMax(input.stream().flatMapToDouble(p -> allDoubles(p.getBlock(0))));
        assertEquals(max, ((DoubleBlock) result).getDouble(0), 0.0);
    }

    public void testAllNaNProducesNaN() {
        assertLenientMax(List.of(Double.NaN, Double.NaN, Double.NaN), Double.NaN);
    }

    public void testNaNSkippedWhenFinitePresent() {
        assertLenientMax(List.of(Double.NaN, 3.0, Double.NaN, 1.0), 3.0);
    }

    public void testAllNegativeInfinity() {
        assertLenientMax(List.of(Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY), Double.NEGATIVE_INFINITY);
    }

    public void testNegativeInfinityAmongFinite() {
        assertLenientMax(List.of(Double.NEGATIVE_INFINITY, -5.0, -3.0), -3.0);
    }

    public void testPositiveInfinityDominates() {
        assertLenientMax(List.of(-2.0, Double.POSITIVE_INFINITY, Double.NaN, 100.0), Double.POSITIVE_INFINITY);
    }

    public void testMixedInfinities() {
        assertLenientMax(List.of(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN), Double.POSITIVE_INFINITY);
    }

    private void assertLenientMax(List<Double> values, double expected) {
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(new SequenceDoubleBlockSourceOperator(runner.blockFactory(), values));
        List<Page> results = runner.run(simple());
        assertThat(results, hasSize(1));
        Block result = results.get(0).getBlock(0);
        assertFalse(result.isNull(0));
        assertEquals(expected, ((DoubleBlock) result).getDouble(0), 0.0);
    }

    private double randomLenientDouble() {
        if (randomIntBetween(0, 7) == 0) {
            return randomFrom(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        }
        return randomDouble();
    }

    /**
     * Reference reduction mirroring {@link MaxDoubleLenientAggregator#combine}: fold the values, seeded with
     * {@code NaN}, keeping the running value unless the incoming one is strictly greater (which skips {@code NaN}
     * once a real value has been adopted). The result is order-independent, matching how the aggregator merges
     * across pages and partial states.
     */
    private static double lenientMax(DoubleStream values) {
        return values.reduce(Double.NaN, (acc, v) -> Double.isNaN(acc) || acc < v ? v : acc);
    }
}
