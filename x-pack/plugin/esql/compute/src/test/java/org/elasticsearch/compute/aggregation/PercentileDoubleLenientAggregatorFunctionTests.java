/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.operator.blocksource.SequenceDoubleBlockSourceOperator;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for the PromQL-only lenient percentile aggregator, {@link PercentileDoubleLenientAggregator}. The strict
 * aggregator's t-digest throws on a non-finite observation, aborting the query; this one tallies them and ranks them as
 * {@code NaN < -Inf < finite < +Inf}.
 * <p>
 *     The random {@link #simpleInput} mixes in {@code NaN} and {@code ±Infinity} so the inherited
 *     {@code SINGLE / INITIAL / INTERMEDIATE / FINAL} matrix exercises the tallies travelling through the intermediate
 *     state alongside the digest. {@link #assertSimpleOutput} shares the rank resolution with the implementation, since
 *     what it checks is that every page reached the aggregator and survived the partial-state round trip; the rank
 *     resolution itself is pinned independently by {@link LenientQuantileStatesTests}, and by the fixed corner cases
 *     below.
 * </p>
 */
public class PercentileDoubleLenientAggregatorFunctionTests extends AggregatorFunctionTestCase {

    private double percentile;

    @Before
    public void initParameters() {
        percentile = randomFrom(0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100);
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new PercentileDoubleLenientAggregatorFunctionSupplier(percentile, QuantileStates.DEFAULT_COMPRESSION);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "percentile_double of lenients";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceDoubleBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToDouble(l -> randomLenientDouble()));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        double[] values = input.stream().flatMapToDouble(p -> allDoubles(p.getBlock(0))).toArray();
        long nanCount = Arrays.stream(values).filter(Double::isNaN).count();
        long negInfCount = Arrays.stream(values).filter(v -> v == Double.NEGATIVE_INFINITY).count();
        long posInfCount = Arrays.stream(values).filter(v -> v == Double.POSITIVE_INFINITY).count();

        try (TDigestState td = TDigestState.create(newLimitedBreaker(ByteSizeValue.ofMb(100)), QuantileStates.DEFAULT_COMPRESSION)) {
            Arrays.stream(values).filter(Double::isFinite).forEach(td::add);
            double expected = LenientQuantileStates.quantile(percentile / 100, td, nanCount, negInfCount, posInfCount);
            double value = ((DoubleBlock) result).getDouble(0);
            if (Double.isNaN(expected)) {
                assertTrue("expected NaN but got [" + value + "]", Double.isNaN(value));
            } else if (Double.isInfinite(expected)) {
                assertEquals(expected, value, 0.0);
            } else {
                // The aggregator merges a digest per page while the reference builds one, so the finite estimate differs slightly.
                assertThat(value, closeTo(expected, Math.abs(expected) * 0.1 + 1e-9));
            }
        }
    }

    public void testAllPositiveInfinity() {
        percentile = 50;
        assertLenientPercentile(
            List.of(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY),
            Double.POSITIVE_INFINITY
        );
    }

    public void testAllNaN() {
        percentile = 50;
        assertLenientPercentile(List.of(Double.NaN, Double.NaN), Double.NaN);
    }

    public void testNaNRanksLowest() {
        percentile = 50;
        assertLenientPercentile(List.of(2.0, Double.NaN, 1.0), 1.0);
    }

    public void testInfinitiesBracketTheFiniteValues() {
        percentile = 0;
        assertLenientPercentile(List.of(Double.NEGATIVE_INFINITY, 1.0, Double.POSITIVE_INFINITY), Double.NEGATIVE_INFINITY);
        percentile = 100;
        assertLenientPercentile(List.of(Double.NEGATIVE_INFINITY, 1.0, Double.POSITIVE_INFINITY), Double.POSITIVE_INFINITY);
    }

    private void assertLenientPercentile(List<Double> values, double expected) {
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(new SequenceDoubleBlockSourceOperator(runner.blockFactory(), values));
        List<Page> results = runner.run(simple());
        assertThat(results, hasSize(1));
        Block result = results.get(0).getBlock(0);
        assertFalse(result.isNull(0));
        double value = ((DoubleBlock) result).getDouble(0);
        if (Double.isNaN(expected)) {
            assertTrue("expected NaN but got [" + value + "]", Double.isNaN(value));
        } else {
            assertEquals(expected, value, 0.0);
        }
    }

    private double randomLenientDouble() {
        if (randomIntBetween(0, 7) == 0) {
            return randomFrom(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        }
        return randomDouble();
    }
}
