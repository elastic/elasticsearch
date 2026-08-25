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
import org.elasticsearch.compute.test.operator.blocksource.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;

/**
 * Grouping counterpart of {@link PercentileDoubleLenientAggregatorFunctionTests}. Beyond the lenient ranking, this
 * pins that a group whose observations are all non-finite still holds a value: rendering it as {@code null} would drop
 * the series from the result.
 */
public class PercentileDoubleLenientGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

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
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        return new LongDoubleTupleBlockSourceOperator(
            blockFactory,
            LongStream.range(0, end).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomLenientDouble()))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        double[] values = input.stream().flatMapToDouble(p -> allDoubles(p, group)).toArray();
        if (values.length == 0) {
            assertTrue(result.isNull(position));
            return;
        }
        long nanCount = Arrays.stream(values).filter(Double::isNaN).count();
        long negInfCount = Arrays.stream(values).filter(v -> v == Double.NEGATIVE_INFINITY).count();
        long posInfCount = Arrays.stream(values).filter(v -> v == Double.POSITIVE_INFINITY).count();

        try (TDigestState td = TDigestState.create(newLimitedBreaker(ByteSizeValue.ofMb(100)), QuantileStates.DEFAULT_COMPRESSION)) {
            Arrays.stream(values).filter(Double::isFinite).forEach(td::add);
            double expected = LenientQuantileStates.quantile(percentile / 100, td, nanCount, negInfCount, posInfCount);
            assertFalse("a group with observations must not be null", result.isNull(position));
            double value = ((DoubleBlock) result).getDouble(position);
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

    private double randomLenientDouble() {
        if (randomIntBetween(0, 7) == 0) {
            return randomFrom(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        }
        return randomDouble();
    }
}
