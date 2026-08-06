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
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.compute.test.operator.blocksource.SequenceDoubleBlockSourceOperator;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PercentileDoubleAggregatorFunctionTests extends AggregatorFunctionTestCase {

    private double percentile;

    @Before
    public void initParameters() {
        percentile = randomFrom(0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100);
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new PercentileDoubleAggregatorFunctionSupplier(TestWarningsSource.INSTANCE, percentile, QuantileStates.DEFAULT_COMPRESSION);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "percentile of doubles";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceDoubleBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToDouble(l -> ESTestCase.randomDouble()));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        try (TDigestState td = TDigestState.create(newLimitedBreaker(ByteSizeValue.ofMb(100)), QuantileStates.DEFAULT_COMPRESSION)) {
            input.stream().flatMapToDouble(p -> allDoubles(p.getBlock(0))).forEach(td::add);
            double expected = td.quantile(percentile / 100);
            double value = ((DoubleBlock) result).getDouble(0);
            assertThat(value, closeTo(expected, expected * 0.1));
        }
    }

    public void testNonFiniteInputReturnsNull() {
        for (double nonFinite : new double[] { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY }) {
            var runner = new TestDriverRunner().builder(driverContext());
            runner.input(new SequenceDoubleBlockSourceOperator(runner.blockFactory(), DoubleStream.of(nonFinite)));

            List<Page> results = runner.run(simple());

            assertThat(results, hasSize(1));
            assertThat(results.get(0).getBlock(0).isNull(0), equalTo(true));
        }
    }

    public void testNonFiniteInputReturnsNullAfterPartialAggregation() {
        var runner = new TestDriverRunner().builder(driverContext());
        runner.input(new SequenceDoubleBlockSourceOperator(runner.blockFactory(), DoubleStream.of(1.0, Double.POSITIVE_INFINITY)));

        List<Page> results = runner.run(
            simpleWithMode(AggregatorMode.INITIAL),
            simpleWithMode(AggregatorMode.INTERMEDIATE),
            simpleWithMode(AggregatorMode.FINAL)
        );

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getBlock(0).isNull(0), equalTo(true));
    }

}
