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
import org.elasticsearch.compute.operator.SequenceIntBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.junit.Before;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;

public class PercentileIntAggregatorFunctionTests extends AggregatorFunctionTestCase {

    private double percentile;

    @Before
    public void initParameters() {
        percentile = randomFrom(0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100);
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new PercentileIntAggregatorFunctionSupplier(inputChannels, percentile);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "percentile of ints";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        int max = between(1, (int) Math.min(Integer.MAX_VALUE, Long.MAX_VALUE / size));
        return new SequenceIntBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToInt(l -> between(0, max)));
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        try (TDigestState td = TDigestState.create(newLimitedBreaker(ByteSizeValue.ofMb(100)), QuantileStates.DEFAULT_COMPRESSION)) {
            input.stream().flatMapToInt(b -> allInts(b)).forEach(td::add);
            double expected = td.quantile(percentile / 100);
            double value = ((DoubleBlock) result).getDouble(0);
            assertThat(value, closeTo(expected, expected * 0.1));
        }
    }
}
