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
import org.elasticsearch.compute.operator.SequenceFloatBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;

public class PercentileFloatAggregatorFunctionTests extends AggregatorFunctionTestCase {

    private double percentile;

    @Before
    public void initParameters() {
        percentile = randomFrom(0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100);
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new PercentileFloatAggregatorFunctionSupplier(inputChannels, percentile);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "percentile of floats";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceFloatBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToObj(l -> ESTestCase.randomFloat()));
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        try (TDigestState td = TDigestState.create(newLimitedBreaker(ByteSizeValue.ofMb(100)), QuantileStates.DEFAULT_COMPRESSION)) {
            input.stream().flatMap(AggregatorFunctionTestCase::allFloats).forEach(td::add);
            double expected = td.quantile(percentile / 100);
            double value = ((DoubleBlock) result).getDouble(0);
            assertThat(value, closeTo(expected, expected * 0.1));
        }
    }
}
