/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.operator.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.junit.Before;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;

public class PercentileLongAggregatorFunctionTests extends AggregatorFunctionTestCase {

    private double percentile;

    @Before
    public void initParameters() {
        percentile = randomFrom(0, 1, 5, 10, 25, 50, 75, 90, 95, 99, 100);
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, List<Integer> inputChannels) {
        return new PercentileLongAggregatorFunctionSupplier(bigArrays, inputChannels, percentile);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "percentile of longs";
    }

    @Override
    protected SourceOperator simpleInput(int size) {
        long max = randomLongBetween(1, 1_000_000);
        return new SequenceLongBlockSourceOperator(LongStream.range(0, size).map(l -> randomLongBetween(0, max)));
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        TDigestState td = new TDigestState(QuantileStates.DEFAULT_COMPRESSION);
        input.stream().flatMapToLong(p -> allLongs(p)).forEach(td::add);
        double expected = td.quantile(percentile / 100);
        double value = ((DoubleBlock) result).getDouble(0);
        assertThat(value, closeTo(expected, expected * 0.1));
    }
}
