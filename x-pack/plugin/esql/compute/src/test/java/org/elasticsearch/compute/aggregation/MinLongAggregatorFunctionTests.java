/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class MinLongAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        long max = randomLongBetween(1, Long.MAX_VALUE / size);
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomLongBetween(-max, max)));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new MinLongAggregatorFunctionSupplier(inputChannels);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min of longs";
    }

    @Override
    public void assertSimpleOutput(List<Block> input, Block result) {
        long min = input.stream().flatMapToLong(b -> allLongs(b)).min().getAsLong();
        assertThat(((LongBlock) result).getLong(0), equalTo(min));
    }
}
