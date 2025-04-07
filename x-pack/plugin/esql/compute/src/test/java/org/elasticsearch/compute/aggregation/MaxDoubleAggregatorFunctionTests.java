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
import org.elasticsearch.compute.operator.SequenceDoubleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class MaxDoubleAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceDoubleBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToDouble(l -> ESTestCase.randomDouble()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MaxDoubleAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "max of doubles";
    }

    @Override
    public void assertSimpleOutput(List<Block> input, Block result) {
        double max = input.stream().flatMapToDouble(b -> allDoubles(b)).max().getAsDouble();
        assertThat(((DoubleBlock) result).getDouble(0), equalTo(max));
    }
}
