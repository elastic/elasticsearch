/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.SequenceIntBlockSourceOperator;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class MinIntAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceIntBlockSourceOperator(blockFactory, IntStream.range(0, size).map(l -> randomInt()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MinIntAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min of ints";
    }

    @Override
    public void assertSimpleOutput(List<Page> input, Block result) {
        int max = input.stream().flatMapToInt(p -> allInts(p.getBlock(0))).min().getAsInt();
        assertThat(((IntBlock) result).getInt(0), equalTo(max));
    }
}
