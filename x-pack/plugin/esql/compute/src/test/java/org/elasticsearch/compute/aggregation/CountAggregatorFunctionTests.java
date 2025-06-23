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
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;

import java.util.List;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.hamcrest.Matchers.equalTo;

public class CountAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomLong()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return CountAggregatorFunction.supplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count";
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        long count = input.stream().flatMapToLong(b -> allLongs(b)).count();
        assertThat(((LongBlock) result).getLong(0), equalTo(count));
    }

    @Override
    protected void assertOutputFromEmpty(Block b) {
        assertThat(b.getPositionCount(), equalTo(1));
        assertThat(valuesAtPositions(b, 0, 1), equalTo(List.of(List.of(0L))));
    }
}
