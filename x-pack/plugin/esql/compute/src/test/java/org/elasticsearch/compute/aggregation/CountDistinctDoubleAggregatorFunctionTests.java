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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.SequenceDoubleBlockSourceOperator;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.LongStream;

import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.hamcrest.Matchers.equalTo;

public class CountDistinctDoubleAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceDoubleBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToDouble(l -> ESTestCase.randomDouble()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new CountDistinctDoubleAggregatorFunctionSupplier(CountDistinctTestUtils.PRECISION);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count_distinct of doubles";
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, Block result) {
        long expected = CountDistinctTestUtils.expectedCount(
            state -> input.stream().flatMapToDouble(p -> allDoubles(p.getBlock(0))).forEach(state::collect)
        );
        assertThat(((LongBlock) result).getLong(0), equalTo(expected));
    }

    @Override
    protected void assertOutputFromEmpty(Block b) {
        assertThat(b.getPositionCount(), equalTo(1));
        assertThat(valuesAtPositions(b, 0, 1), equalTo(List.of(List.of(0L))));
    }
}
