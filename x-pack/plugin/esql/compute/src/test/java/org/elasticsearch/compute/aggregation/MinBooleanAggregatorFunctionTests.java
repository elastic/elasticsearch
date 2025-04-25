/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.operator.SequenceBooleanBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class MinBooleanAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceBooleanBlockSourceOperator(blockFactory, IntStream.range(0, size).mapToObj(l -> randomBoolean()).toList());
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MinBooleanAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min of booleans";
    }

    @Override
    public void assertSimpleOutput(List<Block> input, Block result) {
        Boolean min = input.stream().flatMap(b -> allBooleans(b)).min(Comparator.naturalOrder()).get();
        assertThat(((BooleanBlock) result).getBoolean(0), equalTo(min));
    }
}
