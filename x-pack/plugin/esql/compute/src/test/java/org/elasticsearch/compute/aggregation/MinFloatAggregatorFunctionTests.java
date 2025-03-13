/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.operator.SequenceFloatBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class MinFloatAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceFloatBlockSourceOperator(blockFactory, LongStream.range(0, size).mapToObj(l -> ESTestCase.randomFloat()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new MinFloatAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min of floats";
    }

    @Override
    protected void assertSimpleOutput(List<Block> input, Block result) {
        Float min = input.stream().flatMap(b -> allFloats(b)).min(floatComparator()).get();
        assertThat(((FloatBlock) result).getFloat(0), equalTo(min));
    }
}
