/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.SequenceLongBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class CountAggregatorFunctionTests extends AggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(int size) {
        return new SequenceLongBlockSourceOperator(LongStream.range(0, size).map(l -> randomLong()));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, int inputChannel) {
        return CountAggregatorFunction.supplier(bigArrays, inputChannel);
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
}
