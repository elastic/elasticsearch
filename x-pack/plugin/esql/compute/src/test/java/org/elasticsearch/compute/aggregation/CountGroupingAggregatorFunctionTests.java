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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class CountGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, int inputChannel) {
        return CountAggregatorFunction.supplier(bigArrays, inputChannel);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count";
    }

    @Override
    protected SourceOperator simpleInput(int size) {
        if (randomBoolean()) {
            return new TupleBlockSourceOperator(
                LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomLong()))
            );
        }
        return new LongDoubleTupleBlockSourceOperator(
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomDouble()))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        long count = input.stream().flatMapToInt(p -> allValueOffsets(p, group)).count();
        assertThat(((LongBlock) result).getLong(position), equalTo(count));
    }
}
