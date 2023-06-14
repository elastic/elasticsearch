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
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.OptionalLong;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class MaxLongGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(BigArrays bigArrays, int inputChannel) {
        return new MaxLongAggregatorFunctionSupplier(bigArrays, inputChannel);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "max of longs";
    }

    @Override
    protected SourceOperator simpleInput(int size) {
        return new TupleBlockSourceOperator(LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomLong())));
    }

    @Override
    public void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        OptionalLong max = input.stream().flatMapToLong(p -> allLongs(p, group)).max();
        if (max.isEmpty()) {
            assertThat(result.isNull(position), equalTo(true));
            return;
        }
        assertThat(result.isNull(position), equalTo(false));
        assertThat(((LongBlock) result).getLong(position), equalTo(max.getAsLong()));
    }
}
