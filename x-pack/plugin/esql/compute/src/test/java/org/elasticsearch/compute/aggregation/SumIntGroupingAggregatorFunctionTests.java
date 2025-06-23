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
import org.elasticsearch.compute.operator.LongIntBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class SumIntGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected AggregatorFunctionSupplier aggregatorFunction() {
        return new SumIntAggregatorFunctionSupplier();
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of ints";
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        int max = between(1, (int) Math.min(Integer.MAX_VALUE, Long.MAX_VALUE / size));
        return new LongIntBlockSourceOperator(
            blockFactory,
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), between(-max, max)))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, Long group) {
        long sum = input.stream().flatMapToInt(p -> allInts(p, group)).asLongStream().sum();
        assertThat(((LongBlock) result).getLong(position), equalTo(sum));
    }
}
