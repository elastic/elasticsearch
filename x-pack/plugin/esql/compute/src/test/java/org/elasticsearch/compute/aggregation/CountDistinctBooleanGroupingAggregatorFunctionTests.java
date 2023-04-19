/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.LongBooleanTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class CountDistinctBooleanGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {

    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.COUNT_DISTINCT_BOOLEANS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "count_distinct of booleans";
    }

    @Override
    protected SourceOperator simpleInput(int size) {
        return new LongBooleanTupleBlockSourceOperator(
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomBoolean()))
        );
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        final int groupIndex = 0;
        final int valueIndex = 1;

        long expected = input.stream().flatMap(b -> IntStream.range(0, b.getPositionCount()).filter(p -> {
            LongBlock groupBlock = b.getBlock(groupIndex);
            Block valuesBlock = b.getBlock(valueIndex);
            return false == groupBlock.isNull(p) && false == valuesBlock.isNull(p) && groupBlock.getLong(p) == group;
        }).mapToObj(p -> ((BooleanBlock) b.getBlock(valueIndex)).getBoolean(p))).distinct().count();

        long count = ((LongBlock) result).getLong(position);
        assertThat(count, equalTo(expected));
    }
}
