/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.LongIntBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class AvgIntGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.AVG_INTS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "avg of ints";
    }

    @Override
    protected SourceOperator simpleInput(int size) {
        int max = between(1, (int) Math.min(Integer.MAX_VALUE, Long.MAX_VALUE / size));
        return new LongIntBlockSourceOperator(
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), between(-max, max)))
        );
    }

    @Override
    public void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        double sum = input.stream().flatMapToInt(p -> allInts(p, group)).asLongStream().sum();
        long count = input.stream().flatMapToInt(p -> allInts(p, group)).count();
        if (count == 0) {
            // If all values are null we'll have a count of 0. So we'll be null.
            assertThat(result.isNull(position), equalTo(true));
            return;
        }
        assertThat(((DoubleBlock) result).getDouble(position), closeTo(sum / count, 0.001));
    }
}
