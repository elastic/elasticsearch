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
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.TupleBlockSourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class AvgLongGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.AVG_LONGS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "avg of longs";
    }

    @Override
    protected SourceOperator simpleInput(int size) {
        long max = randomLongBetween(1, Long.MAX_VALUE / size);
        return new TupleBlockSourceOperator(
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomLongBetween(-max, max)))
        );
    }

    @Override
    public void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        double sum = input.stream().flatMapToLong(p -> allLongs(p, group)).sum();
        long count = input.stream().flatMapToLong(p -> allLongs(p, group)).count();
        if (count == 0) {
            // If all values are null we'll have a count of 0. So we'll be null.
            assertThat(result.isNull(position), equalTo(true));
            return;
        }
        assertThat(((DoubleBlock) result).getDouble(position), closeTo(sum / count, 0.001));
    }
}
