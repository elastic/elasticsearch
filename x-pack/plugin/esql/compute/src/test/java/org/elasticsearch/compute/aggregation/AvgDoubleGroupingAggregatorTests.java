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
import org.elasticsearch.compute.operator.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;

public class AvgDoubleGroupingAggregatorTests extends GroupingAggregatorTestCase {
    @Override
    protected SourceOperator simpleInput(int size) {
        return new LongDoubleTupleBlockSourceOperator(
            LongStream.range(0, size).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomDouble()))
        );
    }

    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.AVG_DOUBLES;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "avg of doubles";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        double[] sum = new double[] { 0 };
        long[] count = new long[] { 0 };
        forEachGroupAndValue(input, (groups, groupOffset, values, valueOffset) -> {
            if (groups.getLong(groupOffset) == group) {
                sum[0] += ((DoubleBlock) values).getDouble(valueOffset);
                count[0]++;
            }
        });
        assertThat(((DoubleBlock) result).getDouble(position), closeTo(sum[0] / count[0], 0.001));
    }
}
