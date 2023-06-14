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
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.closeTo;

public class SumDoubleGroupingAggregatorFunctionTests extends GroupingAggregatorFunctionTestCase {
    @Override
    protected SourceOperator simpleInput(int end) {
        return new LongDoubleTupleBlockSourceOperator(
            LongStream.range(0, end).mapToObj(l -> Tuple.tuple(randomLongBetween(0, 4), randomDouble()))
        );
    }

    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.SUM_DOUBLES;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "sum of doubles";
    }

    @Override
    protected void assertSimpleGroup(List<Page> input, Block result, int position, long group) {
        CompensatedSum sum = new CompensatedSum();
        input.stream().flatMapToDouble(p -> allDoubles(p, group)).forEach(sum::add);
        // Won't precisely match in distributed case but will be close
        assertThat(((DoubleBlock) result).getDouble(position), closeTo(sum.value(), 0.01));
    }
}
