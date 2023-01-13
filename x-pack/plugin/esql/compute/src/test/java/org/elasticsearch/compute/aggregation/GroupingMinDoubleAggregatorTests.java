/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.operator.LongDoubleTupleBlockSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Tuple;

import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class GroupingMinDoubleAggregatorTests extends GroupingAggregatorTestCase {

    @Override
    protected SourceOperator simpleInput(int end) {
        return new LongDoubleTupleBlockSourceOperator(LongStream.range(0, end).mapToObj(l -> Tuple.tuple(l % 5, (double) l)));
    }

    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.MIN_DOUBLES;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min of doubles";
    }

    @Override
    public void assertSimpleBucket(Block result, int end, int position, int bucket) {
        assertThat(((DoubleBlock) result).getDouble(position), equalTo((double) bucket));
    }
}
