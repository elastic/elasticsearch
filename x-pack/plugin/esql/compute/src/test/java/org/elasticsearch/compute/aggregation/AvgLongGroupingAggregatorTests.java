/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;

import java.util.function.Supplier;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class AvgLongGroupingAggregatorTests extends GroupingAggregatorTestCase {
    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.AVG_LONGS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "avg of longs";
    }

    @Override
    public void assertSimpleBucket(Block result, int end, int position, int bucket) {
        Supplier<LongStream> seq = () -> LongStream.range(0, end).filter(l -> l % 5 == bucket);
        double expected = seq.get().mapToDouble(Double::valueOf).sum() / seq.get().count();
        assertThat(result.getDouble(position), equalTo(expected));
    }
}
