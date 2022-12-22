/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;

import java.util.function.Supplier;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class GroupingAvgAggregatorTests extends GroupingAggregatorTestCase {
    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.AVG;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "avg";
    }

    @Override
    public void assertSimpleBucket(Block result, int end, int bucket) {
        Supplier<LongStream> seq = () -> LongStream.range(0, end).filter(l -> l % 5 == bucket);
        double expected = seq.get().mapToDouble(Double::valueOf).sum() / seq.get().count();
        assertThat(result.getDouble(bucket), equalTo(expected));
    }
}
