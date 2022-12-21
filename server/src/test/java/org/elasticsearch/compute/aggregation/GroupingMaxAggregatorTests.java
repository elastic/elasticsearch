/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;

import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class GroupingMaxAggregatorTests extends GroupingAggregatorTestCase {
    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.MAX;
    }

    @Override
    public void assertSimpleBucket(Block result, int end, int bucket) {
        double expected = LongStream.range(0, end).filter(l -> l % 5 == bucket).max().getAsLong();
        assertThat(result.getDouble(bucket), equalTo(expected));
    }
}
