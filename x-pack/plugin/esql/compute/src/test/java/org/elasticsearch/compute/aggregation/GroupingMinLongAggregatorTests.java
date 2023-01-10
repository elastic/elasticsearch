/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;

import static org.hamcrest.Matchers.equalTo;

public class GroupingMinLongAggregatorTests extends GroupingAggregatorTestCase {
    @Override
    protected GroupingAggregatorFunction.Factory aggregatorFunction() {
        return GroupingAggregatorFunction.MIN_LONGS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min of longs";
    }

    @Override
    public void assertSimpleBucket(Block result, int end, int position, int bucket) {
        assertThat(result.getDouble(position), equalTo((double) bucket));
    }
}
