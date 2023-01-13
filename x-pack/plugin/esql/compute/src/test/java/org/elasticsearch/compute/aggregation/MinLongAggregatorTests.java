/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.LongBlock;

import static org.hamcrest.Matchers.equalTo;

public class MinLongAggregatorTests extends AggregatorTestCase {
    @Override
    protected AggregatorFunction.Factory aggregatorFunction() {
        return AggregatorFunction.MIN_LONGS;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "min of longs";
    }

    @Override
    public void assertSimpleResult(int end, Block result) {
        assertThat(((LongBlock) result).getLong(0), equalTo(0L));
    }
}
