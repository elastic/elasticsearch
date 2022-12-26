/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;

import static org.hamcrest.Matchers.equalTo;

public class MaxAggregatorTests extends AggregatorTestCase {
    @Override
    protected AggregatorFunction.Factory aggregatorFunction() {
        return AggregatorFunction.MAX;
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "max";
    }

    @Override
    public void assertSimpleResult(int end, Block result) {
        assertThat(result.getDouble(0), equalTo((double) end - 1));
    }
}
