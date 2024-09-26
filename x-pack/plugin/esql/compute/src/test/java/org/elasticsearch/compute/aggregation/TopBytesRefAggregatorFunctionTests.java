/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;

import java.util.List;

public class TopBytesRefAggregatorFunctionTests extends AbstractTopBytesRefAggregatorFunctionTests {
    @Override
    protected BytesRef randomValue() {
        return new BytesRef(randomAlphaOfLength(10));
    }

    @Override
    protected AggregatorFunctionSupplier aggregatorFunction(List<Integer> inputChannels) {
        return new TopBytesRefAggregatorFunctionSupplier(inputChannels, LIMIT, true);
    }

    @Override
    protected String expectedDescriptionOfAggregator() {
        return "top of bytes";
    }
}
