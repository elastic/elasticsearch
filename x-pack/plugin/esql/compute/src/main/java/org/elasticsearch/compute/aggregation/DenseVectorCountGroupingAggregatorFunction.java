/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

/**
 * Specialized CountGroupingAggregatorFunction for dense_vectors. dense_vectors are represented as multivalued fields,
 * so we should only count 1 value for each dense_vector row instead of counting the number of values
 */
public class DenseVectorCountGroupingAggregatorFunction extends CountGroupingAggregatorFunction {

    public static DenseVectorCountGroupingAggregatorFunction create(DriverContext driverContext, List<Integer> inputChannels) {
        return new DenseVectorCountGroupingAggregatorFunction(
            inputChannels,
            new LongArrayState(driverContext.bigArrays(), 0),
            driverContext
        );
    }

    protected DenseVectorCountGroupingAggregatorFunction(List<Integer> channels, LongArrayState state, DriverContext driverContext) {
        super(channels, state, driverContext);
    }

    @Override
    protected int getBlockValueCountAtPosition(Block values, int position) {
        // Count 1 for each position, not the number of values (which is the number of vector dimensions)
        return 1;
    }
}
