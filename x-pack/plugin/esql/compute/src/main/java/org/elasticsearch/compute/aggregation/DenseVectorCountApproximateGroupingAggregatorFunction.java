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
 * Specialized {@link CountApproximateGroupingAggregatorFunction} for dense_vectors.
 * Dense vectors are represented as multivalued fields, so we should only
 * count 1 value for each dense_vector row instead of counting the number
 * of dimensions.
 */
public class DenseVectorCountApproximateGroupingAggregatorFunction extends CountApproximateGroupingAggregatorFunction {

    DenseVectorCountApproximateGroupingAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        super(channels, driverContext);
    }

    @Override
    protected int getBlockValueCountAtPosition(Block values, int position) {
        return 1;
    }
}
