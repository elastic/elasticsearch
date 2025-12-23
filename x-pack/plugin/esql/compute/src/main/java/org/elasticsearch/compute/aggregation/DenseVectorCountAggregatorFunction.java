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
 * Specialized CountAggregatorFunction for dense_vectors. dense_vectors are represented as multivalued fields,
 * so we should only count 1 value for each dense_vector row instead of counting the number of values
 */
public class DenseVectorCountAggregatorFunction extends CountAggregatorFunction {

    public static AggregatorFunctionSupplier supplier() {
        return new DenseVectorCountAggregatorFunctionSupplier();
    }

    public static DenseVectorCountAggregatorFunction create(List<Integer> inputChannels) {
        return new DenseVectorCountAggregatorFunction(inputChannels, new LongState(0));
    }

    protected DenseVectorCountAggregatorFunction(List<Integer> channels, LongState state) {
        super(channels, state);
    }

    @Override
    protected int getBlockTotalValueCount(Block block) {
        if (block.mayHaveNulls() == false) {
            return block.getPositionCount();
        }
        int count = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            // Count 1 for each non-null position, not the total number of values
            // TODO We could include this as a Block operation and implement it directly on subclasses to avoid position by position checks
            if (block.isNull(i) == false) {
                count++;
            }
        }
        return count;
    }

    @Override
    protected int getBlockValueCountAtPosition(Block block, int position) {
        // Count 1 for each position that is not null, not the number of values (which is the number of vector dimensions)
        return block.isNull(position) ? 0 : 1;
    }

    private static class DenseVectorCountAggregatorFunctionSupplier extends CountAggregatorFunctionSupplier {
        @Override
        public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
            return DenseVectorCountAggregatorFunction.create(channels);
        }

        @Override
        public DenseVectorCountGroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return DenseVectorCountGroupingAggregatorFunction.create(driverContext, channels);
        }
    }
}
