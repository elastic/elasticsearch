/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

/**
 * Specialized {@link CountApproximateAggregatorFunction} for dense_vectors.
 * Dense vectors are represented as multivalued fields, so we should only
 * count 1 value for each dense_vector row instead of counting the number
 * of dimensions.
 */
public class DenseVectorCountApproximateAggregatorFunction extends CountApproximateAggregatorFunction {

    public static AggregatorFunctionSupplier supplier() {
        return new DenseVectorCountApproximateAggregatorFunctionSupplier();
    }

    DenseVectorCountApproximateAggregatorFunction(DriverContext driverContext, List<ExpressionEvaluator> inputs, DoubleState state) {
        super(driverContext, inputs, state);
    }

    @Override
    protected int getBlockTotalValueCount(Block block) {
        if (block.mayHaveNulls() == false) {
            return block.getPositionCount();
        }
        int count = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i) == false) {
                count++;
            }
        }
        return count;
    }

    @Override
    protected int getBlockValueCountAtPosition(Block block, int position) {
        return block.isNull(position) ? 0 : 1;
    }

    private static class DenseVectorCountApproximateAggregatorFunctionSupplier extends CountApproximateAggregatorFunctionSupplier {
        @Override
        public AggregatorFunction aggregator(DriverContext driverContext, List<ExpressionEvaluator> inputs) {
            return new DenseVectorCountApproximateAggregatorFunction(driverContext, inputs, new DoubleState(0));
        }

        @Override
        public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new DenseVectorCountApproximateGroupingAggregatorFunction(channels, driverContext);
        }
    }
}
