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
        return block.getPositionCount();
    }

    @Override
    protected int getBlockValueCount(Block block, int position) {
        return 1;
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
