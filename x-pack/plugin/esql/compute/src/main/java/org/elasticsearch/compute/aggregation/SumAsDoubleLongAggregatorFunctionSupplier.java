/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

/**
 * Builds the aggregators that sum {@code long}s as a {@code double}, see {@link SumAsDoubleLongAggregatorFunction}
 * and {@link SumAsDoubleLongGroupingAggregatorFunction}.
 */
public final class SumAsDoubleLongAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        return SumAsDoubleLongAggregatorFunction.intermediateStateDesc();
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return SumAsDoubleLongGroupingAggregatorFunction.intermediateStateDesc();
    }

    @Override
    public SumAsDoubleLongAggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        return new SumAsDoubleLongAggregatorFunction(channels);
    }

    @Override
    public SumAsDoubleLongGroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        return new SumAsDoubleLongGroupingAggregatorFunction(channels, driverContext);
    }

    @Override
    public String describe() {
        return "sum_as_double of longs";
    }
}
