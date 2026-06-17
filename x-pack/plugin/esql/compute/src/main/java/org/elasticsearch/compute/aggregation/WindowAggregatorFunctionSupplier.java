/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.operator.DriverContext;

import java.time.Duration;
import java.util.List;

/**
 * A {@link AggregatorFunctionSupplier} that wraps another, and apply a window function on the final aggregation.
 */
public record WindowAggregatorFunctionSupplier(AggregatorFunctionSupplier supplier, Duration window) implements AggregatorFunctionSupplier {

    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        return supplier.nonGroupingIntermediateStateDesc();
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return supplier.groupingIntermediateStateDesc();
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        return supplier.aggregator(driverContext, channels);
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        GroupingAggregatorFunction fn = supplier.groupingAggregator(driverContext, channels);
        return new WindowGroupingAggregatorFunction(fn, supplier, window);
    }

    @Override
    public String describe() {
        return "Window[agg=" + supplier.describe() + ", window=" + window + "]";
    }
}
