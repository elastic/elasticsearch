/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

public final class PackDimsAggregatorFunctionSupplier implements AggregatorFunctionSupplier {

    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return PackDimsGroupingAggregatorFunction.INTERMEDIATE_STATE_DESC;
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        throw new UnsupportedOperationException("non-grouping aggregator is not supported");
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        return new PackDimsGroupingAggregatorFunction(channels, driverContext);
    }

    @Override
    public String describe() {
        return "packed dimension values";
    }
}
