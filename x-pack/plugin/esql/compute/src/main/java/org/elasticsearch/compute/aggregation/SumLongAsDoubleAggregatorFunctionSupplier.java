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
 * {@link AggregatorFunctionSupplier} for {@link SumLongAsDoubleAggregatorFunction}. Its intermediate state is the one of
 * {@code SumDoubleAggregatorFunctionSupplier}, see {@link SumLongAsDoubleAggregatorFunction} for why.
 */
public final class SumLongAsDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
    public SumLongAsDoubleAggregatorFunctionSupplier() {}

    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        return SumLongAsDoubleAggregatorFunction.intermediateStateDesc();
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return SumDoubleGroupingAggregatorFunction.intermediateStateDesc();
    }

    @Override
    public SumLongAsDoubleAggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        return new SumLongAsDoubleAggregatorFunction(channels);
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        throw new UnsupportedOperationException("sum of longs as doubles is only implemented without grouping");
    }

    @Override
    public String describe() {
        return "sum of longs as doubles";
    }
}
