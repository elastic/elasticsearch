/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

public class SumDenseVectorAggregatorFunctionSupplier implements AggregatorFunctionSupplier {

    public SumDenseVectorAggregatorFunctionSupplier() {}

    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        return List.of(new IntermediateStateDesc("sum", ElementType.FLOAT, "dense_vector"));
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return List.of(new IntermediateStateDesc("sum", ElementType.FLOAT, "dense_vector"));
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        return new SumDenseVectorAggregatorFunction(channels);
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        return SumDenseVectorGroupingAggregatorFunction.create(channels, driverContext);
    }

    @Override
    public String describe() {
        return "sum of dense_vectors";
    }
}
