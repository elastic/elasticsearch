/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

/**
 * Builds aggregation implementations, closing over any state required to do so.
 */
public interface AggregatorFunctionSupplier extends Describable {
    List<IntermediateStateDesc> nonGroupingIntermediateStateDesc();

    List<IntermediateStateDesc> groupingIntermediateStateDesc();

    AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels);

    GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels);

    default Aggregator.Factory aggregatorFactory(AggregatorMode mode, List<Integer> channels) {
        return new Aggregator.Factory() {
            @Override
            public Aggregator apply(DriverContext driverContext) {
                return new Aggregator(aggregator(driverContext, channels), mode);
            }

            @Override
            public String describe() {
                return AggregatorFunctionSupplier.this.describe();
            }
        };
    }

    default GroupingAggregator.Factory groupingAggregatorFactory(AggregatorMode mode, List<Integer> channels) {
        return new GroupingAggregator.Factory() {
            @Override
            public GroupingAggregator apply(DriverContext driverContext) {
                return new GroupingAggregator(groupingAggregator(driverContext, channels), mode);
            }

            @Override
            public String describe() {
                return AggregatorFunctionSupplier.this.describe();
            }
        };
    }
}
