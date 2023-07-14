/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * Builds aggregation implementations, closing over any state required to do so.
 */
public interface AggregatorFunctionSupplier extends Describable {
    AggregatorFunction aggregator();

    GroupingAggregatorFunction groupingAggregator(DriverContext driverContext);

    default Aggregator.Factory aggregatorFactory(AggregatorMode mode) {
        return new Aggregator.Factory() {
            @Override
            public Aggregator get() {
                return new Aggregator(aggregator(), mode);
            }

            @Override
            public String describe() {
                return AggregatorFunctionSupplier.this.describe();
            }
        };
    }

    default GroupingAggregator.Factory groupingAggregatorFactory(AggregatorMode mode) {
        return new GroupingAggregator.Factory() {
            @Override
            public GroupingAggregator apply(DriverContext driverContext) {
                return new GroupingAggregator(groupingAggregator(driverContext), mode);
            }

            @Override
            public String describe() {
                return AggregatorFunctionSupplier.this.describe();
            }
        };
    }
}
