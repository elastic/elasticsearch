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
public abstract class AggregatorFunctionSupplier implements Describable {
    
    protected abstract AggregatorFunction aggregator(DriverContext driverContext);

    protected abstract GroupingAggregatorFunction groupingAggregator(DriverContext driverContext);

    public Aggregator.Factory aggregatorFactory(AggregatorMode mode) {
        return new Aggregator.Factory() {
            @Override
            public Aggregator apply(DriverContext driverContext) {
                return new Aggregator(aggregator(driverContext), mode);
            }

            @Override
            public String describe() {
                return AggregatorFunctionSupplier.this.describe();
            }
        };
    }

    public GroupingAggregator.Factory groupingAggregatorFactory(AggregatorMode mode) {
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
