/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * Provides a context for evaluating a grouping aggregator.
 * A time-series aggregator might need more than just the DriverContext to evaluate, such as the range interval of each group.
 */
public class GroupingAggregatorEvaluationContext {
    private final DriverContext driverContext;

    public GroupingAggregatorEvaluationContext(DriverContext driverContext) {
        this.driverContext = driverContext;
    }

    public DriverContext driverContext() {
        return driverContext;
    }

    public BlockFactory blockFactory() {
        return driverContext.blockFactory();
    }
}
