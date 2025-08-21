/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * A {@link AggregatorFunctionSupplier} that wraps another, filtering which positions
 * are supplied to the aggregator.
 */
public record FilteredAggregatorFunctionSupplier(AggregatorFunctionSupplier next, EvalOperator.ExpressionEvaluator.Factory filter)
    implements
        AggregatorFunctionSupplier {

    @Override
    public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
        return next.nonGroupingIntermediateStateDesc();
    }

    @Override
    public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
        return next.groupingIntermediateStateDesc();
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
        AggregatorFunction next = this.next.aggregator(driverContext, channels);
        EvalOperator.ExpressionEvaluator filter = null;
        try {
            filter = this.filter.get(driverContext);
            AggregatorFunction result = new FilteredAggregatorFunction(next, filter);
            next = null;
            filter = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(next, filter);
        }
    }

    @Override
    public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
        GroupingAggregatorFunction next = this.next.groupingAggregator(driverContext, channels);
        EvalOperator.ExpressionEvaluator filter = null;
        try {
            filter = this.filter.get(driverContext);
            GroupingAggregatorFunction result = new FilteredGroupingAggregatorFunction(next, filter);
            next = null;
            filter = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(next, filter);
        }
    }

    @Override
    public String describe() {
        return "Filtered[next=" + next.describe() + ", filter=" + filter + "]";
    }
}
