/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
import org.elasticsearch.compute.operator.DriverContext;

import java.time.Duration;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * A {@link AggregatorFunctionSupplier} that wraps another, and apply a window function on the final aggregation.
 */
public record WindowAggregatorFunctionSupplier(
    AggregatorFunctionSupplier supplier,
    Duration window,
    WindowEvaluationContext.Factory ctxFactory
) implements AggregatorFunctionSupplier {

    /**
     * Builds a supplier whose window is {@code [rangeStart, rangeStart + window)}.
     */
    public static AggregatorFunctionSupplier forwardWindowFnSupplier(AggregatorFunctionSupplier supplier, Duration window) {
        return new WindowAggregatorFunctionSupplier(supplier, window, ctx -> new WindowEvaluationContext() {
            @Override
            public void forEachGroupInWindow(int startingGroupId, IntConsumer action) {
                ctx.forEachGroupInWindow(startingGroupId, window, action);
            }

            @Override
            public void forEachBucketInWindow(long groupId, TimeSeriesBlockHash tsBlockHash, LongConsumer action) {
                ctx.forEachBucketInWindow(groupId, window, tsBlockHash, action);
            }

            @Override
            public long rangeStartInMillis(int groupId) {
                return ctx.rangeStartInMillis(groupId);
            }

            @Override
            public long rangeEndInMillis(int groupId) {
                return ctx.rangeStartInMillis(groupId) + window.toMillis();
            }
        });
    }

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
        return new WindowGroupingAggregatorFunction(fn, supplier, window, ctxFactory);
    }

    @Override
    public String describe() {
        return "Window[agg=" + supplier.describe() + ", window=" + window + "]";
    }
}
