/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.DriverContext;

@Aggregator({ @IntermediateState(name = "value", type = "TDIGEST"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
public class HistogramMergeTDigestAggregator {

    public static TDigestStates.SingleState initSingle(DriverContext driverContext) {
        return new TDigestStates.SingleState(driverContext.breaker());
    }

    public static void combine(TDigestStates.SingleState state, TDigestHolder value) {
        state.add(value);
    }

    public static void combineIntermediate(TDigestStates.SingleState state, TDigestHolder value, boolean seen) {
        if (seen) {
            state.add(value);
        }
    }

    public static Block evaluateFinal(TDigestStates.SingleState state, DriverContext driverContext) {
        return state.evaluateFinal(driverContext);
    }

    public static TDigestStates.GroupingState initGrouping(BigArrays bigArrays, DriverContext driverContext) {
        return new TDigestStates.GroupingState(bigArrays, driverContext.breaker());
    }

    public static void combine(TDigestStates.GroupingState current, int groupId, TDigestHolder value) {
        current.add(groupId, value);
    }

    public static void combineIntermediate(
        TDigestStates.GroupingState state,
        int groupId,
        TDigestHolder value,
        boolean seen
    ) {
        if (seen) {
            state.add(groupId, value);
        }
    }

    public static Block evaluateFinal(
        TDigestStates.GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.evaluateFinal(selected, ctx.driverContext());
    }
}
