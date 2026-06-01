/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * Returns the first non-null tdigest value encountered. Used by FUSE
 * and by {@code FIRST(field, NULL)} when the sort field is null or constant.
 */
@Aggregator({ @IntermediateState(name = "values", type = "TDIGEST"), @IntermediateState(name = "seen", type = "BOOLEAN") })
@GroupingAggregator
public class AnyTDigestAggregator {
    public static String describe() {
        return "any_TDigest";
    }

    public static TDigestStates.SeenSingleState initSingle(DriverContext driverContext) {
        return new TDigestStates.SeenSingleState(driverContext.breaker());
    }

    public static void combine(TDigestStates.SeenSingleState current, TDigestHolder value) {
        if (current.isSeen() == false) {
            current.set(value);
        }
    }

    public static void combineIntermediate(TDigestStates.SeenSingleState current, TDigestHolder values, boolean seen) {
        if (seen && current.isSeen() == false) {
            current.set(values);
        }
    }

    public static Block evaluateFinal(TDigestStates.SeenSingleState current, DriverContext ctx) {
        return current.evaluateFinalTDigest(ctx);
    }

    public static TDigestStates.SeenGroupingState initGrouping(DriverContext driverContext) {
        return new TDigestStates.SeenGroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(TDigestStates.SeenGroupingState current, int groupId, TDigestHolder value) {
        if (current.seen(groupId) == false) {
            current.set(groupId, value);
        }
    }

    public static void combineIntermediate(TDigestStates.SeenGroupingState current, int groupId, TDigestHolder values, boolean seen) {
        if (seen) {
            combine(current, groupId, values);
        }
    }

    public static Block evaluateFinal(TDigestStates.SeenGroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluateFinalTDigests(selected, ctx.driverContext());
    }
}
