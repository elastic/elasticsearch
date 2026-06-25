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
import org.elasticsearch.compute.data.TDigestBlock;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * Picks the tdigest with the maximum long sort key.
 */
@Aggregator(
    processNulls = true,
    value = {
        @IntermediateState(name = "sortKeys", type = "LONG"),
        @IntermediateState(name = "values", type = "TDIGEST_BLOCK"),
        @IntermediateState(name = "seen", type = "BOOLEAN") }
)
@GroupingAggregator
public class AllLastTDigestByLongAggregator {
    public static String describe() {
        return "all_last_TDigest_by_long";
    }

    public static TDigestStates.WithLongSingleState initSingle(DriverContext driverContext) {
        return new TDigestStates.WithLongSingleState(driverContext.breaker());
    }

    public static void combine(TDigestStates.WithLongSingleState current, TDigestHolder tdigest, long sortKey) {
        if (current.isSeen() == false || sortKey > current.longValue()) {
            current.set(sortKey, tdigest);
        }
    }

    public static void combineIntermediate(TDigestStates.WithLongSingleState current, long sortKey, TDigestBlock values, boolean seen) {
        if (seen) {
            TDigestHolder tdigest = values.getTDigestHolder(values.getFirstValueIndex(0), new TDigestHolder());
            if (current.isSeen()) {
                combine(current, tdigest, sortKey);
            } else {
                current.set(sortKey, tdigest);
            }
        }
    }

    public static Block evaluateFinal(TDigestStates.WithLongSingleState current, DriverContext ctx) {
        return current.evaluateFinalTDigest(ctx);
    }

    public static TDigestStates.WithLongGroupingState initGrouping(DriverContext driverContext) {
        return new TDigestStates.WithLongGroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(TDigestStates.WithLongGroupingState current, int groupId, TDigestHolder tdigest, long sortKey) {
        if (current.seen(groupId) == false || sortKey > current.longValue(groupId)) {
            current.set(groupId, sortKey, tdigest);
        }
    }

    public static void combineIntermediate(
        TDigestStates.WithLongGroupingState current,
        int groupId,
        long sortKey,
        TDigestBlock values,
        boolean seen,
        int otherPosition
    ) {
        if (seen) {
            TDigestHolder tdigest = values.getTDigestHolder(values.getFirstValueIndex(otherPosition), new TDigestHolder());
            combine(current, groupId, tdigest, sortKey);
        }
    }

    public static Block evaluateFinal(
        TDigestStates.WithLongGroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.evaluateFinalTDigests(selected, ctx.driverContext());
    }
}
