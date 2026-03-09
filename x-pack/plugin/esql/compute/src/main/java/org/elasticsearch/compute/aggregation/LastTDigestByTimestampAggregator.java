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

@Aggregator(
    {
        @IntermediateState(name = "timestamps", type = "LONG"),
        @IntermediateState(name = "values", type = "TDIGEST"),
        @IntermediateState(name = "seen", type = "BOOLEAN") }
)
@GroupingAggregator
public class LastTDigestByTimestampAggregator {
    public static String describe() {
        return "last_TDigest_by_timestamp";
    }

    public static TDigestStates.WithLongSingleState initSingle(DriverContext driverContext) {
        return new TDigestStates.WithLongSingleState(driverContext.breaker());
    }

    public static void combine(TDigestStates.WithLongSingleState current, TDigestHolder tdigest, long timestamp) {
        if (current.isSeen() == false || timestamp > current.longValue()) {
            current.set(timestamp, tdigest);
        }
    }

    public static void combineIntermediate(TDigestStates.WithLongSingleState current, long timestamp, TDigestHolder tdigest, boolean seen) {
        if (seen) {
            if (current.isSeen()) {
                combine(current, tdigest, timestamp);
            } else {
                current.set(timestamp, tdigest);
            }
        }
    }

    public static Block evaluateFinal(TDigestStates.WithLongSingleState current, DriverContext ctx) {
        return current.evaluateFinalTDigest(ctx);
    }

    public static TDigestStates.WithLongGroupingState initGrouping(DriverContext driverContext) {
        return new TDigestStates.WithLongGroupingState(driverContext.bigArrays(), driverContext.breaker());
    }

    public static void combine(TDigestStates.WithLongGroupingState current, int groupId, TDigestHolder tdigest, long timestamp) {
        if (current.seen(groupId) == false || timestamp > current.longValue(groupId)) {
            current.set(groupId, timestamp, tdigest);
        }
    }

    public static void combineIntermediate(
        TDigestStates.WithLongGroupingState current,
        int groupId,
        long timestamp,
        TDigestHolder value,
        boolean seen
    ) {
        if (seen) {
            combine(current, groupId, value, timestamp);
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
