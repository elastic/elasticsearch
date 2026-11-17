/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * A {@code double} percentile aggregator. In non-finite mode, used only by the PromQL translation, {@code NaN} and
 * {@code ±Inf} observations are tallied alongside the t-digest (which cannot hold them) and ranked as
 * {@code NaN < -Inf < finite < +Inf}; otherwise such an observation is rejected by the digest.
 */
@Aggregator({ @IntermediateState(name = "quart", type = "BYTES_REF") })
@GroupingAggregator
class PercentileDoubleAggregator {

    public static QuantileStates.SingleState initSingle(
        DriverContext driverContext,
        double percentile,
        double tDigestStateCompression,
        boolean allowNonFinite
    ) {
        return new QuantileStates.SingleState(driverContext.breaker(), percentile, tDigestStateCompression, allowNonFinite);
    }

    public static void combine(QuantileStates.SingleState current, double v) {
        current.add(v);
    }

    public static void combineIntermediate(QuantileStates.SingleState state, BytesRef inValue) {
        state.add(inValue);
    }

    public static Block evaluateFinal(QuantileStates.SingleState state, DriverContext driverContext) {
        return state.evaluatePercentile(driverContext);
    }

    public static QuantileStates.GroupingState initGrouping(
        DriverContext driverContext,
        double percentile,
        double tDigestStateCompression,
        boolean allowNonFinite
    ) {
        return new QuantileStates.GroupingState(
            driverContext.breaker(),
            driverContext.bigArrays(),
            percentile,
            tDigestStateCompression,
            allowNonFinite
        );
    }

    public static void combine(QuantileStates.GroupingState state, int groupId, double v) {
        state.add(groupId, v);
    }

    public static void combineIntermediate(QuantileStates.GroupingState state, int groupId, BytesRef inValue) {
        state.add(groupId, inValue);
    }

    public static Block evaluateFinal(QuantileStates.GroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.evaluatePercentile(selected, ctx.driverContext());
    }
}
