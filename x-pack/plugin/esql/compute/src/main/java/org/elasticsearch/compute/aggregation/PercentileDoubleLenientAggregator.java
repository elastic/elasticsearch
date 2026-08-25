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
 * A {@code double} percentile aggregator that accepts non-finite observations, used only by the PromQL translation.
 * Unlike {@link PercentileDoubleAggregator}, whose t-digest rejects {@code NaN} and {@code ±Inf} outright, this tallies
 * the non-finite observations alongside the digest and resolves the requested rank across both. The tallies travel with
 * the digest in the intermediate state so that a partial aggregation can be merged on the coordinating node.
 */
@Aggregator(
    {
        @IntermediateState(name = "quart", type = "BYTES_REF"),
        @IntermediateState(name = "nan", type = "LONG"),
        @IntermediateState(name = "negInf", type = "LONG"),
        @IntermediateState(name = "posInf", type = "LONG") }
)
@GroupingAggregator
class PercentileDoubleLenientAggregator {

    public static LenientQuantileStates.SingleState initSingle(
        DriverContext driverContext,
        double percentile,
        double tDigestStateCompression
    ) {
        return new LenientQuantileStates.SingleState(driverContext.breaker(), percentile, tDigestStateCompression);
    }

    public static void combine(LenientQuantileStates.SingleState current, double v) {
        current.add(v);
    }

    public static void combineIntermediate(LenientQuantileStates.SingleState state, BytesRef quart, long nan, long negInf, long posInf) {
        state.add(quart, nan, negInf, posInf);
    }

    public static Block evaluateFinal(LenientQuantileStates.SingleState state, DriverContext driverContext) {
        return state.evaluatePercentile(driverContext);
    }

    public static LenientQuantileStates.GroupingState initGrouping(
        DriverContext driverContext,
        double percentile,
        double tDigestStateCompression
    ) {
        return new LenientQuantileStates.GroupingState(
            driverContext.breaker(),
            driverContext.bigArrays(),
            percentile,
            tDigestStateCompression
        );
    }

    public static void combine(LenientQuantileStates.GroupingState state, int groupId, double v) {
        state.add(groupId, v);
    }

    public static void combineIntermediate(
        LenientQuantileStates.GroupingState state,
        int groupId,
        BytesRef quart,
        long nan,
        long negInf,
        long posInf
    ) {
        state.add(groupId, quart, nan, negInf, posInf);
    }

    public static Block evaluateFinal(
        LenientQuantileStates.GroupingState state,
        IntVector selected,
        GroupingAggregatorEvaluationContext ctx
    ) {
        return state.evaluatePercentile(selected, ctx.driverContext());
    }
}
