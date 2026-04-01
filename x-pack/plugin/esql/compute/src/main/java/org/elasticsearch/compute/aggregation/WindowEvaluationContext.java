/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;

import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * Direction-specific window evaluation behavior, created per window aggregator from the base
 * time-series context. Forward and backward implementations provide different iteration order
 * and range computation.
 */
public interface WindowEvaluationContext {

    /**
     * Calls {@code action} for each group ID in the window anchored at {@code startingGroupId}.
     */
    void forEachGroupInWindow(int startingGroupId, IntConsumer action);

    /**
     * Calls {@code action} for each extra bucket timestamp that needs to be materialized
     * so the window for {@code groupId} has all required neighbors.
     */
    void forEachBucketInWindow(long groupId, TimeSeriesBlockHash tsBlockHash, LongConsumer action);

    /**
     * Effective start of the window range for the given group (for downstream final aggregation).
     */
    long rangeStartInMillis(int groupId);

    /**
     * Effective end of the window range for the given group (for downstream final aggregation).
     */
    long rangeEndInMillis(int groupId);

    /**
     * Start of the original data range for post-expansion trimming.
     * Return {@link Long#MAX_VALUE} to signal no trimming (the default).
     */
    default long trimRangeStart(long dataRangeStartMillis, long dataRangeEndMillis) {
        return Long.MAX_VALUE;
    }

    /**
     * End of the original data range for post-expansion trimming.
     * Return {@link Long#MIN_VALUE} to signal no trimming (the default).
     */
    default long trimRangeEnd(long dataRangeStartMillis, long dataRangeEndMillis) {
        return Long.MIN_VALUE;
    }

    /**
     * Creates a {@link WindowEvaluationContext} from a base time-series context.
     */
    @FunctionalInterface
    interface Factory {
        WindowEvaluationContext create(TimeSeriesGroupingAggregatorEvaluationContext aggCtx);
    }
}
