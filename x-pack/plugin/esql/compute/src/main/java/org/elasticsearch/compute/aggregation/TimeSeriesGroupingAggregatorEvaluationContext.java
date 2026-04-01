/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.index.mapper.DateFieldMapper;

import java.time.Duration;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

public abstract class TimeSeriesGroupingAggregatorEvaluationContext extends GroupingAggregatorEvaluationContext {
    @FunctionalInterface
    public interface Factory {
        TimeSeriesGroupingAggregatorEvaluationContext create(
            DriverContext driverContext,
            TimeSeriesBlockHash tsBlockHash,
            Rounding.Prepared timeBucketRounding,
            DateFieldMapper.Resolution timeResolution
        );
    }

    public TimeSeriesGroupingAggregatorEvaluationContext(DriverContext driverContext) {
        super(driverContext);
    }

    /**
     * Returns the inclusive start of the evaluation window, in milliseconds, for the specified group ID.
     * <p>
     * The window is the interval used by time-series aggregators for interpolation, extrapolation, and
     * rolling-window evaluation. The group key may be start-aligned or end-aligned, but aggregators should
     * treat the effective interval as {@code [rangeStartInMillis, rangeEndInMillis)}.
     *
     * @param groupId the group ID
     * @return the evaluation window start in milliseconds (inclusive)
     */
    public abstract long rangeStartInMillis(int groupId);

    /**
     * Returns the exclusive end of the evaluation window, in milliseconds, for the specified group ID.
     * See {@link #rangeStartInMillis(int)} for the definition of the evaluation window.
     */
    public abstract long rangeEndInMillis(int groupId);

    /**
     * Calls {@code action} for each group ID that participates in the specified window anchored at
     * {@code startingGroupId}. The traversal order follows time order for the effective bucket semantics,
     * and the starting group ID is included.
     *
     * @param startingGroupId the starting group ID
     * @param window          the window duration
     * @param action          called for each group ID within the window
     */
    public abstract void forEachGroupInWindow(int startingGroupId, Duration window, IntConsumer action);

    /**
     * Calls {@code action} for each extra bucket key whose window includes {@code groupId}.
     *
     * Used to materialize missing buckets before final time-series aggregation so windowed
     * results match sliding-window evaluation over the raw input.
     *
     * @param groupId source group to expand
     * @param window rolling window size
     * @param tsBlockHash block hash used for bucket bounds
     * @param action called for each extra bucket key
     */
    public abstract void forEachBucketInWindow(long groupId, Duration window, TimeSeriesBlockHash tsBlockHash, LongConsumer action);

    public abstract int previousGroupId(int currentGroupId);

    public abstract int nextGroupId(int currentGroupId);

    /**
     * Computes and caches the adjacent group ID-s used by {@link #previousGroupId} and {@link #nextGroupId}
     */
    public abstract void computeAdjacentGroupIds();
}
