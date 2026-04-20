/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

public abstract class TimeSeriesGroupingAggregatorEvaluationContext extends GroupingAggregatorEvaluationContext {
    private IntVector allGroupIds;

    public TimeSeriesGroupingAggregatorEvaluationContext(DriverContext driverContext) {
        super(driverContext);
    }

    /**
     * Returns the full set of group IDs when output filtering is active.
     * Window aggregations use this to build intermediate states for non-output buckets.
     */
    public IntVector allGroupIds() {
        return allGroupIds;
    }

    public void setAllGroupIds(IntVector allGroupIds) {
        this.allGroupIds = allGroupIds;
    }

    /**
     * Returns the inclusive start of the time range, in milliseconds, for the specified group ID.
     */
    public abstract long rangeStartInMillis(int groupId);

    /**
     * Returns the exclusive end of the time range, in milliseconds, for the specified group ID.
     */
    public abstract long rangeEndInMillis(int groupId);

    /**
     * Invokes {@code action} for each group ID whose bucket falls within
     * {@code [rangeStartMillis, rangeEndMillis)}. The starting group is always visited first.
     */
    public abstract void forEachGroupInRange(int startingGroupId, long rangeStartMillis, long rangeEndMillis, IntConsumer action);

    /**
     * Invokes {@code action} for each bucket timestamp in {@code [rangeStartMillis, rangeEndMillis)}
     * that does not already exist as a group. Used by expansion to create synthetic buckets.
     */
    public abstract void forEachBucketInRange(long rangeStartMillis, long rangeEndMillis, LongConsumer action);

    public abstract int previousGroupId(int currentGroupId);

    public abstract int nextGroupId(int currentGroupId);

    /**
     * Computes and caches the adjacent group IDs. They will be used in #previousGroupId and #nextGroupId.
     */
    public abstract void computeAdjacentGroupIds();
}
