/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

import java.time.Duration;
import java.util.function.IntConsumer;

public abstract class TimeSeriesGroupingAggregatorEvaluationContext extends GroupingAggregatorEvaluationContext {
    private IntVector allGroupIds;

    public TimeSeriesGroupingAggregatorEvaluationContext(DriverContext driverContext) {
        super(driverContext);
    }

    /**
     * Returns the full set of group IDs when output filtering is active (i.e., the operator
     * passes only output-aligned groups to aggregators). Window functions need the complete set
     * to produce intermediate results for neighbor lookups during the merge step.
     *
     * @return all group IDs, or {@code null} when no output filtering is applied
     */
    public IntVector allGroupIds() {
        return allGroupIds;
    }

    public void setAllGroupIds(IntVector allGroupIds) {
        this.allGroupIds = allGroupIds;
    }

    /**
     * Returns the inclusive start of the time range, in milliseconds, for the specified group ID.
     * Data points for this group are within the range [rangeStartInMillis, rangeEndInMillis).
     *
     * @param groupId the group ID
     * @return the start of the time range in milliseconds (inclusive)
     */
    public abstract long rangeStartInMillis(int groupId);

    /**
     * Returns the exclusive end of the time range, in milliseconds, for the specified group ID.
     * Data points for this group are within the range [rangeStartInMillis, rangeEndInMillis).
     */
    public abstract long rangeEndInMillis(int groupId);

    /**
     * Calls {@code action} for each group ID in the window starting with {@code startingGroupId}.
     * Groups are ordered by time, and the starting group ID is included.
     *
     * @param startingGroupId the starting group ID
     * @param window          the window duration
     * @param action          called for each group ID within the window
     */
    public abstract void forEachGroupInWindow(int startingGroupId, Duration window, IntConsumer action);

    public abstract int previousGroupId(int currentGroupId);

    public abstract int nextGroupId(int currentGroupId);

    /**
     * Computes and caches the adjacent group ID-s used by {@link #previousGroupId} and {@link #nextGroupId}
     */
    public abstract void computeAdjacentGroupIds();
}
