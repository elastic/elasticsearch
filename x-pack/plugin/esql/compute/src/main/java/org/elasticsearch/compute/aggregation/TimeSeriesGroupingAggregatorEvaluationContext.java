/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.operator.DriverContext;

import java.time.Duration;
import java.util.List;

public abstract class TimeSeriesGroupingAggregatorEvaluationContext extends GroupingAggregatorEvaluationContext {
    public TimeSeriesGroupingAggregatorEvaluationContext(DriverContext driverContext) {
        super(driverContext);
    }

    /**
     * Returns the inclusive start of the time range, in milliseconds, for the specified group ID.
     * Data points for this group are within the range [rangeStartInMillis, rangeEndInMillis).
     *
     * @param groupId the group id
     * @return the start of the time range in milliseconds (inclusive)
     */
    public abstract long rangeStartInMillis(int groupId);

    /**
     * Returns the exclusive end of the time range, in milliseconds, for the specified group ID.
     * Data points for this group are within the range [rangeStartInMillis, rangeEndInMillis).
     */
    public abstract long rangeEndInMillis(int groupId);

    /**
     * Returns the group IDs of subsequent groups that belong to the window starting with the {@code startingGroupId}.
     * The time ranges of returned group IDs are within the interval
     * {@code [rangeStartInMillis(startingGroupId), rangeStartInMillis(startingGroupId) + window.toMillis())}.
     *
     * @param startingGroupId the starting group ID
     * @param window          the window duration
     * @return a list of group IDs within the window
     */
    public abstract List<Integer> groupIdsFromWindow(int startingGroupId, Duration window);

    /**
     * Returns the group ID of the group that is chronologically before the specified {@code currentGroupId}.
     * In this case, the current and previous group ID refer to buckets in the same time series.
     * If there is no previous group, returns -1.
     *
     * @param currentGroupId the current group ID
     * @return the previous group ID, or -1 if none exists
     */
    public abstract int previousGroupId(int currentGroupId);
}
