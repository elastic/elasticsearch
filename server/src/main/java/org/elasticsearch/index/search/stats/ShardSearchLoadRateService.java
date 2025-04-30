/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

/**
 * Service interface for estimating the search load rate of a shard using provided search statistics.
 * <p>
 * Implementations may apply various heuristics or models, such as exponentially weighted moving averages,
 * to track and estimate the current load on a shard based on its search activity.
 */
public interface ShardSearchLoadRateService {

    /**
     * A no-op implementation of {@code ShardSearchLoadRateService} that always returns {@link SearchLoadRate#NO_OP}.
     * This can be used as a fallback or default when no actual load tracking is required.
     */
    ShardSearchLoadRateService NOOP = (stats) -> SearchLoadRate.NO_OP;

    /**
     * Computes the search load rate based on the provided shard-level search statistics.
     *
     * @param stats the search statistics for the shard, typically including metrics like query count, latency, etc.
     * @return the {@link SearchLoadRate} representing the current estimated load on the shard
     */
    SearchLoadRate getSearchLoadRate(SearchStats.Stats stats);

    /**
     * Represents the search load rate as computed over time using an exponentially weighted moving average (EWM).
     * <p>
     * This record captures the timing of the last update, the delta since the last observation,
     * and the computed rate itself.
     *
     * @param lastTrackedTime the timestamp (e.g., in milliseconds or nanoseconds) of the last update
     * @param delta the elapsed time since the previous update
     * @param ewmRate the current exponentially weighted moving average rate
     */
    record SearchLoadRate(long lastTrackedTime, long delta, double ewmRate) {

        /**
         * A static no-op instance representing a default or zeroed state of {@code SearchLoadRate}.
         * All numeric values are initialized to zero.
         */
        public static final SearchLoadRate NO_OP = new SearchLoadRate(0, 0, 0.0);
    }
}
