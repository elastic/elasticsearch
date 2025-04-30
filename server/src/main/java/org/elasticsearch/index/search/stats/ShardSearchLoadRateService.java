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
 * Interface for prewarming the segments of a shard, tailored for consumption at
 * higher volumes than alternative warming strategies (i.e. offline / recovery warming)
 * that are more speculative.
 */
public interface ShardSearchLoadRateService {

    ShardSearchLoadRateService NOOP = (stats) -> SearchLoadRate.NO_OP;

    /**
     * Computes the search load rate based on the provided shard-level search statistics.
     *
     * @param stats the search statistics for the shard, typically including metrics like query count, latency, etc.
     * @return the {@link SearchLoadRate} representing the current estimated load on the shard
     */
    SearchLoadRate getSearchLoadRate(SearchStats.Stats stats);

    /**
     * Represents the rate of search load using an exponentially weighted moving rate (EWMRate).
     *
     * @param lastTrackedTime the last timestamp (in ms or ns, depending on context) when the rate was updated
     * @param delta the time difference since the previous update
     * @param ewmRate the exponentially weighted moving average of the rate
     */
    record SearchLoadRate(long lastTrackedTime, long delta, double ewmRate) {

        /**
         * A no-op instance of {@code SearchLoadRate} representing an empty or default state.
         * All values are set to zero.
         */
        public static final SearchLoadRate NO_OP = new SearchLoadRate(0,0,0.0);
    }
}


