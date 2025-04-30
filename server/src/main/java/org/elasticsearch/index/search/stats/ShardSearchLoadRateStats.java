/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import java.util.ServiceLoader;
import java.util.function.Supplier;

/**
 * Service interface for calculating the search load rate on a shard.
 * <p>
 * Implementations of this interface provide a way to compute an exponentially weighted moving rate
 * (EWMRate) of search load based on shard-level statistics.
 */
public interface ShardSearchLoadRateStats {

    /**
     * The implementation of this interface that was loaded via SPI.
     */
    ShardSearchLoadRateStats SPI_IMPLEMENTATION = getSpiImplementation();

    /**
     * Computes the search load rate based on the provided shard-level search statistics.
     *
     * @param stats the search statistics for the shard, typically including metrics like query count, latency, etc.
     * @return the {@link SearchLoadRate} representing the current estimated load on the shard
     */
    SearchLoadRate getSearchLoadRate(SearchStats.Stats stats);

    /**
     * Initializes the service with the given settings and a time provider.
     * <p>
     * This method is a no-op by default and is intended to be optionally overridden by implementations
     * that require setup based on configuration or time-based logic.
     *
     * @param settings the search stats settings used to configure the service
     * @param timeProvider a supplier of the current time (typically in milliseconds or nanoseconds)
     */
    default void init(SearchStatsSettings settings, Supplier<Long> timeProvider) {}

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

    /**
     * Loads an implementation of {@link ShardSearchLoadRateStats} using Java's Service Provider Interface (SPI).
     * <p>
     * If no implementation is found on the classpath, a default {@link NoOpShardSearchLoadRateStats}
     * is returned as a fallback. This ensures the application can safely proceed even when no SPI
     * provider is explicitly registered.
     *
     * @return an implementation of {@code ShardSearchLoadRateStats}, or a no-op fallback if none is found
     */
    private static ShardSearchLoadRateStats getSpiImplementation() {
        ServiceLoader<ShardSearchLoadRateStats> loader = ServiceLoader.load(ShardSearchLoadRateStats.class);
        return loader.findFirst().orElse(new NoOpShardSearchLoadRateStats());
    }
}
