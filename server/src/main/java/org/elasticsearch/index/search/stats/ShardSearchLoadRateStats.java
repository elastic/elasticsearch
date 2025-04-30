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
     * Loads the first available {@link ShardSearchLoadRateStats} implementation via Java's Service Provider Interface (SPI)
     * and initializes it with the provided settings and time provider.
     * <p>
     * If no SPI implementation is found on the classpath, a {@link NoOpShardSearchLoadRateStats} is returned as a fallback.
     *
     * @param settings     the configuration settings used to initialize the service
     * @param timeProvider a supplier of the current time, typically in milliseconds or nanoseconds
     * @return a fully initialized {@code ShardSearchLoadRateStats} implementation, or a no-op fallback if none is found
     */
    static ShardSearchLoadRateStats getSpiImplementation(SearchStatsSettings settings, Supplier<Long> timeProvider) {
        return LoaderHolder.LOADER.findFirst()
            .map(impl -> {
                impl.init(settings, timeProvider);
                return impl;
            })
            .orElseGet(NoOpShardSearchLoadRateStats::new);
    }


    /**
     * Internal holder class for lazy, thread-safe, and cached access to the {@link ServiceLoader}.
     */
    class LoaderHolder {
        private static final ServiceLoader<ShardSearchLoadRateStats> LOADER =
            ServiceLoader.load(ShardSearchLoadRateStats.class);
    }
}
