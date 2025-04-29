/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.common.metrics.ExponentiallyWeightedMovingRate;

import java.util.function.Supplier;

/**
 * Represents the statistics for tracking the search load rate of a shard.
 *
 * <p>This class calculates and maintains an exponentially weighted moving rate (EWMRate)
 * for the search load of a shard, based on query, fetch, and scroll times. It is designed
 * to provide a smooth and responsive measure of the shard's search load over time.</p>
 *
 * <p>Key Features:</p>
 * <ul>
 *   <li>Tracks the last recorded search load time and calculates the delta for updates.</li>
 *   <li>Uses an {@code ExponentiallyWeightedMovingRate} to compute a smoothed rate of change.</li>
 *   <li>Provides a method to retrieve the current search load rate as a combination of
 *       the last tracked time, delta, and the EWMRate.</li>
 * </ul>
 *
 * <p>Usage:</p>
 * <ul>
 *   <li>Initialize the class with {@code SearchStatsSettings} to configure the half-life for the EWMRate.</li>
 *   <li>Call {@code getSearchLoadRate} with the current shard stats to update and retrieve the load rate.</li>
 * </ul>
 *
 * @see ExponentiallyWeightedMovingRate
 * @see SearchStats.Stats
 */
public class ShardSearchLoadRateStats {

    private long lastTrackedTime;

    private ExponentiallyWeightedMovingRate ewmRate;

    private double lambdaInInverseMillis;

    public ShardSearchLoadRateStats(SearchStatsSettings settings, Supplier<Long> timeProvider) {
        this.lastTrackedTime = 0L;
        this.lambdaInInverseMillis = Math.log(2.0) / settings.getRecentReadLoadHalfLifeForNewShards().millis();
        this.ewmRate = new ExponentiallyWeightedMovingRate(lambdaInInverseMillis, timeProvider.get());
    }

    /**
     * Returns the current search load rate based on the provided stats.
     *
     * @param stats The current shard stats to calculate the load rate from.
     * @return A {@link SearchLoadRate} object containing the last tracked time, delta, and EWMRate.
     */
    public SearchLoadRate getSearchLoadRate(SearchStats.Stats stats) {
        long trackedTime = stats.getQueryTimeInMillis() +
            stats.getFetchTimeInMillis() +
            stats.getScrollTimeInMillis() +
            stats.getSuggestTimeInMillis();

        long delta = Math.max(0, trackedTime - lastTrackedTime);
        lastTrackedTime = trackedTime;

        long currentTime = System.currentTimeMillis();
        ewmRate.addIncrement(delta,  currentTime);

        return new SearchLoadRate(lastTrackedTime, delta, ewmRate.getRate(currentTime));
    }

    /**
     * Represents the search load rate statistics for a shard.
     *
     * <p>This class encapsulates the last tracked time, delta, and the exponentially weighted moving rate (EWMRate)
     * for the search load of a shard.</p>
     *
     * @param lastTrackedTime The last recorded search load time.
     * @param delta The time difference since the last update.
     * @param ewmRate The exponentially weighted moving rate of the search load.
     */
    public record SearchLoadRate(long lastTrackedTime, long delta, double ewmRate) {}
}
