/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.index.search.stats.multi;

import org.elasticsearch.common.metrics.ExponentiallyWeightedMovingRate;
import org.elasticsearch.index.search.stats.SearchStats;

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
public class ServerlessShardSearchLoadRateStats implements ShardSearchLoadRateStats {

    private long lastTrackedTime;

    private ExponentiallyWeightedMovingRate ewmRate;

    private double lambdaInInverseMillis;

    public ServerlessShardSearchLoadRateStats() {}

    @Override
    public void init(SearchStatsSettings settings, Supplier<Long> timeProvider) {
        this.lastTrackedTime = 0L;
        this.lambdaInInverseMillis = Math.log(2.0) / settings.getRecentReadLoadHalfLifeForNewShards().millis();
        this.ewmRate = new ExponentiallyWeightedMovingRate(lambdaInInverseMillis, timeProvider.get());
    }

    @Override
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
}
