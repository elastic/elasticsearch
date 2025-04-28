/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;

public class ShardSearchRRCStats {

    private Long lastTrackedTime;

    private ExponentiallyWeightedMovingAverage ewma;

    private static final double ALPHA = 0.6;

    public ShardSearchRRCStats() {
        this.lastTrackedTime = 0L;
    }

    public ShardStats getShardStatsRRC(SearchStats.Stats stats) {
        long trackedTime = stats.getQueryTimeInMillis() +
            stats.getFetchTimeInMillis() +
            stats.getScrollTimeInMillis() +
            stats.getScrollTimeInMillis();

        long delta = trackedTime - lastTrackedTime;
        lastTrackedTime = trackedTime;

        if (ewma == null) {
            ewma = new ExponentiallyWeightedMovingAverage(ALPHA, delta);
        } else {
            ewma.addValue(delta);
        }
        return new ShardStats(lastTrackedTime, delta, ewma.getAverage());
    }

    public record ShardStats(long lastTrackedTime, long delta, double ewma) {}
}
