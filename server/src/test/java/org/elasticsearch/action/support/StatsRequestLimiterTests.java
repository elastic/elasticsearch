/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.action.support.StatsRequestLimiter.MAX_CONCURRENT_STATS_REQUESTS_PER_NODE;

public class StatsRequestLimiterTests extends ESTestCase {

    public void testGrantsPermitsUpToMaxPermits() {
        int maxPermits = randomIntBetween(1, 5);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        StatsRequestLimiter statsRequestLimiter = new StatsRequestLimiter(
            Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), maxPermits).build(),
            clusterSettings
        );

        for (int i = 0; i < maxPermits; i++) {
            assertTrue(statsRequestLimiter.tryAcquire());
        }
        assertFalse(statsRequestLimiter.tryAcquire());
        statsRequestLimiter.release();
        assertTrue(statsRequestLimiter.tryAcquire());
    }

    public void testStatsRequestPermitCanBeDynamicallyUpdated() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        StatsRequestLimiter statsRequestLimiter = new StatsRequestLimiter(
            Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 1).build(),
            clusterSettings
        );

        assertTrue(statsRequestLimiter.tryAcquire());
        assertFalse(statsRequestLimiter.tryAcquire());

        clusterSettings.applySettings(Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 2).build());

        assertTrue(statsRequestLimiter.tryAcquire());

        clusterSettings.applySettings(Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 1).build());

        assertFalse(statsRequestLimiter.tryAcquire());
        statsRequestLimiter.release();
        statsRequestLimiter.release();

        assertTrue(statsRequestLimiter.tryAcquire());
        assertFalse(statsRequestLimiter.tryAcquire());
    }

    public void testMaxConcurrentStatsRequestsPerNodeIsValidated() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings invalidSetting = Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 0).build();
        expectThrows(IllegalArgumentException.class, () -> new StatsRequestLimiter(invalidSetting, clusterSettings));
        new StatsRequestLimiter(Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 1).build(), clusterSettings);
        expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 0).build())
        );
    }
}
