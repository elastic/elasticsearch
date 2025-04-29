/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.stats;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.SearchStatsSettings;
import org.elasticsearch.index.search.stats.ShardSearchLoadRateStatsService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardSearchLoadRateStatsTests extends ESTestCase {

    private ShardSearchLoadRateStatsService shardStats;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        SearchStatsSettings mockSettings = mock(SearchStatsSettings.class);
        TimeValue mockHalfLife = mock(TimeValue.class);
        when(mockSettings.getRecentReadLoadHalfLifeForNewShards()).thenReturn(mockHalfLife);
        when(mockHalfLife.millis()).thenReturn(1000L);
        AtomicLong fakeClock = new AtomicLong(1_000_000L);
        shardStats = null;
        //shardStats = new NoOpShardSearchLoadRateStats(mockSettings, fakeClock::get);
    }

    public void testZeroDeltaReturnsZeroRate() {
        SearchStats.Stats mockStats = mock(SearchStats.Stats.class);
        when(mockStats.getQueryTimeInMillis()).thenReturn(0L);
        when(mockStats.getFetchTimeInMillis()).thenReturn(0L);
        when(mockStats.getScrollTimeInMillis()).thenReturn(0L);
        when(mockStats.getSuggestTimeInMillis()).thenReturn(0L);
        ShardSearchLoadRateStatsService.SearchLoadRate result = shardStats.getSearchLoadRate(mockStats);

        assertEquals(0L, result.lastTrackedTime());
        assertEquals(0L, result.delta());
        assertEquals(0.0, result.ewmRate(), 0.0);
    }

    public void testInitialLoadRateComputation() {
        SearchStats.Stats mockStats = mock(SearchStats.Stats.class);
        when(mockStats.getQueryTimeInMillis()).thenReturn(100L);
        when(mockStats.getFetchTimeInMillis()).thenReturn(50L);
        when(mockStats.getScrollTimeInMillis()).thenReturn(25L);
        when(mockStats.getSuggestTimeInMillis()).thenReturn(25L);
        ShardSearchLoadRateStatsService.SearchLoadRate result = shardStats.getSearchLoadRate(mockStats);

        assertEquals(200L, result.lastTrackedTime());
        assertEquals(200L, result.delta());
        assertTrue(result.ewmRate() > 0);
    }

    public void testLoadRateComputationConsecutiveCalls() throws InterruptedException {
        SearchStats.Stats mockStats1 = mock(SearchStats.Stats.class);
        when(mockStats1.getQueryTimeInMillis()).thenReturn(100L);
        when(mockStats1.getFetchTimeInMillis()).thenReturn(50L);
        when(mockStats1.getScrollTimeInMillis()).thenReturn(25L);
        when(mockStats1.getSuggestTimeInMillis()).thenReturn(25L);
        shardStats.getSearchLoadRate(mockStats1);

        SearchStats.Stats mockStats2 = mock(SearchStats.Stats.class);
        when(mockStats2.getQueryTimeInMillis()).thenReturn(200L);
        when(mockStats2.getFetchTimeInMillis()).thenReturn(70L);
        when(mockStats2.getScrollTimeInMillis()).thenReturn(30L);
        when(mockStats2.getSuggestTimeInMillis()).thenReturn(30L);
        Thread.sleep(10);
        ShardSearchLoadRateStatsService.SearchLoadRate result = shardStats.getSearchLoadRate(mockStats2);

        assertEquals(330L, result.lastTrackedTime());
        assertEquals(130L, result.delta());
        assertTrue(result.ewmRate() > 0);
    }
}
