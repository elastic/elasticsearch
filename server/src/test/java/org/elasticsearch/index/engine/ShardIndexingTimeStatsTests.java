/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class ShardIndexingTimeStatsTests extends ESTestCase {
    public void testBulkTimeIsTracked() {
        final AtomicLong fakeClock = new AtomicLong();
        final ShardIndexingTimeStats shardIndexingTimeStats = new ShardIndexingTimeStats(fakeClock::get);

        assertThat(shardIndexingTimeStats.totalBulkTimeInNanos(), equalTo(0L));
        try (Releasable unused = shardIndexingTimeStats.startTrackingBulkOperationTime()) {
            fakeClock.addAndGet(120);
            // Ensure that we can get the elapsed time in bulk indexing before the operation completes
            assertThat(shardIndexingTimeStats.totalBulkTimeInNanos(), equalTo(120L));
            fakeClock.addAndGet(10);
        }
        assertThat(shardIndexingTimeStats.totalBulkTimeInNanos(), equalTo(130L));
    }

    public void testMergeTimeIsTracked() {
        final AtomicLong fakeClock = new AtomicLong();
        final ShardIndexingTimeStats shardIndexingTimeStats = new ShardIndexingTimeStats(fakeClock::get);

        assertThat(shardIndexingTimeStats.totalMergeTimeInNanos(), equalTo(0L));
        try (Releasable unused = shardIndexingTimeStats.startTrackingMergeTime()) {
            fakeClock.addAndGet(120);
            // Ensure that we can get the elapsed time in bulk indexing before the operation completes
            assertThat(shardIndexingTimeStats.totalMergeTimeInNanos(), equalTo(120L));
            fakeClock.addAndGet(10);
        }
        assertThat(shardIndexingTimeStats.totalMergeTimeInNanos(), equalTo(130L));
    }

    public void testRefreshTimeIsTracked() throws Exception {
        final AtomicLong fakeClock = new AtomicLong();
        final ShardIndexingTimeStats shardIndexingTimeStats = new ShardIndexingTimeStats(fakeClock::get);
        shardIndexingTimeStats.beforeRefresh();
        fakeClock.addAndGet(60);
        assertThat(shardIndexingTimeStats.totalRefreshTimeInNanos(), equalTo(60L));
        fakeClock.addAndGet(60);
        shardIndexingTimeStats.afterRefresh(true);
        assertThat(shardIndexingTimeStats.totalRefreshTimeInNanos(), equalTo(120L));
    }
}
