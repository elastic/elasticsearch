/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ShardWriteLoadStatsCollectorTests extends ESTestCase {
    public void testBulkTimeIsTracked() {
        final AtomicLong fakeClock = new AtomicLong();
        final ShardWriteLoadStatsCollector shardWriteLoadStatsCollector = new ShardWriteLoadStatsCollector(fakeClock::get);

        assertThat(shardWriteLoadStatsCollector.totalBulkTimeInNanos(), equalTo(0L));
        try (Releasable unused = shardWriteLoadStatsCollector.startTrackingBulkOperationTime()) {
            fakeClock.addAndGet(120);
            // Ensure that we can get the elapsed time in bulk indexing before the operation completes
            assertThat(shardWriteLoadStatsCollector.inFlightOps(), is(equalTo(1)));
            assertThat(shardWriteLoadStatsCollector.totalBulkTimeInNanos(), equalTo(120L));
            fakeClock.addAndGet(10);
        }
        assertThat(shardWriteLoadStatsCollector.totalBulkTimeInNanos(), equalTo(130L));
        assertThat(shardWriteLoadStatsCollector.inFlightOps(), is(equalTo(0)));
    }
}
