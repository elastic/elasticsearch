/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalIndexingStatsTests extends ESTestCase {

    public void testPollUtilizationTracksSuccessfulIndexingTime() {
        AtomicLong currentTime = new AtomicLong(0);
        final int numThreads = randomIntBetween(1, 8);
        InternalIndexingStats internalIndexingStats = new InternalIndexingStats(
            () -> currentTime.get(),
            new IndexingStatsSettings(ClusterSettings.createBuiltInClusterSettings()),
            numThreads
        );

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(Uid.encodeId(doc.id()), 1L, doc);
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);
        final long operationTimeNanos = 500_000;
        when(result.getTook()).thenReturn(operationTimeNanos);

        // Verify that utilization is zero before any writes are done.
        currentTime.set(1000L);
        assertThat(internalIndexingStats.pollUtilization(), equalTo(0.0d));

        internalIndexingStats.preIndex(null /* unused */, index);
        internalIndexingStats.postIndex(null /* unused */, index, result);
        currentTime.set(operationTimeNanos + 1000L);  // Advance time by the operation time to simulate the passage of time.
        final double utilization = internalIndexingStats.pollUtilization();

        // Verify that utilization is no longer zero now that a write operation has occurred.
        assertThat(utilization, greaterThan(0.0d));

        // One operation ran the duration of the time window polled. So the utilization is relative to the number of threads available.
        assertEquals(1.0 / numThreads, utilization, 0.0001);
    }
}
