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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalIndexingStatsTests extends ESTestCase {

    public void testPollUtilizationTracksSuccessfulIndexingTime() {
        InternalIndexingStats internalIndexingStats = new InternalIndexingStats(
            System::nanoTime,
            new IndexingStatsSettings(ClusterSettings.createBuiltInClusterSettings()),
            2 /* number of WRITE thread pool threads, specific number does not matter much */
        );

        ParsedDocument doc = EngineTestCase.createParsedDoc("1", null);
        Engine.Index index = new Engine.Index(Uid.encodeId(doc.id()), 1L, doc);
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);
        final long operationTimeNanos = 500_000;
        when(result.getTook()).thenReturn(operationTimeNanos);

        // Verify that utilization is zero before any writes are done.
        assertThat(internalIndexingStats.pollUtilization(), equalTo(0.0d));

        internalIndexingStats.preIndex(null /* unused */, index);
        internalIndexingStats.postIndex(null /* unused */, index, result);
        final double utilization = internalIndexingStats.pollUtilization();

        // Sleep briefly so that the polling period is greater than the operation time.
        safeSleep(TimeValue.timeValueNanos(2 * operationTimeNanos));

        // Verify that utilization is no longer zero now that a write operation has occurred.
        assertThat(utilization, greaterThan(0.0d));
        assertThat(utilization, lessThanOrEqualTo(1.0d));
    }
}
