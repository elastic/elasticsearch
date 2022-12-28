/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.ccr.AutoFollowStats;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

import java.io.IOException;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.ccr.action.AutoFollowStatsTests.randomReadExceptions;
import static org.elasticsearch.xpack.ccr.action.AutoFollowStatsTests.randomTrackingClusters;
import static org.elasticsearch.xpack.ccr.action.StatsResponsesTests.createStatsResponse;

public class AutoFollowStatsResponseTests extends AbstractWireSerializingTestCase<CcrStatsAction.Response> {

    @Override
    protected Writeable.Reader<CcrStatsAction.Response> instanceReader() {
        return CcrStatsAction.Response::new;
    }

    @Override
    protected CcrStatsAction.Response createTestInstance() {
        AutoFollowStats autoFollowStats = new AutoFollowStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomReadExceptions(),
            randomTrackingClusters()
        );
        FollowStatsAction.StatsResponses statsResponse = createStatsResponse();
        return new CcrStatsAction.Response(autoFollowStats, statsResponse);
    }

    public void testChunking() throws IOException {
        final var instance = createTestInstance();
        int chunkCount = 0;
        try (var builder = jsonBuilder()) {
            final var iterator = instance.toXContentChunked(EMPTY_PARAMS);
            while (iterator.hasNext()) {
                iterator.next().toXContent(builder, ToXContent.EMPTY_PARAMS);
                chunkCount += 1;
            }
        } // closing the builder verifies that the XContent is well-formed

        var indexCount = instance.getFollowStats().getStatsResponses().stream().map(s -> s.status().followerIndex()).distinct().count();
        assertEquals(instance.getFollowStats().getStatsResponses().size() + indexCount * 2 + 4, chunkCount);
    }
}
