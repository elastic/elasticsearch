/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.ShardFollowNodeTaskStatus;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StatsResponsesTests extends AbstractWireSerializingTestCase<FollowStatsAction.StatsResponses> {

    @Override
    protected Writeable.Reader<FollowStatsAction.StatsResponses> instanceReader() {
        return FollowStatsAction.StatsResponses::new;
    }

    @Override
    protected FollowStatsAction.StatsResponses createTestInstance() {
        return createStatsResponse();
    }

    @Override
    protected FollowStatsAction.StatsResponses mutateInstance(FollowStatsAction.StatsResponses instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    static FollowStatsAction.StatsResponses createStatsResponse() {
        int numResponses = randomIntBetween(0, 8);
        List<FollowStatsAction.StatsResponse> responses = new ArrayList<>(numResponses);
        for (int i = 0; i < numResponses; i++) {
            ShardFollowNodeTaskStatus status = new ShardFollowNodeTaskStatus(
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomInt(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                Collections.emptyNavigableMap(),
                randomNonNegativeLong(),
                randomBoolean() ? new ElasticsearchException("fatal error") : null
            );
            responses.add(new FollowStatsAction.StatsResponse(status));
        }
        return new FollowStatsAction.StatsResponses(Collections.emptyList(), Collections.emptyList(), responses);
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(
            createTestInstance(),
            instance -> Math.toIntExact(
                2 * instance.getStatsResponses().stream().map(s -> s.status().followerIndex()).distinct().count() + instance
                    .getStatsResponses()
                    .size() + 2
            )
        );
    }
}
