/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class GetAutoFollowPatternResponseTests extends AbstractResponseTestCase<
    GetAutoFollowPatternAction.Response,
    GetAutoFollowPatternResponse> {

    @Override
    protected GetAutoFollowPatternAction.Response createServerTestInstance(XContentType xContentType) {
        int numPatterns = randomIntBetween(0, 16);
        NavigableMap<String, AutoFollowMetadata.AutoFollowPattern> patterns = new TreeMap<>();
        for (int i = 0; i < numPatterns; i++) {
            String remoteCluster = randomAlphaOfLength(4);
            List<String> leaderIndexPatterns = Collections.singletonList(randomAlphaOfLength(4));
            List<String> leaderIndexExclusionsPatterns = randomList(0, randomIntBetween(1, 10), () -> randomAlphaOfLength(4));
            String followIndexNamePattern = randomAlphaOfLength(4);
            final Settings settings =
                Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 4)).build();
            boolean active = randomBoolean();

            Integer maxOutstandingReadRequests = null;
            if (randomBoolean()) {
                maxOutstandingReadRequests = randomIntBetween(0, Integer.MAX_VALUE);
            }
            Integer maxOutstandingWriteRequests = null;
            if (randomBoolean()) {
                maxOutstandingWriteRequests = randomIntBetween(0, Integer.MAX_VALUE);
            }
            Integer maxReadRequestOperationCount = null;
            if (randomBoolean()) {
                maxReadRequestOperationCount = randomIntBetween(0, Integer.MAX_VALUE);
            }
            ByteSizeValue maxReadRequestSize = null;
            if (randomBoolean()) {
                maxReadRequestSize = new ByteSizeValue(randomNonNegativeLong());
            }
            Integer maxWriteBufferCount = null;
            if (randomBoolean()) {
                maxWriteBufferCount = randomIntBetween(0, Integer.MAX_VALUE);
            }
            ByteSizeValue maxWriteBufferSize = null;
            if (randomBoolean()) {
                maxWriteBufferSize = new ByteSizeValue(randomNonNegativeLong());
            }
            Integer maxWriteRequestOperationCount = null;
            if (randomBoolean()) {
                maxWriteRequestOperationCount = randomIntBetween(0, Integer.MAX_VALUE);
            }
            ByteSizeValue maxWriteRequestSize = null;
            if (randomBoolean()) {
                maxWriteRequestSize = new ByteSizeValue(randomNonNegativeLong());
            }
            TimeValue maxRetryDelay =  null;
            if (randomBoolean()) {
                maxRetryDelay = new TimeValue(randomNonNegativeLong());
            }
            TimeValue readPollTimeout = null;
            if (randomBoolean()) {
                readPollTimeout = new TimeValue(randomNonNegativeLong());
            }
            patterns.put(
                randomAlphaOfLength(4),
                new AutoFollowMetadata.AutoFollowPattern(
                    remoteCluster,
                    leaderIndexPatterns,
                    leaderIndexExclusionsPatterns,
                    followIndexNamePattern,
                    settings,
                    active,
                    maxReadRequestOperationCount,
                    maxWriteRequestOperationCount,
                    maxOutstandingReadRequests,
                    maxOutstandingWriteRequests,
                    maxReadRequestSize,
                    maxWriteRequestSize,
                    maxWriteBufferCount,
                    maxWriteBufferSize,
                    maxRetryDelay,
                    readPollTimeout
                )
            );
        }
        return new GetAutoFollowPatternAction.Response(patterns);
    }

    @Override
    protected GetAutoFollowPatternResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetAutoFollowPatternResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(GetAutoFollowPatternAction.Response serverTestInstance, GetAutoFollowPatternResponse clientInstance) {
        assertThat(serverTestInstance.getAutoFollowPatterns().size(), equalTo(clientInstance.getPatterns().size()));
        for (Map.Entry<String, AutoFollowMetadata.AutoFollowPattern> entry : serverTestInstance.getAutoFollowPatterns().entrySet()) {
            AutoFollowMetadata.AutoFollowPattern serverPattern = entry.getValue();
            GetAutoFollowPatternResponse.Pattern clientPattern = clientInstance.getPatterns().get(entry.getKey());
            assertThat(clientPattern, notNullValue());

            assertThat(serverPattern.getRemoteCluster(), equalTo(clientPattern.getRemoteCluster()));
            assertThat(serverPattern.getLeaderIndexPatterns(), equalTo(clientPattern.getLeaderIndexPatterns()));
            assertThat(serverPattern.getFollowIndexPattern(), equalTo(clientPattern.getFollowIndexNamePattern()));
            assertThat(serverPattern.getLeaderIndexExclusionPatterns(), equalTo(clientPattern.getLeaderIndexExclusionPatterns()));
            assertThat(serverPattern.getSettings(), equalTo(clientPattern.getSettings()));
            assertThat(serverPattern.getMaxOutstandingReadRequests(), equalTo(clientPattern.getMaxOutstandingReadRequests()));
            assertThat(serverPattern.getMaxOutstandingWriteRequests(), equalTo(clientPattern.getMaxOutstandingWriteRequests()));
            assertThat(serverPattern.getMaxReadRequestOperationCount(), equalTo(clientPattern.getMaxReadRequestOperationCount()));
            assertThat(serverPattern.getMaxWriteRequestOperationCount(), equalTo(clientPattern.getMaxWriteRequestOperationCount()));
            assertThat(serverPattern.getMaxReadRequestSize(), equalTo(clientPattern.getMaxReadRequestSize()));
            assertThat(serverPattern.getMaxWriteRequestSize(), equalTo(clientPattern.getMaxWriteRequestSize()));
            assertThat(serverPattern.getMaxWriteBufferCount(), equalTo(clientPattern.getMaxWriteBufferCount()));
            assertThat(serverPattern.getMaxWriteBufferSize(), equalTo(clientPattern.getMaxWriteBufferSize()));
            assertThat(serverPattern.getMaxRetryDelay(), equalTo(clientPattern.getMaxRetryDelay()));
            assertThat(serverPattern.getReadPollTimeout(), equalTo(clientPattern.getReadPollTimeout()));
        }
    }

}
