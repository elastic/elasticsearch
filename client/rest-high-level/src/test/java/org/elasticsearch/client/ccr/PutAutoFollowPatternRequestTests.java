/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.client.AbstractRequestTestCase;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.client.ccr.PutFollowRequestTests.assertFollowConfig;
import static org.hamcrest.Matchers.equalTo;

public class PutAutoFollowPatternRequestTests extends AbstractRequestTestCase<
    PutAutoFollowPatternRequest,
    PutAutoFollowPatternAction.Request> {

    @Override
    protected PutAutoFollowPatternRequest createClientTestInstance() {
        // Name isn't serialized, because it specified in url path, so no need to randomly generate it here.
        PutAutoFollowPatternRequest putAutoFollowPatternRequest = new PutAutoFollowPatternRequest("name",
            randomAlphaOfLength(4),
            Arrays.asList(generateRandomStringArray(4, 4, false)),
            Arrays.asList(generateRandomStringArray(4, 4, false))
        );
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setFollowIndexNamePattern(randomAlphaOfLength(4));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
        }
        return putAutoFollowPatternRequest;
    }

    @Override
    protected PutAutoFollowPatternAction.Request doParseToServerInstance(XContentParser parser) throws IOException {
        return PutAutoFollowPatternAction.Request.fromXContent(parser, "name");
    }

    @Override
    protected void assertInstances(PutAutoFollowPatternAction.Request serverInstance, PutAutoFollowPatternRequest clientTestInstance) {
        assertThat(serverInstance.getName(), equalTo(clientTestInstance.getName()));
        assertThat(serverInstance.getRemoteCluster(), equalTo(clientTestInstance.getRemoteCluster()));
        assertThat(serverInstance.getLeaderIndexPatterns(), equalTo(clientTestInstance.getLeaderIndexPatterns()));
        assertThat(serverInstance.getLeaderIndexExclusionPatterns(), equalTo(clientTestInstance.getLeaderIndexExclusionPatterns()));
        assertThat(serverInstance.getFollowIndexNamePattern(), equalTo(clientTestInstance.getFollowIndexNamePattern()));
        assertFollowConfig(serverInstance.getParameters(), clientTestInstance);
    }

}
