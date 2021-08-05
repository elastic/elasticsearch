/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.AbstractRequestTestCase;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class PutFollowRequestTests extends AbstractRequestTestCase<PutFollowRequest, PutFollowAction.Request> {

    @Override
    protected PutFollowRequest createClientTestInstance() {
        PutFollowRequest putFollowRequest =
            new PutFollowRequest(randomAlphaOfLength(4), randomAlphaOfLength(4), "followerIndex");
        if (randomBoolean()) {
            putFollowRequest.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putFollowRequest.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
        }
        return putFollowRequest;
    }

    @Override
    protected PutFollowAction.Request doParseToServerInstance(XContentParser parser) throws IOException {
        return PutFollowAction.Request.fromXContent(parser, "followerIndex", ActiveShardCount.DEFAULT);
    }

    @Override
    protected void assertInstances(PutFollowAction.Request serverInstance, PutFollowRequest clientTestInstance) {
        assertThat(serverInstance.getRemoteCluster(), equalTo(clientTestInstance.getRemoteCluster()));
        assertThat(serverInstance.getLeaderIndex(), equalTo(clientTestInstance.getLeaderIndex()));
        assertThat(serverInstance.getFollowerIndex(), equalTo(clientTestInstance.getFollowerIndex()));
        assertFollowConfig(serverInstance.getParameters(), clientTestInstance);
    }

    static void assertFollowConfig(FollowParameters serverParameters, FollowConfig clientConfig) {
        assertThat(serverParameters.getMaxReadRequestOperationCount(), equalTo(clientConfig.getMaxReadRequestOperationCount()));
        assertThat(serverParameters.getMaxWriteRequestOperationCount(), equalTo(clientConfig.getMaxWriteRequestOperationCount()));
        assertThat(serverParameters.getMaxOutstandingReadRequests(), equalTo(clientConfig.getMaxOutstandingReadRequests()));
        assertThat(serverParameters.getMaxOutstandingWriteRequests(), equalTo(clientConfig.getMaxOutstandingWriteRequests()));
        assertThat(serverParameters.getMaxReadRequestSize(), equalTo(clientConfig.getMaxReadRequestSize()));
        assertThat(serverParameters.getMaxWriteRequestSize(), equalTo(clientConfig.getMaxWriteRequestSize()));
        assertThat(serverParameters.getMaxWriteBufferCount(), equalTo(clientConfig.getMaxWriteBufferCount()));
        assertThat(serverParameters.getMaxWriteBufferSize(), equalTo(clientConfig.getMaxWriteBufferSize()));
        assertThat(serverParameters.getMaxRetryDelay(), equalTo(clientConfig.getMaxRetryDelay()));
        assertThat(serverParameters.getReadPollTimeout(), equalTo(clientConfig.getReadPollTimeout()));
    }

}
