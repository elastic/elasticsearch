/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.AbstractRequestTestCase;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
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
