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

import org.elasticsearch.client.AbstractRequestTestCase;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
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
            randomAlphaOfLength(4), Arrays.asList(generateRandomStringArray(4, 4, false)));
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
        assertThat(serverInstance.getFollowIndexNamePattern(), equalTo(clientTestInstance.getFollowIndexNamePattern()));
        assertFollowConfig(serverInstance.getParameters(), clientTestInstance);
    }

}
