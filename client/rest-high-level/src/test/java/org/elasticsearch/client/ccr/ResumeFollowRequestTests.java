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
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.io.IOException;

import static org.elasticsearch.client.ccr.PutFollowRequestTests.assertFollowConfig;
import static org.hamcrest.Matchers.equalTo;

public class ResumeFollowRequestTests extends AbstractRequestTestCase<ResumeFollowRequest, ResumeFollowAction.Request> {

    @Override
    protected ResumeFollowRequest createClientTestInstance() {
        ResumeFollowRequest resumeFollowRequest = new ResumeFollowRequest("followerIndex");
        if (randomBoolean()) {
            resumeFollowRequest.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
        }
        return resumeFollowRequest;
    }

    @Override
    protected ResumeFollowAction.Request doParseToServerInstance(XContentParser parser) throws IOException {
        return ResumeFollowAction.Request.fromXContent(parser, "followerIndex");
    }

    @Override
    protected void assertInstances(ResumeFollowAction.Request serverInstance, ResumeFollowRequest clientTestInstance) {
        assertThat(serverInstance.getFollowerIndex(), equalTo(clientTestInstance.getFollowerIndex()));
        assertFollowConfig(serverInstance.getParameters(), clientTestInstance);
    }

}
