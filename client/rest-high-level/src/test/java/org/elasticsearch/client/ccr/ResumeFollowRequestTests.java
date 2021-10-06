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
