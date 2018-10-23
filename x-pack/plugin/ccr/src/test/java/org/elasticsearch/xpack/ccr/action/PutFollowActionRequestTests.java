/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.io.IOException;

public class PutFollowActionRequestTests extends AbstractStreamableXContentTestCase<PutFollowAction.Request> {

    @Override
    protected PutFollowAction.Request createBlankInstance() {
        return new PutFollowAction.Request();
    }

    @Override
    protected PutFollowAction.Request createTestInstance() {
        PutFollowAction.Request request = new PutFollowAction.Request();
        request.setLeaderCluster(randomAlphaOfLength(4));
        request.setLeaderIndex(randomAlphaOfLength(4));
        request.setFollowRequest(ResumeFollowActionRequestTests.createTestRequest());
        return request;
    }

    @Override
    protected PutFollowAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PutFollowAction.Request.fromXContent(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
