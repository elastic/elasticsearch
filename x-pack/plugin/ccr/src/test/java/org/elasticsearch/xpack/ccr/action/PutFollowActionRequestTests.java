/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.io.IOException;

public class PutFollowActionRequestTests extends AbstractSerializingTestCase<PutFollowAction.Request> {

    @Override
    protected Writeable.Reader<PutFollowAction.Request> instanceReader() {
        return PutFollowAction.Request::new;
    }

    @Override
    protected PutFollowAction.Request createTestInstance() {
        PutFollowAction.Request request = new PutFollowAction.Request();
        request.setRemoteCluster(randomAlphaOfLength(4));
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
