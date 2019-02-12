/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
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
        request.setFollowerIndex(randomAlphaOfLength(4));
        request.waitForActiveShards(randomFrom(ActiveShardCount.DEFAULT, ActiveShardCount.NONE, ActiveShardCount.ONE,
            ActiveShardCount.ALL));

        request.setRemoteCluster(randomAlphaOfLength(4));
        request.setLeaderIndex(randomAlphaOfLength(4));
        ResumeFollowActionRequestTests.generateFollowParameters(request.getParameters());
        return request;
    }

    @Override
    protected PutFollowAction.Request createXContextTestInstance(XContentType xContentType) {
        // follower index parameter and wait for active shards params are not part of the request body and
        // are provided in the url path. So these fields cannot be used for creating a test instance for xcontent testing.
        PutFollowAction.Request request = new PutFollowAction.Request();
        request.setRemoteCluster(randomAlphaOfLength(4));
        request.setLeaderIndex(randomAlphaOfLength(4));
        request.setFollowerIndex("followerIndex");
        ResumeFollowActionRequestTests.generateFollowParameters(request.getParameters());
        return request;
    }

    @Override
    protected PutFollowAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PutFollowAction.Request.fromXContent(parser, "followerIndex", ActiveShardCount.DEFAULT);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
