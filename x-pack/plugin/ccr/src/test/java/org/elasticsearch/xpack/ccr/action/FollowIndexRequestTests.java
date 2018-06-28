/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class FollowIndexRequestTests extends AbstractStreamableTestCase<FollowIndexAction.Request> {

    @Override
    protected FollowIndexAction.Request createBlankInstance() {
        return new FollowIndexAction.Request();
    }

    @Override
    protected FollowIndexAction.Request createTestInstance() {
        return createTestRequest();
    }

    static FollowIndexAction.Request createTestRequest() {
        FollowIndexAction.Request request = new FollowIndexAction.Request();
        request.setLeaderIndex(randomAlphaOfLength(4));
        request.setFollowIndex(randomAlphaOfLength(4));
        request.setMaxReadSize(randomIntBetween(1, Integer.MAX_VALUE));
        request.setMaxConcurrentReads(randomIntBetween(1, Integer.MAX_VALUE));
        request.setMaxOperationSizeInBytes(randomNonNegativeLong());
        request.setMaxWriteSize(randomIntBetween(1, Integer.MAX_VALUE));
        request.setMaxConcurrentWrites(randomIntBetween(1, Integer.MAX_VALUE));
        request.setMaxConcurrentWrites(randomIntBetween(1, Integer.MAX_VALUE));
        request.setMaxBufferSize(randomIntBetween(1, Integer.MAX_VALUE));
        return request;
    }
}
