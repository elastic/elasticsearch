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
        request.setBatchSize(randomNonNegativeLong());
        request.setConcurrentProcessors(randomIntBetween(0, Integer.MAX_VALUE));
        request.setProcessorMaxTranslogBytes(randomNonNegativeLong());
        return request;
    }
}
