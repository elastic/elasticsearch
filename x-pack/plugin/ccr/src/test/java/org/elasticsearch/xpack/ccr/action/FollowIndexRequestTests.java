/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ccr.action.FollowIndexAction;

import java.io.IOException;

public class FollowIndexRequestTests extends AbstractStreamableXContentTestCase<FollowIndexAction.Request> {

    @Override
    protected FollowIndexAction.Request createBlankInstance() {
        return new FollowIndexAction.Request();
    }

    @Override
    protected FollowIndexAction.Request createTestInstance() {
        return createTestRequest();
    }

    @Override
    protected FollowIndexAction.Request doParseInstance(XContentParser parser) throws IOException {
        return FollowIndexAction.Request.fromXContent(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    static FollowIndexAction.Request createTestRequest() {
        return new FollowIndexAction.Request(randomAlphaOfLength(4), randomAlphaOfLength(4), randomIntBetween(1, Integer.MAX_VALUE),
            randomIntBetween(1, Integer.MAX_VALUE), randomNonNegativeLong(), randomIntBetween(1, Integer.MAX_VALUE),
            randomIntBetween(1, Integer.MAX_VALUE), TimeValue.timeValueMillis(500), TimeValue.timeValueMillis(500));
    }
}
