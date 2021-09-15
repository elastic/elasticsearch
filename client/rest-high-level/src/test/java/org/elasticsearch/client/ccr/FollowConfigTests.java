/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class FollowConfigTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            FollowConfigTests::createTestInstance,
            (followConfig, xContentBuilder) -> {
                xContentBuilder.startObject();
                followConfig.toXContentFragment(xContentBuilder, ToXContent.EMPTY_PARAMS);
                xContentBuilder.endObject();
            },
            FollowConfig::fromXContent)
            .supportsUnknownFields(true)
            .test();
    }

    static FollowConfig createTestInstance() {
        FollowConfig followConfig = new FollowConfig();
        if (randomBoolean()) {
            followConfig.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            followConfig.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            followConfig.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            followConfig.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            followConfig.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            followConfig.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            followConfig.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            followConfig.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            followConfig.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            followConfig.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
        }
        return followConfig;
    }
}
