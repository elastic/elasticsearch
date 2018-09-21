/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ccr.action.FollowIndexAction;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
        FollowIndexAction.Request request = new FollowIndexAction.Request();
        request.setLeaderIndex(randomAlphaOfLength(4));
        request.setFollowerIndex(randomAlphaOfLength(4));
        if (randomBoolean()) {
            request.setMaxBatchOperationCount(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxConcurrentReadBatches(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxConcurrentWriteBatches(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxOperationSizeInBytes(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            request.setMaxWriteBufferSize(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxRetryDelay(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.setPollTimeout(TimeValue.timeValueMillis(500));
        }
        return request;
    }

    public void testValidate() {
        FollowIndexAction.Request request = new FollowIndexAction.Request();
        request.setLeaderIndex("index1");
        request.setFollowerIndex("index2");
        request.setMaxRetryDelay(TimeValue.ZERO);

        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[max_retry_delay] must be positive but was [0ms]"));

        request.setMaxRetryDelay(TimeValue.timeValueMinutes(10));
        validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[max_retry_delay] must be less than [5m] but was [10m]"));

        request.setMaxRetryDelay(TimeValue.timeValueMinutes(1));
        validationException = request.validate();
        assertThat(validationException, nullValue());
    }
}
