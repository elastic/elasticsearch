/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ResumeFollowActionRequestTests extends AbstractSerializingTestCase<ResumeFollowAction.Request> {

    @Override
    protected Writeable.Reader<ResumeFollowAction.Request> instanceReader() {
        return ResumeFollowAction.Request::new;
    }

    @Override
    protected ResumeFollowAction.Request createTestInstance() {
        return createTestRequest();
    }

    @Override
    protected ResumeFollowAction.Request doParseInstance(XContentParser parser) throws IOException {
        return ResumeFollowAction.Request.fromXContent(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    static ResumeFollowAction.Request createTestRequest() {
        ResumeFollowAction.Request request = new ResumeFollowAction.Request();
        request.setFollowerIndex(randomAlphaOfLength(4));
        if (randomBoolean()) {
            request.setMaxReadRequestOperationCount(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxOutstandingReadRequests(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxOutstandingWriteRequests(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES));
        }
        if (randomBoolean()) {
            request.setMaxWriteBufferCount(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxWriteRequestOperationCount(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES));
        }
        if (randomBoolean()) {
            request.setMaxRetryDelay(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.setReadPollTimeout(TimeValue.timeValueMillis(500));
        }
        return request;
    }

    public void testValidate() {
        ResumeFollowAction.Request request = new ResumeFollowAction.Request();
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
