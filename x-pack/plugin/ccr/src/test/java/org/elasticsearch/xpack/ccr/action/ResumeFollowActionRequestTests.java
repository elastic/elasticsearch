/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;
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
        ResumeFollowAction.Request request = new ResumeFollowAction.Request();
        request.setFollowerIndex(randomAlphaOfLength(4));

        generateFollowParameters(request.getParameters());
        return request;
    }

    @Override
    protected ResumeFollowAction.Request createXContextTestInstance(XContentType type) {
        // follower index parameter is not part of the request body and is provided in the url path.
        // So this field cannot be used for creating a test instance for xcontent testing.
        ResumeFollowAction.Request request = new ResumeFollowAction.Request();
        generateFollowParameters(request.getParameters());
        return request;
    }

    @Override
    protected ResumeFollowAction.Request doParseInstance(XContentParser parser) throws IOException {
        return ResumeFollowAction.Request.fromXContent(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    static void generateFollowParameters(FollowParameters followParameters) {
        if (randomBoolean()) {
            followParameters.setMaxReadRequestOperationCount(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            followParameters.setMaxOutstandingReadRequests(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            followParameters.setMaxOutstandingWriteRequests(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            followParameters.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES));
        }
        if (randomBoolean()) {
            followParameters.setMaxWriteBufferCount(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            followParameters.setMaxWriteRequestOperationCount(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            followParameters.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            followParameters.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES));
        }
        if (randomBoolean()) {
            followParameters.setMaxRetryDelay(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            followParameters.setReadPollTimeout(TimeValue.timeValueMillis(500));
        }
    }

    public void testValidate() {
        ResumeFollowAction.Request request = new ResumeFollowAction.Request();
        request.setFollowerIndex("index2");
        request.getParameters().setMaxRetryDelay(TimeValue.ZERO);

        ActionRequestValidationException validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[max_retry_delay] must be positive but was [0ms]"));

        request.getParameters().setMaxRetryDelay(TimeValue.timeValueMinutes(10));
        validationException = request.validate();
        assertThat(validationException, notNullValue());
        assertThat(validationException.getMessage(), containsString("[max_retry_delay] must be less than [5m] but was [10m]"));

        request.getParameters().setMaxRetryDelay(TimeValue.timeValueMinutes(1));
        validationException = request.validate();
        assertThat(validationException, nullValue());
    }
}
