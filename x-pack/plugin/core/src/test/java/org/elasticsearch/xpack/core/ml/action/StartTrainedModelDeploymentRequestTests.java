/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationStatus;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class StartTrainedModelDeploymentRequestTests extends AbstractSerializingTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) throws IOException {
        return Request.parseRequest(null, parser);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return createRandom();
    }

    public static Request createRandom() {
        Request request = new Request(randomAlphaOfLength(10));
        if (randomBoolean()) {
            request.setTimeout(TimeValue.parseTimeValue(randomTimeValue(), Request.TIMEOUT.getPreferredName()));
        }
        if (randomBoolean()) {
            request.setWaitForState(randomFrom(AllocationStatus.State.values()));
        }
        if (randomBoolean()) {
            request.setInferenceThreads(randomIntBetween(1, 8));
        }
        if (randomBoolean()) {
            request.setModelThreads(randomIntBetween(1, 8));
        }
        if (randomBoolean()) {
            request.setQueueCapacity(randomIntBetween(1, 10000));
        }
        return request;
    }

    public void testValidate_GivenInferenceThreadsIsZero() {
        Request request = createRandom();
        request.setInferenceThreads(0);

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[inference_threads] must be a positive integer"));
    }

    public void testValidate_GivenInferenceThreadsIsNegative() {
        Request request = createRandom();
        request.setInferenceThreads(randomIntBetween(-100, -1));

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[inference_threads] must be a positive integer"));
    }

    public void testValidate_GivenModelThreadsIsZero() {
        Request request = createRandom();
        request.setModelThreads(0);

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[model_threads] must be a positive integer"));
    }

    public void testValidate_GivenModelThreadsIsNegative() {
        Request request = createRandom();
        request.setModelThreads(randomIntBetween(-100, -1));

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[model_threads] must be a positive integer"));
    }

    public void testValidate_GivenQueueCapacityIsZero() {
        Request request = createRandom();
        request.setQueueCapacity(0);

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[queue_capacity] must be a positive integer"));
    }

    public void testValidate_GivenQueueCapacityIsNegative() {
        Request request = createRandom();
        request.setQueueCapacity(randomIntBetween(Integer.MIN_VALUE, -1));

        ActionRequestValidationException e = request.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(e.getMessage(), containsString("[queue_capacity] must be a positive integer"));
    }

    public void testDefaults() {
        Request request = new Request(randomAlphaOfLength(10));
        assertThat(request.getTimeout(), equalTo(TimeValue.timeValueSeconds(20)));
        assertThat(request.getWaitForState(), equalTo(AllocationStatus.State.STARTED));
        assertThat(request.getInferenceThreads(), equalTo(1));
        assertThat(request.getModelThreads(), equalTo(1));
        assertThat(request.getQueueCapacity(), equalTo(1024));
    }
}
