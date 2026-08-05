/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.inference.InferenceContextTests;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class InferenceActionProxyRequestTests extends AbstractBWCWireSerializationTestCase<InferenceActionProxy.Request> {

    private static final TransportVersion INFERENCE_CONTEXT = TransportVersion.fromName("inference_context");

    public void testConstructor_WithNullTimeout_UsesPlaceholder() {
        var request = new InferenceActionProxy.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomBytesReference(10),
            randomFrom(XContentType.values()),
            null,
            false,
            InferenceContextTests.createRandom()
        );

        assertThat(request.getTimeout(), is(TIMEOUT_NOT_DETERMINED));
    }

    public void testConstructor_WithNonNullTimeout_UsesTimeout() {
        var timeout = randomTimeValue();
        var request = new InferenceActionProxy.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomBytesReference(10),
            randomFrom(XContentType.values()),
            timeout,
            false,
            InferenceContextTests.createRandom()
        );

        assertThat(request.getTimeout(), is(timeout));
    }

    public void testNegativeTimeout_NotEqualToTimeoutNotDefined() {
        var negativeTimeout = new InferenceActionProxy.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomBytesReference(10),
            randomFrom(XContentType.values()),
            TimeValue.MINUS_ONE,
            false,
            InferenceContextTests.createRandom()
        );

        var timeoutNotDetermined = new InferenceActionProxy.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomBytesReference(10),
            randomFrom(XContentType.values()),
            TIMEOUT_NOT_DETERMINED,
            false,
            InferenceContextTests.createRandom()
        );

        assertThat(negativeTimeout, not(timeoutNotDetermined));
    }

    @Override
    protected Writeable.Reader<InferenceActionProxy.Request> instanceReader() {
        return InferenceActionProxy.Request::new;
    }

    @Override
    protected InferenceActionProxy.Request createTestInstance() {
        return new InferenceActionProxy.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomBytesReference(10),
            randomFrom(XContentType.values()),
            randomFrom(randomTimeValue(), null),
            false,
            InferenceContextTests.createRandom()
        );
    }

    @Override
    protected InferenceActionProxy.Request mutateInstance(InferenceActionProxy.Request instance) throws IOException {
        var taskType = instance.getTaskType();
        var inferenceEntityId = instance.getInferenceEntityId();
        var content = instance.getContent();
        var contentType = instance.getContentType();
        var timeout = instance.getTimeout();
        var context = instance.getContext();

        switch (randomIntBetween(0, 5)) {
            case 0 -> taskType = randomValueOtherThan(taskType, () -> randomFrom(TaskType.values()));
            case 1 -> inferenceEntityId = randomValueOtherThan(inferenceEntityId, () -> randomAlphaOfLength(6));
            case 2 -> content = randomValueOtherThan(content, () -> randomBytesReference(10));
            case 3 -> contentType = randomValueOtherThan(contentType, () -> randomFrom(XContentType.values()));
            case 4 -> {
                if (timeout.equals(TIMEOUT_NOT_DETERMINED)) {
                    // Using null as timeout will translate it internally to TIMEOUT_NOT_DETERMINED, which would not mutate the instance
                    timeout = randomValueOtherThan(timeout, ESTestCase::randomTimeValue);
                } else {
                    timeout = randomValueOtherThan(timeout, () -> randomFrom(randomTimeValue(), null));
                }
            }
            case 5 -> context = randomValueOtherThan(context, InferenceContextTests::createRandom);
            default -> throw new UnsupportedOperationException();

        }

        return new InferenceActionProxy.Request(taskType, inferenceEntityId, content, contentType, timeout, false, context);
    }

    @Override
    protected InferenceActionProxy.Request mutateInstanceForVersion(InferenceActionProxy.Request instance, TransportVersion version) {
        var context = instance.getContext();
        var timeout = instance.getTimeout();

        if (version.supports(INFERENCE_CONTEXT) == false) {
            context = InferenceContext.EMPTY_INSTANCE;
        }
        if (version.supports(INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED) == false) {
            if (timeout.equals(TIMEOUT_NOT_DETERMINED)) {
                timeout = BaseInferenceActionRequest.OLD_DEFAULT_TIMEOUT;
            }
        }

        return new InferenceActionProxy.Request(
            instance.getTaskType(),
            instance.getInferenceEntityId(),
            instance.getContent(),
            instance.getContentType(),
            timeout,
            false,
            context
        );
    }
}
