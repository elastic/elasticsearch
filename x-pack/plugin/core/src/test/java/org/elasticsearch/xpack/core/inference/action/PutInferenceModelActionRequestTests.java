/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.utils.MlStringsTests;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.OLD_DEFAULT_TIMEOUT;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;
import static org.hamcrest.Matchers.is;

public class PutInferenceModelActionRequestTests extends AbstractBWCWireSerializationTestCase<PutInferenceModelAction.Request> {
    private static final TransportVersion INFERENCE_ADD_TIMEOUT_PUT_ENDPOINT = TransportVersion.fromName(
        "inference_add_timeout_put_endpoint"
    );

    private TaskType taskType;
    private String modelId;
    private XContentType xContentType;
    private BytesReference bytes;

    @Before
    public void setup() throws Exception {
        taskType = randomFrom(TaskType.values());
        modelId = randomAlphaOfLengthBetween(1, 10).toLowerCase(Locale.ROOT);
        xContentType = randomFrom(XContentType.values());
        bytes = new BytesArray(randomAlphaOfLengthBetween(1, 10));
    }

    public void testConstructor_WithNullTimeout_UsesPlaceholder() {
        var request = new PutInferenceModelAction.Request(randomFrom(TaskType.values()), modelId, bytes, xContentType, null);
        assertThat(request.getTimeout(), is(TIMEOUT_NOT_DETERMINED));
    }

    public void testConstructor_WithNonNullTimeout_UsesTimeout() {
        var timeout = randomTimeValue();
        var request = new PutInferenceModelAction.Request(randomFrom(TaskType.values()), modelId, bytes, xContentType, timeout);
        assertThat(request.getTimeout(), is(timeout));
    }

    public void testValidate() {
        // valid model ID
        var request = new PutInferenceModelAction.Request(taskType, modelId + "_-0", bytes, xContentType, null);
        ActionRequestValidationException validationException = request.validate();
        assertNull(validationException);

        // invalid model IDs

        var invalidRequest = new PutInferenceModelAction.Request(taskType, "", bytes, xContentType, null);
        validationException = invalidRequest.validate();
        assertNotNull(validationException);

        var invalidRequest2 = new PutInferenceModelAction.Request(
            taskType,
            randomAlphaOfLengthBetween(1, 10) + randomFrom(MlStringsTests.SOME_INVALID_CHARS),
            bytes,
            xContentType,
            null
        );
        validationException = invalidRequest2.validate();
        assertNotNull(validationException);

        var invalidRequest3 = new PutInferenceModelAction.Request(taskType, null, bytes, xContentType, null);
        validationException = invalidRequest3.validate();
        assertNotNull(validationException);
    }

    public void testValidate_ReturnsException_WhenIdStartsWithADot() {
        var invalidRequest = new PutInferenceModelAction.Request(taskType, ".elser-2-elastic", bytes, xContentType, null);
        var validationException = invalidRequest.validate();
        assertNotNull(validationException);
    }

    @Override
    protected PutInferenceModelAction.Request mutateInstanceForVersion(PutInferenceModelAction.Request instance, TransportVersion version) {
        var timeout = instance.getTimeout();
        if (version.supports(INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED) == false) {
            if (timeout.equals(TIMEOUT_NOT_DETERMINED)) {
                timeout = OLD_DEFAULT_TIMEOUT;
            }
        }
        if (version.supports(INFERENCE_ADD_TIMEOUT_PUT_ENDPOINT) == false) {
            timeout = TIMEOUT_NOT_DETERMINED;
        }
        return new PutInferenceModelAction.Request(
            instance.getTaskType(),
            instance.getInferenceEntityId(),
            instance.getContent(),
            instance.getContentType(),
            timeout
        );
    }

    @Override
    protected Writeable.Reader<PutInferenceModelAction.Request> instanceReader() {
        return PutInferenceModelAction.Request::new;
    }

    @Override
    protected PutInferenceModelAction.Request createTestInstance() {
        return new PutInferenceModelAction.Request(
            randomFrom(TaskType.values()),
            randomIdentifier(),
            randomBytesReference(10),
            randomFrom(XContentType.values()),
            randomFrom(randomTimeValue(), null)
        );
    }

    @Override
    protected PutInferenceModelAction.Request mutateInstance(PutInferenceModelAction.Request instance) throws IOException {
        var taskType = instance.getTaskType();
        var inferenceEntityId = instance.getInferenceEntityId();
        var content = instance.getContent();
        var contentType = instance.getContentType();
        var timeout = instance.getTimeout();
        switch (randomIntBetween(0, 4)) {
            case 0 -> taskType = randomValueOtherThan(taskType, () -> randomFrom(TaskType.values()));
            case 1 -> inferenceEntityId = randomValueOtherThan(inferenceEntityId, ESTestCase::randomIdentifier);
            case 2 -> content = randomValueOtherThan(content, () -> randomBytesReference(10));
            case 3 -> contentType = randomValueOtherThan(contentType, () -> randomFrom(XContentType.values()));
            case 4 -> {
                if (timeout == null || timeout.equals(TIMEOUT_NOT_DETERMINED)) {
                    // Using null as timeout will translate it internally to TIMEOUT_NOT_DETERMINED, which would not mutate the instance
                    timeout = randomValueOtherThan(timeout, ESTestCase::randomTimeValue);
                } else {
                    timeout = randomValueOtherThan(timeout, () -> randomFrom(randomTimeValue(), null));
                }
            }
        }

        return new PutInferenceModelAction.Request(taskType, inferenceEntityId, content, contentType, timeout);
    }
}
