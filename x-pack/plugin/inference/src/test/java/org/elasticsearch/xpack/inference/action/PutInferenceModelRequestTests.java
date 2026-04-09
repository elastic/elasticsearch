/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

public class PutInferenceModelRequestTests extends AbstractBWCWireSerializationTestCase<PutInferenceModelAction.Request> {

    private static final TransportVersion INFERENCE_ADD_TIMEOUT_PUT_ENDPOINT = TransportVersion.fromName(
        "inference_add_timeout_put_endpoint"
    );

    @Override
    protected Writeable.Reader<PutInferenceModelAction.Request> instanceReader() {
        return PutInferenceModelAction.Request::new;
    }

    @Override
    protected PutInferenceModelAction.Request createTestInstance() {
        return new PutInferenceModelAction.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomBytesReference(50),
            randomFrom(XContentType.values()),
            randomTimeValue()
        );
    }

    @Override
    protected PutInferenceModelAction.Request mutateInstance(PutInferenceModelAction.Request instance) {
        TaskType taskType = instance.getTaskType();
        String inferenceId = instance.getInferenceEntityId();
        BytesReference content = instance.getContent();
        XContentType contentType = instance.getContentType();
        TimeValue timeout = instance.getTimeout();
        switch (randomInt(4)) {
            case 0 -> taskType = randomValueOtherThan(taskType, () -> randomFrom(TaskType.values()));
            case 1 -> inferenceId = randomValueOtherThan(inferenceId, () -> randomAlphaOfLength(6));
            case 2 -> content = randomValueOtherThan(content, () -> randomBytesReference(50));
            case 3 -> contentType = randomValueOtherThan(contentType, () -> randomFrom(XContentType.values()));
            case 4 -> timeout = randomValueOtherThan(timeout, ESTestCase::randomTimeValue);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PutInferenceModelAction.Request(taskType, inferenceId, content, contentType, timeout);
    }

    @Override
    protected PutInferenceModelAction.Request mutateInstanceForVersion(PutInferenceModelAction.Request instance, TransportVersion version) {
        if (version.supports(INFERENCE_ADD_TIMEOUT_PUT_ENDPOINT)) {
            return instance;
        } else {
            return new PutInferenceModelAction.Request(
                instance.getTaskType(),
                instance.getInferenceEntityId(),
                instance.getContent(),
                instance.getContentType(),
                InferenceAction.Request.DEFAULT_TIMEOUT
            );
        }
    }
}
