/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class UnifiedCompletionActionRequestTests extends AbstractBWCWireSerializationTestCase<UnifiedCompletionAction.Request> {

    public void testValidation_ReturnsException_When_UnifiedCompletionRequestMessage_Is_Null() {
        var request = new UnifiedCompletionAction.Request(
            "inference_id",
            TaskType.COMPLETION,
            UnifiedCompletionRequest.of(null),
            TimeValue.timeValueSeconds(10)
        );
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Field [messages] cannot be null;"));
    }

    public void testValidation_ReturnsException_When_UnifiedCompletionRequest_Is_EmptyArray() {
        var request = new UnifiedCompletionAction.Request(
            "inference_id",
            TaskType.COMPLETION,
            UnifiedCompletionRequest.of(List.of()),
            TimeValue.timeValueSeconds(10)
        );
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Field [messages] cannot be an empty array;"));
    }

    public void testValidation_ReturnsException_When_TaskType_IsNot_Completion() {
        var request = new UnifiedCompletionAction.Request(
            "inference_id",
            TaskType.SPARSE_EMBEDDING,
            UnifiedCompletionRequest.of(List.of(UnifiedCompletionRequestTests.randomMessage())),
            TimeValue.timeValueSeconds(10)
        );
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Field [taskType] must be [chat_completion];"));
    }

    public void testValidation_ReturnsNull_When_TaskType_IsAny() {
        var request = new UnifiedCompletionAction.Request(
            "inference_id",
            TaskType.ANY,
            UnifiedCompletionRequest.of(List.of(UnifiedCompletionRequestTests.randomMessage())),
            TimeValue.timeValueSeconds(10)
        );
        assertNull(request.validate());
    }

    public void testWriteTo_WhenVersionIsBeforeAdaptiveRateLimiting_ShouldSetHasBeenReroutedToTrue() throws IOException {
        var instance = new UnifiedCompletionAction.Request(
            "model",
            TaskType.ANY,
            UnifiedCompletionRequest.of(List.of(UnifiedCompletionRequestTests.randomMessage())),
            TimeValue.timeValueSeconds(10)
        );

        UnifiedCompletionAction.Request deserializedInstance = copyWriteable(
            instance,
            getNamedWriteableRegistry(),
            instanceReader(),
            TransportVersions.ELASTIC_INFERENCE_SERVICE_UNIFIED_CHAT_COMPLETIONS_INTEGRATION
        );

        // Verify that hasBeenRerouted is true after deserializing a request coming from an older transport version
        assertTrue(deserializedInstance.hasBeenRerouted());
    }

    public void testWriteTo_WhenVersionIsBeforeInferenceContext_ShouldSetContextToEmptyContext() throws IOException {
        var instance = new UnifiedCompletionAction.Request(
            "model",
            TaskType.ANY,
            UnifiedCompletionRequest.of(List.of(UnifiedCompletionRequestTests.randomMessage())),
            InferenceContext.EMPTY_INSTANCE,
            TimeValue.timeValueSeconds(10)
        );

        UnifiedCompletionAction.Request deserializedInstance = copyWriteable(
            instance,
            getNamedWriteableRegistry(),
            instanceReader(),
            TransportVersions.ELASTIC_INFERENCE_SERVICE_UNIFIED_CHAT_COMPLETIONS_INTEGRATION
        );
        assertThat(deserializedInstance.getContext(), equalTo(InferenceContext.EMPTY_INSTANCE));
    }

    @Override
    protected UnifiedCompletionAction.Request mutateInstanceForVersion(UnifiedCompletionAction.Request instance, TransportVersion version) {
        if (version.before(TransportVersions.INFERENCE_CONTEXT)) {
            return new UnifiedCompletionAction.Request(
                instance.getInferenceEntityId(),
                instance.getTaskType(),
                instance.getUnifiedCompletionRequest(),
                InferenceContext.EMPTY_INSTANCE,
                instance.getTimeout()
            );
        }

        return instance;
    }

    @Override
    protected Writeable.Reader<UnifiedCompletionAction.Request> instanceReader() {
        return UnifiedCompletionAction.Request::new;
    }

    @Override
    protected UnifiedCompletionAction.Request createTestInstance() {
        return new UnifiedCompletionAction.Request(
            randomAlphaOfLength(10),
            randomFrom(TaskType.values()),
            UnifiedCompletionRequestTests.randomUnifiedCompletionRequest(),
            InferenceContext.EMPTY_INSTANCE,
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );
    }

    @Override
    protected UnifiedCompletionAction.Request mutateInstance(UnifiedCompletionAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequest.getNamedWriteables());
    }
}
