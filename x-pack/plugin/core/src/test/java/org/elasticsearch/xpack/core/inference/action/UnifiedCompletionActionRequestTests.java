/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.inference.UnifiedCompletionRequest.MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED;
import static org.elasticsearch.test.BWCVersions.DEFAULT_BWC_VERSIONS;
import static org.hamcrest.Matchers.is;

public class UnifiedCompletionActionRequestTests extends AbstractBWCWireSerializationTestCase<UnifiedCompletionAction.Request> {

    private static final TransportVersion INFERENCE_CONTEXT = TransportVersion.fromName("inference_context");

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

    @Override
    protected UnifiedCompletionAction.Request mutateInstanceForVersion(UnifiedCompletionAction.Request instance, TransportVersion version) {
        InferenceContext context = instance.getContext();
        if (version.supports(INFERENCE_CONTEXT) == false) {
            context = InferenceContext.EMPTY_INSTANCE;
        }

        return new UnifiedCompletionAction.Request(
            instance.getInferenceEntityId(),
            instance.getTaskType(),
            instance.getUnifiedCompletionRequest(),
            context,
            instance.getTimeout()
        );
    }

    // Versions before MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED throw an exception when serializing non-text content
    // Those are tested in testMultimodalContentIsNotBackwardsCompatible
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED)).toList();
    }

    public void testMultimodalContentIsNotBackwardsCompatible() throws IOException {
        var unsupportedVersions = DEFAULT_BWC_VERSIONS.stream()
            .filter(Predicate.not(version -> version.supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED)))
            .toList();
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            var testInstance = createTestInstance();
            for (var unsupportedVersion : unsupportedVersions) {
                if (testInstance.getUnifiedCompletionRequest().containsMultimodalContent()) {
                    var statusException = assertThrows(
                        ElasticsearchStatusException.class,
                        () -> copyWriteable(testInstance, getNamedWriteableRegistry(), instanceReader(), unsupportedVersion)
                    );
                    assertThat(statusException.status(), is(RestStatus.BAD_REQUEST));
                    assertThat(
                        statusException.getMessage(),
                        is(
                            "Cannot send a multimodal chat completion request to an older node. "
                                + "Please wait until all nodes are upgraded before using multimodal chat completion inputs"
                        )
                    );
                } else {
                    // If the instance doesn't contain multimodal content, assert that it can still be serialized
                    assertBwcSerialization(testInstance, unsupportedVersion);
                }
            }
        }
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
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );
    }

    @Override
    protected UnifiedCompletionAction.Request mutateInstance(UnifiedCompletionAction.Request instance) throws IOException {
        String inferenceEntityId = instance.getInferenceEntityId();
        TaskType taskType = instance.getTaskType();
        UnifiedCompletionRequest unifiedCompletionRequest = instance.getUnifiedCompletionRequest();
        InferenceContext inferenceContext = instance.getContext();
        TimeValue timeout = instance.getTimeout();
        switch (between(0, 4)) {
            case 0 -> inferenceEntityId = randomValueOtherThan(inferenceEntityId, () -> randomAlphaOfLength(10));
            case 1 -> taskType = randomValueOtherThan(taskType, () -> randomFrom(TaskType.values()));
            case 2 -> unifiedCompletionRequest = randomValueOtherThan(
                unifiedCompletionRequest,
                UnifiedCompletionRequestTests::randomUnifiedCompletionRequest
            );
            case 3 -> inferenceContext = randomValueOtherThan(inferenceContext, () -> new InferenceContext(randomAlphaOfLength(10)));
            case 4 -> timeout = randomValueOtherThan(timeout, () -> TimeValue.timeValueMillis(randomLongBetween(1, 2048)));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new UnifiedCompletionAction.Request(inferenceEntityId, taskType, unifiedCompletionRequest, inferenceContext, timeout);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequest.getNamedWriteables());
    }
}
