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
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.inference.completion.Template;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.inference.completion.UnifiedCompletionUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_TEMPLATE_ADDED;
import static org.elasticsearch.test.BWCVersions.DEFAULT_BWC_VERSIONS;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;
import static org.hamcrest.Matchers.is;

public class UnifiedCompletionActionRequestTests extends AbstractBWCWireSerializationTestCase<UnifiedCompletionAction.Request> {

    private static final TransportVersion INFERENCE_CONTEXT = TransportVersion.fromName("inference_context");

    public void testConstructor_WithNullTimeout_UsesPlaceholder() {
        var request = new UnifiedCompletionAction.Request(
            randomAlphaOfLength(8),
            TaskType.COMPLETION,
            UnifiedCompletionRequest.streaming(UnifiedCompletionRequestBody.of(null)),
            null
        );
        assertThat(request.getTimeout(), is(TIMEOUT_NOT_DETERMINED));
    }

    public void testConstructor_WithNonNullTimeout_UsesTimeout() {
        var timeout = randomTimeValue();
        var request = new UnifiedCompletionAction.Request(
            randomAlphaOfLength(8),
            TaskType.COMPLETION,
            UnifiedCompletionRequest.streaming(UnifiedCompletionRequestBody.of(null)),
            timeout
        );
        assertThat(request.getTimeout(), is(timeout));
    }

    public void testValidation_ReturnsException_When_UnifiedCompletionRequestMessage_Is_Null() {
        var request = new UnifiedCompletionAction.Request(
            "inference_id",
            TaskType.COMPLETION,
            UnifiedCompletionRequest.streaming(UnifiedCompletionRequestBody.of(null)),
            TimeValue.timeValueSeconds(10)
        );
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Field [messages] cannot be null;"));
    }

    public void testValidation_ReturnsException_When_MessagesAndTemplate_AreBothPresent() {
        var request = templateRequest(
            List.of(UnifiedCompletionRequestBodyTests.randomMessage()),
            new Template(".summarize", 1, Map.of()),
            null,
            null
        );
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Fields [messages] and [template] are mutually exclusive;"));
    }

    public void testValidation_ReturnsException_When_TemplateId_IsBlank() {
        var request = templateRequest(null, new Template(" ", 1, Map.of()), null, null);
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Field [template.id] cannot be empty;"));
    }

    public void testValidation_ReturnsException_When_TemplateId_DoesNotStartWithDot() {
        var request = templateRequest(null, new Template("summarize", 1, Map.of()), null, null);
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Field [template.id] must start with a dot ([.]);"));
    }

    public void testValidation_ReturnsException_When_TemplateVersion_IsNotPositive() {
        var request = templateRequest(null, new Template(".summarize", randomIntBetween(-10, 0), Map.of()), null, null);
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Field [template.version] must be a positive integer;"));
    }

    public void testValidation_ReturnsException_When_Template_IsCombinedWithTools() {
        var request = templateRequest(
            null,
            new Template(".summarize", 1, Map.of()),
            List.of(new Tool("function", new Tool.FunctionField("description", "name", null, null))),
            null
        );
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Field [tools] cannot be specified with [template];"));
    }

    public void testValidation_ReturnsException_When_Template_IsCombinedWithToolChoice() {
        var request = templateRequest(null, new Template(".summarize", 1, Map.of()), null, new ToolChoice.ToolChoiceString("auto"));
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Field [tool_choice] cannot be specified with [template];"));
    }

    public void testValidation_ReturnsNull_When_TemplateIsValid() {
        var body = new UnifiedCompletionRequestBody(
            null,
            "model",
            10L,
            List.of("stop"),
            0.5f,
            null,
            null,
            0.9f,
            new Reasoning(Reasoning.ReasoningEffort.HIGH, null, null, null),
            null,
            "session",
            new Template(".summarize", 1, Map.of("time_range", "24h"))
        );
        var request = new UnifiedCompletionAction.Request(
            "inference_id",
            TaskType.CHAT_COMPLETION,
            UnifiedCompletionRequest.streaming(body),
            TimeValue.timeValueSeconds(10)
        );
        assertNull(request.validate());
    }

    private static UnifiedCompletionAction.Request templateRequest(
        List<Message> messages,
        Template template,
        List<Tool> tools,
        ToolChoice toolChoice
    ) {
        var body = new UnifiedCompletionRequestBody(messages, null, null, null, null, toolChoice, tools, null, null, null, null, template);
        return new UnifiedCompletionAction.Request(
            "inference_id",
            TaskType.CHAT_COMPLETION,
            UnifiedCompletionRequest.streaming(body),
            TimeValue.timeValueSeconds(10)
        );
    }

    public void testValidation_ReturnsException_When_UnifiedCompletionRequest_Is_EmptyArray() {
        var request = new UnifiedCompletionAction.Request(
            "inference_id",
            TaskType.COMPLETION,
            UnifiedCompletionRequest.streaming(UnifiedCompletionRequestBody.of(List.of())),
            TimeValue.timeValueSeconds(10)
        );
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Field [messages] cannot be an empty array;"));
    }

    public void testValidation_ReturnsException_When_TaskType_IsNot_Completion() {
        var request = new UnifiedCompletionAction.Request(
            "inference_id",
            TaskType.SPARSE_EMBEDDING,
            UnifiedCompletionRequest.streaming(UnifiedCompletionRequestBody.of(List.of(UnifiedCompletionRequestBodyTests.randomMessage()))),
            TimeValue.timeValueSeconds(10)
        );
        var exception = request.validate();
        assertThat(exception.getMessage(), is("Validation Failed: 1: Field [taskType] must be [chat_completion];"));
    }

    public void testValidation_ReturnsNull_When_TaskType_IsAny() {
        var request = new UnifiedCompletionAction.Request(
            "inference_id",
            TaskType.ANY,
            UnifiedCompletionRequest.streaming(UnifiedCompletionRequestBody.of(List.of(UnifiedCompletionRequestBodyTests.randomMessage()))),
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

        var timeout = instance.getTimeout();
        if (version.supports(INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED) == false) {
            if (timeout.equals(TIMEOUT_NOT_DETERMINED)) {
                timeout = BaseInferenceActionRequest.OLD_DEFAULT_TIMEOUT;
            }
        }

        return new UnifiedCompletionAction.Request(
            instance.getInferenceEntityId(),
            instance.getTaskType(),
            new UnifiedCompletionRequest(
                UnifiedCompletionRequestBodyTests.mutateInstanceForTransportVersion(instance.getUnifiedCompletionRequest().body(), version),
                instance.isStreaming()
            ),
            context,
            timeout
        );
    }

    /**
     * Versions before {@link UnifiedCompletionUtils#MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED} refuse to serialize non-text content
     * and versions before {@link UnifiedCompletionUtils#CHAT_COMPLETION_TEMPLATE_ADDED} refuse to serialize a template, so the
     * generic BWC test only covers versions that accept every random instance. Older versions are covered by
     * {@link #testBwcSerialization_VersionsBeforeTemplates}.
     */
    @Override
    protected Collection<TransportVersion> bwcVersions() {
        return super.bwcVersions().stream().filter(version -> version.supports(CHAT_COMPLETION_TEMPLATE_ADDED)).toList();
    }

    /**
     * The template and multimodal refusals are asserted by
     * {@link UnifiedCompletionRequestBodyTests#testUnsupportedFieldsAreNotBackwardsCompatible}; they only pass through this request
     * unchanged. This loop exists for the instances older versions do accept: it is the only run of the context and timeout
     * down-casts in {@link #mutateInstanceForVersion} against versions that {@link #bwcVersions()} leaves out.
     */
    public void testBwcSerialization_VersionsBeforeTemplates() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            var instance = createTestInstance();
            for (var version : DEFAULT_BWC_VERSIONS) {
                var expectedError = UnifiedCompletionRequestBodyTests.expectedSerializationError(
                    instance.getUnifiedCompletionRequest().body(),
                    version
                );
                if (expectedError == null) {
                    assertBwcSerialization(instance, version);
                } else {
                    var e = expectThrows(
                        ElasticsearchStatusException.class,
                        () -> copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), version)
                    );
                    assertThat(e.status(), is(RestStatus.BAD_REQUEST));
                    assertThat(e.getMessage(), is(expectedError));
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
            UnifiedCompletionRequestTests.createRandom(),
            new InferenceContext(randomAlphaOfLength(10)),
            randomFrom(randomTimeValue(), null)
        );
    }

    @Override
    protected UnifiedCompletionAction.Request mutateInstance(UnifiedCompletionAction.Request instance) throws IOException {
        var inferenceEntityId = instance.getInferenceEntityId();
        var taskType = instance.getTaskType();
        var unifiedCompletionRequest = instance.getUnifiedCompletionRequest();
        var inferenceContext = instance.getContext();
        var timeout = instance.getTimeout();
        switch (between(0, 4)) {
            case 0 -> inferenceEntityId = randomValueOtherThan(inferenceEntityId, () -> randomAlphaOfLength(10));
            case 1 -> taskType = randomValueOtherThan(taskType, () -> randomFrom(TaskType.values()));
            case 2 -> unifiedCompletionRequest = randomValueOtherThan(
                unifiedCompletionRequest,
                UnifiedCompletionRequestTests::createRandom
            );
            case 3 -> inferenceContext = randomValueOtherThan(inferenceContext, () -> new InferenceContext(randomAlphaOfLength(10)));
            case 4 -> {
                if (timeout.equals(TIMEOUT_NOT_DETERMINED)) {
                    // Using null as timeout will translate it internally to TIMEOUT_NOT_DETERMINED, which would not mutate the instance
                    timeout = randomValueOtherThan(timeout, ESTestCase::randomTimeValue);
                } else {
                    timeout = randomValueOtherThan(timeout, () -> randomFrom(randomTimeValue(), null));
                }
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new UnifiedCompletionAction.Request(inferenceEntityId, taskType, unifiedCompletionRequest, inferenceContext, timeout);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequestBody.getNamedWriteables());
    }
}
