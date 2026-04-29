/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.EmbeddingRequest.JINA_AI_EMBEDDING_TASK_ADDED;
import static org.elasticsearch.inference.InferenceStringTests.TEST_DATA_URI;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;
import static org.elasticsearch.xpack.core.inference.action.EmbeddingAction.Request.parseRequest;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class EmbeddingActionRequestTests extends AbstractBWCWireSerializationTestCase<EmbeddingAction.Request> {
    private static final TransportVersion INFERENCE_CONTEXT = TransportVersion.fromName("inference_context");

    public void testConstructor_WithNullTimeout_UsesPlaceholder() {
        var request = new EmbeddingAction.Request(
            randomAlphanumericOfLength(8),
            randomFrom(TaskType.values()),
            randomEmbeddingRequest(),
            null
        );
        assertThat(request.getTimeout(), is(TIMEOUT_NOT_DETERMINED));
    }

    public void testConstructor_WithNonNullTimeout_UsesTimeout() {
        var timeout = randomTimeValue();
        var request = new EmbeddingAction.Request(
            randomAlphanumericOfLength(8),
            randomFrom(TaskType.values()),
            randomEmbeddingRequest(),
            timeout
        );
        assertThat(request.getTimeout(), is(timeout));
    }

    public void testParseRequest() throws IOException {
        var requestJson = Strings.format("""
            {
                "input": [
                    {
                        "content": {"type": "image", "format": "base64", "value": "%s" }
                    }
                ],
                "input_type": "search",
                "task_settings": {
                  "field": "value"
                }
            }
            """, TEST_DATA_URI);
        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var inferenceId = randomAlphanumericOfLength(8);
            var taskType = randomFrom(TaskType.values());
            var context = new InferenceContext(randomAlphaOfLength(10));
            var timeout = TimeValue.timeValueMillis(randomLongBetween(1, 2048));

            var expectedRequest = new EmbeddingAction.Request(
                inferenceId,
                taskType,
                new EmbeddingRequest(
                    List.of(new InferenceStringGroup(new InferenceString(DataType.IMAGE, DataFormat.BASE64, TEST_DATA_URI))),
                    InputType.SEARCH,
                    Map.of("field", "value")
                ),
                context,
                timeout
            );

            var parsedRequest = parseRequest(inferenceId, taskType, timeout, context, parser);

            assertThat(parsedRequest, is(expectedRequest));
        }
    }

    public void testIsStreaming_returnsFalse() {
        assertThat(createRandom().isStreaming(), is(false));
    }

    public void testValidate_withNullEmbeddingRequestInputs_returnsValidationException() {
        var request = new EmbeddingAction.Request(
            randomAlphanumericOfLength(8),
            TaskType.EMBEDDING,
            new EmbeddingRequest(null, randomFrom(InputType.values()), Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().getFirst(), is("Field [inputs] cannot be null"));
    }

    public void testValidate_withEmptyEmbeddingRequestInputs_returnsValidationException() {
        var request = new EmbeddingAction.Request(
            randomAlphanumericOfLength(8),
            TaskType.EMBEDDING,
            new EmbeddingRequest(List.of(), randomFrom(InputType.values()), Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().getFirst(), is("Field [inputs] cannot be an empty array"));
    }

    public void testValidate_withNonEmbeddingTaskType_returnsValidationException() {
        var request = new EmbeddingAction.Request(
            randomAlphanumericOfLength(8),
            randomValueOtherThanMany(TaskType.EMBEDDING::isAnyOrSame, () -> randomFrom(TaskType.values())),
            randomEmbeddingRequest(),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.validationErrors().getFirst(), is("Field [taskType] must be [embedding]"));
    }

    public void testValidate_withMultipleValidationErrors_returnsAll() {
        // Create a request with an invalid task type and null inputs
        var request = new EmbeddingAction.Request(
            randomAlphanumericOfLength(8),
            randomValueOtherThanMany(TaskType.EMBEDDING::isAnyOrSame, () -> randomFrom(TaskType.values())),
            new EmbeddingRequest(null, randomFrom(InputType.values()), Map.of()),
            new InferenceContext(randomAlphaOfLength(10)),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048))
        );

        var validationException = request.validate();
        assertThat(validationException.validationErrors(), hasSize(2));
        assertThat(validationException.validationErrors().getFirst(), is("Field [inputs] cannot be null"));
        assertThat(validationException.validationErrors().getLast(), is("Field [taskType] must be [embedding]"));
    }

    @Override
    protected EmbeddingAction.Request mutateInstanceForVersion(EmbeddingAction.Request instance, TransportVersion version) {
        // Use empty task settings if node is on a version before Jina AI embedding task support was added, since embedding request task
        // settings were added with that change
        var embeddingRequest = instance.getEmbeddingRequest();
        if (version.supports(JINA_AI_EMBEDDING_TASK_ADDED) == false) {
            embeddingRequest = new EmbeddingRequest(embeddingRequest.inputs(), embeddingRequest.inputType(), Map.of());
        }

        var context = instance.getContext();
        if (version.supports(INFERENCE_CONTEXT) == false) {
            context = InferenceContext.EMPTY_INSTANCE;
        }

        var timeout = instance.getTimeout();
        if (version.supports(INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED) == false) {
            if (timeout.equals(TIMEOUT_NOT_DETERMINED)) {
                timeout = BaseInferenceActionRequest.OLD_DEFAULT_TIMEOUT;
            }
        }

        return new EmbeddingAction.Request(instance.getInferenceEntityId(), instance.getTaskType(), embeddingRequest, context, timeout);
    }

    @Override
    protected Writeable.Reader<EmbeddingAction.Request> instanceReader() {
        return EmbeddingAction.Request::new;
    }

    @Override
    protected EmbeddingAction.Request createTestInstance() {
        return createRandom();
    }

    public static EmbeddingAction.Request createRandom() {
        var inferenceId = randomAlphanumericOfLength(8);
        var taskType = randomFrom(TaskType.values());
        var embeddingRequest = randomEmbeddingRequest();
        var context = new InferenceContext(randomAlphaOfLength(10));
        var timeout = randomFrom(randomTimeValue(), null);
        return new EmbeddingAction.Request(inferenceId, taskType, embeddingRequest, context, timeout);
    }

    private static EmbeddingRequest randomEmbeddingRequest() {
        return new EmbeddingRequest(
            List.of(new InferenceStringGroup(randomAlphanumericOfLength(8))),
            randomFrom(InputType.values()),
            Map.of(randomAlphanumericOfLength(8), randomAlphanumericOfLength(8))
        );
    }

    @Override
    protected EmbeddingAction.Request mutateInstance(EmbeddingAction.Request instance) throws IOException {
        var inferenceId = instance.getInferenceEntityId();
        var taskType = instance.getTaskType();
        var embeddingRequest = instance.getEmbeddingRequest();
        var context = instance.getContext();
        var timeout = instance.getTimeout();
        switch (between(0, 4)) {
            case 0 -> inferenceId = randomValueOtherThan(inferenceId, () -> randomAlphaOfLength(8));
            case 1 -> taskType = randomValueOtherThan(taskType, () -> randomFrom(TaskType.values()));
            case 2 -> embeddingRequest = randomValueOtherThan(embeddingRequest, EmbeddingActionRequestTests::randomEmbeddingRequest);
            case 3 -> context = randomValueOtherThan(context, () -> new InferenceContext(randomAlphaOfLength(10)));
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
        return new EmbeddingAction.Request(inferenceId, taskType, embeddingRequest, context, timeout);
    }
}
