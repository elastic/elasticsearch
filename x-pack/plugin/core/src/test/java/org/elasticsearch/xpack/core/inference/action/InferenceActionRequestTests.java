/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.inference.InferenceContextTests;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED;
import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;
import static org.elasticsearch.xpack.core.inference.action.InferenceAction.Request.SUPPORTED_INFERENCE_ACTION_TASK_TYPES;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class InferenceActionRequestTests extends AbstractBWCWireSerializationTestCase<InferenceAction.Request> {

    private static final TransportVersion INFERENCE_CONTEXT = TransportVersion.fromName("inference_context");

    private static final String TEST_INFERENCE_ENDPOINT = "test_endpoint";
    private static final List<String> TEST_INPUT = List.of("test input");

    @Override
    protected Writeable.Reader<InferenceAction.Request> instanceReader() {
        return InferenceAction.Request::new;
    }

    @Override
    protected InferenceAction.Request createTestInstance() {
        return new InferenceAction.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomList(1, 5, () -> randomAlphaOfLength(8)),
            randomMap(0, 3, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4))),
            randomFrom(InputType.values()),
            randomFrom(TimeValue.timeValueMillis(randomLongBetween(1, 2048)), null),
            false,
            InferenceContextTests.createRandom()
        );
    }

    public void testConstructor_WithNullTimeout_UsesPlaceholder() {
        var request = new InferenceAction.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomList(1, 5, () -> randomAlphaOfLength(8)),
            randomMap(0, 3, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4))),
            randomFrom(InputType.values()),
            null,
            false,
            InferenceContextTests.createRandom()
        );
        assertThat(request.getInferenceTimeout(), is(TIMEOUT_NOT_DETERMINED));
    }

    public void testConstructor_WithNonNullTimeout_UsesTimeout() {
        TimeValue inferenceTimeout = randomTimeValue();
        var request = new InferenceAction.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomList(1, 5, () -> randomAlphaOfLength(8)),
            randomMap(0, 3, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4))),
            randomFrom(InputType.values()),
            inferenceTimeout,
            false,
            InferenceContextTests.createRandom()
        );
        assertThat(request.getInferenceTimeout(), is(inferenceTimeout));
    }

    public void testParsing() throws IOException {
        String singleInputRequest = """
            {
              "input": "single text input"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, singleInputRequest)) {
            var request = InferenceAction.Request.parseRequest(
                "model_id",
                TaskType.SPARSE_EMBEDDING,
                InferenceContext.EMPTY_INSTANCE,
                parser
            ).build();
            assertThat(request.getInput(), contains("single text input"));
        }

        String multiInputRequest = """
            {
              "input": ["an array", "of", "inputs"]
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, multiInputRequest)) {
            var request = InferenceAction.Request.parseRequest("model_id", TaskType.ANY, InferenceContext.EMPTY_INSTANCE, parser).build();
            assertThat(request.getInput(), contains("an array", "of", "inputs"));
        }
    }

    public void testValidation_TextEmbedding() {
        InferenceAction.Request request = new InferenceAction.Request(
            TaskType.TEXT_EMBEDDING,
            "model",
            TEST_INPUT,
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException e = request.validate();
        assertNull(e);
    }

    public void testValidation_TextEmbedding_Null() {
        InferenceAction.Request inputNullRequest = new InferenceAction.Request(
            TaskType.TEXT_EMBEDDING,
            "model",
            null,
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException inputNullError = inputNullRequest.validate();
        assertNotNull(inputNullError);
        assertThat(inputNullError.getMessage(), is("Validation Failed: 1: Field [input] cannot be null;"));
    }

    public void testValidation_TextEmbedding_Empty() {
        InferenceAction.Request inputEmptyRequest = new InferenceAction.Request(
            TaskType.TEXT_EMBEDDING,
            "model",
            List.of(),
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException inputEmptyError = inputEmptyRequest.validate();
        assertNotNull(inputEmptyError);
        assertThat(inputEmptyError.getMessage(), is("Validation Failed: 1: Field [input] cannot be an empty array;"));
    }

    public void testValidation_UnsupportedTaskTypes() {
        EnumSet.complementOf(SUPPORTED_INFERENCE_ACTION_TASK_TYPES).forEach(unsupportedTaskType -> {
            var unsupportedTaskTypeRequest = new InferenceAction.Request(unsupportedTaskType, "model", TEST_INPUT, null, null, null, false);
            var unsupportedTaskTypeError = unsupportedTaskTypeRequest.validate();
            assertNotNull(unsupportedTaskTypeError);
            assertThat(
                unsupportedTaskTypeError.getMessage(),
                is(
                    Strings.format(
                        "Validation Failed: 1: Task type [%s] cannot be used with InferenceAction.Request. Supported task types are %s;",
                        unsupportedTaskType,
                        SUPPORTED_INFERENCE_ACTION_TASK_TYPES
                    )
                )
            );
        });
    }

    public void testValidation_SparseEmbedding_WithInputType() {
        InferenceAction.Request queryRequest = new InferenceAction.Request(
            TaskType.SPARSE_EMBEDDING,
            "model",
            TEST_INPUT,
            null,
            InputType.SEARCH,
            null,
            false
        );
        ActionRequestValidationException queryError = queryRequest.validate();
        assertNotNull(queryError);
        assertThat(
            queryError.getMessage(),
            is("Validation Failed: 1: Field [input_type] cannot be specified for task type [sparse_embedding];")
        );
    }

    public void testValidation_Completion_WithInputType() {
        InferenceAction.Request queryRequest = new InferenceAction.Request(
            TaskType.COMPLETION,
            "model",
            TEST_INPUT,
            null,
            InputType.SEARCH,
            null,
            false
        );
        ActionRequestValidationException queryError = queryRequest.validate();
        assertNotNull(queryError);
        assertThat(queryError.getMessage(), is("Validation Failed: 1: Field [input_type] cannot be specified for task type [completion];"));
    }

    public void testBuilder_DefaultContextIsEmptyInstance() {
        var request = InferenceAction.Request.builder(TEST_INFERENCE_ENDPOINT, TaskType.TEXT_EMBEDDING).setInput(TEST_INPUT).build();
        assertThat(request.getContext(), sameInstance(InferenceContext.EMPTY_INSTANCE));
    }

    public void testBuilder_SetContextNull_UsesEmptyContext() {
        var request = InferenceAction.Request.builder(TEST_INFERENCE_ENDPOINT, TaskType.TEXT_EMBEDDING).setContext(null).build();
        assertThat(request.getContext(), sameInstance(InferenceContext.EMPTY_INSTANCE));
    }

    public void testConstructor_NullContext_UsesEmptyContext() {
        var request = new InferenceAction.Request(
            TaskType.TEXT_EMBEDDING,
            TEST_INFERENCE_ENDPOINT,
            TEST_INPUT,
            null,
            null,
            null,
            false,
            null
        );

        assertThat(request.getContext(), sameInstance(InferenceContext.EMPTY_INSTANCE));
    }

    public void testParseRequest_DefaultsInputTypeToIngest() throws IOException {
        String singleInputRequest = """
            {
              "input": "single text input"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, singleInputRequest)) {
            var request = InferenceAction.Request.parseRequest(
                "model_id",
                TaskType.SPARSE_EMBEDDING,
                InferenceContext.EMPTY_INSTANCE,
                parser
            ).build();
            assertThat(request.getInputType(), is(InputType.UNSPECIFIED));
        }
    }

    @Override
    protected InferenceAction.Request mutateInstance(InferenceAction.Request instance) throws IOException {
        var taskType = instance.getTaskType();
        var inferenceEntityId = instance.getInferenceEntityId();
        var input = instance.getInput();
        var taskSettings = instance.getTaskSettings();
        var inputType = instance.getInputType();
        var inferenceTimeout = instance.getInferenceTimeout();
        var context = instance.getContext();

        switch (randomIntBetween(0, 6)) {
            case 0 -> taskType = randomValueOtherThan(taskType, () -> randomFrom(TaskType.values()));
            case 1 -> inferenceEntityId = randomValueOtherThan(inferenceEntityId, () -> randomAlphaOfLength(6));
            case 2 -> input = randomValueOtherThan(input, () -> randomList(1, 5, () -> randomAlphaOfLength(8)));
            case 3 -> taskSettings = randomValueOtherThan(
                taskSettings,
                () -> randomMap(0, 3, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4)))
            );
            case 4 -> inputType = randomValueOtherThan(inputType, () -> randomFrom(InputType.values()));
            case 5 -> {
                if (inferenceTimeout.equals(TIMEOUT_NOT_DETERMINED)) {
                    // Using null as timeout will translate it internally to TIMEOUT_NOT_DETERMINED, which would not mutate the instance
                    inferenceTimeout = randomValueOtherThan(inferenceTimeout, ESTestCase::randomTimeValue);
                } else {
                    inferenceTimeout = randomValueOtherThan(inferenceTimeout, () -> randomFrom(randomTimeValue(), null));
                }
            }
            case 6 -> context = randomValueOtherThan(context, InferenceContextTests::createRandom);
            default -> throw new UnsupportedOperationException();

        }

        return new InferenceAction.Request(taskType, inferenceEntityId, input, taskSettings, inputType, inferenceTimeout, false, context);
    }

    @Override
    protected InferenceAction.Request mutateInstanceForVersion(InferenceAction.Request instance, TransportVersion version) {
        var context = instance.getContext();
        var inferenceTimeout = instance.getInferenceTimeout();

        if (version.supports(INFERENCE_CONTEXT) == false) {
            context = InferenceContext.EMPTY_INSTANCE;
        }
        if (version.supports(INFERENCE_REQUEST_PER_TASK_TIMEOUT_ADDED) == false) {
            if (inferenceTimeout.equals(TIMEOUT_NOT_DETERMINED)) {
                inferenceTimeout = BaseInferenceActionRequest.OLD_DEFAULT_TIMEOUT;
            }
        }

        return new InferenceAction.Request(
            instance.getTaskType(),
            instance.getInferenceEntityId(),
            instance.getInput(),
            instance.getTaskSettings(),
            instance.getInputType(),
            inferenceTimeout,
            false,
            context
        );
    }
}
