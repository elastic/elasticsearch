/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.action.InferenceAction.Request.getInputTypeToWrite;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class InferenceActionRequestTests extends AbstractBWCWireSerializationTestCase<InferenceAction.Request> {

    @Override
    protected Writeable.Reader<InferenceAction.Request> instanceReader() {
        return InferenceAction.Request::new;
    }

    @Override
    protected InferenceAction.Request createTestInstance() {
        return new InferenceAction.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomAlphaOfLengthOrNull(10),
            randomBoolean(),
            randomIntBetween(0, 10),
            randomList(1, 5, () -> randomAlphaOfLength(8)),
            randomMap(0, 3, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4))),
            randomFrom(InputType.values()),
            TimeValue.timeValueMillis(randomLongBetween(1, 2048)),
            false,
            new InferenceContext(randomAlphanumericOfLength(10))
        );
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
            null,
            null,
            null,
            List.of("input"),
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException e = request.validate();
        assertNull(e);
    }

    public void testValidation_Rerank() {
        InferenceAction.Request request = new InferenceAction.Request(
            TaskType.RERANK,
            "model",
            "query",
            Boolean.TRUE,
            34,
            List.of("input"),
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
            null,
            null,
            null,
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

    public void testValidation_TextEmbedding_WithReturnDocument() {
        InferenceAction.Request inputRequest = new InferenceAction.Request(
            TaskType.TEXT_EMBEDDING,
            "model",
            null,
            Boolean.TRUE,
            null,
            List.of("input"),
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException inputError = inputRequest.validate();
        assertNotNull(inputError);
        assertThat(
            inputError.getMessage(),
            is("Validation Failed: 1: Field [return_documents] cannot be specified for task type [text_embedding];")
        );
    }

    public void testValidation_TextEmbedding_WithTopN() {
        InferenceAction.Request inputRequest = new InferenceAction.Request(
            TaskType.TEXT_EMBEDDING,
            "model",
            null,
            null,
            12,
            List.of("input"),
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException inputError = inputRequest.validate();
        assertNotNull(inputError);
        assertThat(inputError.getMessage(), is("Validation Failed: 1: Field [top_n] cannot be specified for task type [text_embedding];"));
    }

    public void testValidation_Rerank_Null() {
        InferenceAction.Request queryNullRequest = new InferenceAction.Request(
            TaskType.RERANK,
            "model",
            null,
            null,
            null,
            List.of("input"),
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException queryNullError = queryNullRequest.validate();
        assertNotNull(queryNullError);
        assertThat(queryNullError.getMessage(), is("Validation Failed: 1: Field [query] cannot be null for task type [rerank];"));
    }

    public void testValidation_Rerank_Empty() {
        InferenceAction.Request queryEmptyRequest = new InferenceAction.Request(
            TaskType.RERANK,
            "model",
            "",
            null,
            null,
            List.of("input"),
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException queryEmptyError = queryEmptyRequest.validate();
        assertNotNull(queryEmptyError);
        assertThat(queryEmptyError.getMessage(), is("Validation Failed: 1: Field [query] cannot be empty for task type [rerank];"));
    }

    public void testValidation_Rerank_WithInputType() {
        InferenceAction.Request request = new InferenceAction.Request(
            TaskType.RERANK,
            "model",
            "query",
            null,
            null,
            List.of("input"),
            null,
            InputType.SEARCH,
            null,
            false
        );
        ActionRequestValidationException queryError = request.validate();
        assertNotNull(queryError);
        assertThat(queryError.getMessage(), is("Validation Failed: 1: Field [input_type] cannot be specified for task type [rerank];"));
    }

    public void testValidation_SparseEmbedding_WithInputType() {
        InferenceAction.Request queryRequest = new InferenceAction.Request(
            TaskType.SPARSE_EMBEDDING,
            "model",
            "",
            null,
            null,
            List.of("input"),
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

    public void testValidation_SparseEmbedding_WithReturnDocument() {
        InferenceAction.Request queryRequest = new InferenceAction.Request(
            TaskType.SPARSE_EMBEDDING,
            "model",
            "",
            Boolean.FALSE,
            null,
            List.of("input"),
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException queryError = queryRequest.validate();
        assertNotNull(queryError);
        assertThat(
            queryError.getMessage(),
            is("Validation Failed: 1: Field [return_documents] cannot be specified for task type [sparse_embedding];")
        );

    }

    public void testValidation_SparseEmbedding_WithTopN() {
        InferenceAction.Request queryRequest = new InferenceAction.Request(
            TaskType.SPARSE_EMBEDDING,
            "model",
            "",
            null,
            22,
            List.of("input"),
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException queryError = queryRequest.validate();
        assertNotNull(queryError);
        assertThat(
            queryError.getMessage(),
            is("Validation Failed: 1: Field [top_n] cannot be specified for task type [sparse_embedding];")
        );
    }

    public void testValidation_Completion_WithInputType() {
        InferenceAction.Request queryRequest = new InferenceAction.Request(
            TaskType.COMPLETION,
            "model",
            "",
            null,
            null,
            List.of("input"),
            null,
            InputType.SEARCH,
            null,
            false
        );
        ActionRequestValidationException queryError = queryRequest.validate();
        assertNotNull(queryError);
        assertThat(queryError.getMessage(), is("Validation Failed: 1: Field [input_type] cannot be specified for task type [completion];"));
    }

    public void testValidation_Completion_WithReturnDocuments() {
        InferenceAction.Request queryRequest = new InferenceAction.Request(
            TaskType.COMPLETION,
            "model",
            "",
            Boolean.TRUE,
            null,
            List.of("input"),
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException queryError = queryRequest.validate();
        assertNotNull(queryError);
        assertThat(
            queryError.getMessage(),
            is("Validation Failed: 1: Field [return_documents] cannot be specified for task type [completion];")
        );
    }

    public void testValidation_Completion_WithTopN() {
        InferenceAction.Request queryRequest = new InferenceAction.Request(
            TaskType.COMPLETION,
            "model",
            "",
            null,
            77,
            List.of("input"),
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException queryError = queryRequest.validate();
        assertNotNull(queryError);
        assertThat(queryError.getMessage(), is("Validation Failed: 1: Field [top_n] cannot be specified for task type [completion];"));
    }

    public void testValidation_ChatCompletion_WithInputType() {
        InferenceAction.Request queryRequest = new InferenceAction.Request(
            TaskType.CHAT_COMPLETION,
            "model",
            "",
            null,
            null,
            List.of("input"),
            null,
            InputType.SEARCH,
            null,
            false
        );
        ActionRequestValidationException queryError = queryRequest.validate();
        assertNotNull(queryError);
        assertThat(
            queryError.getMessage(),
            is("Validation Failed: 1: Field [input_type] cannot be specified for task type [chat_completion];")
        );
    }

    public void testValidation_ChatCompletion_WithReturnDocuments() {
        InferenceAction.Request queryRequest = new InferenceAction.Request(
            TaskType.CHAT_COMPLETION,
            "model",
            "",
            Boolean.TRUE,
            null,
            List.of("input"),
            null,
            null,
            null,
            false
        );
        ActionRequestValidationException queryError = queryRequest.validate();
        assertNotNull(queryError);
        assertThat(
            queryError.getMessage(),
            is("Validation Failed: 1: Field [return_documents] cannot be specified for task type [chat_completion];")
        );
    }

    public void testValidation_ChatCompletion_WithTopN() {
        InferenceAction.Request queryRequest = new InferenceAction.Request(
            TaskType.CHAT_COMPLETION,
            "model",
            "",
            null,
            11,
            List.of("input"),
            null,
            InputType.SEARCH,
            null,
            false
        );
        ActionRequestValidationException queryError = queryRequest.validate();
        assertNotNull(queryError);
        assertThat(queryError.getMessage(), is("Validation Failed: 1: Field [top_n] cannot be specified for task type [chat_completion];"));
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
        int select = randomIntBetween(0, 7);
        return switch (select) {
            case 0 -> {
                var nextTask = TaskType.values()[(instance.getTaskType().ordinal() + 1) % TaskType.values().length];
                yield new InferenceAction.Request(
                    nextTask,
                    instance.getInferenceEntityId(),
                    instance.getQuery(),
                    instance.getReturnDocuments(),
                    instance.getTopN(),
                    instance.getInput(),
                    instance.getTaskSettings(),
                    instance.getInputType(),
                    instance.getInferenceTimeout(),
                    false,
                    instance.getContext()
                );
            }
            case 1 -> new InferenceAction.Request(
                instance.getTaskType(),
                instance.getInferenceEntityId() + "foo",
                instance.getQuery(),
                instance.getReturnDocuments(),
                instance.getTopN(),
                instance.getInput(),
                instance.getTaskSettings(),
                instance.getInputType(),
                instance.getInferenceTimeout(),
                false,
                instance.getContext()
            );
            case 2 -> {
                var changedInputs = new ArrayList<String>(instance.getInput());
                changedInputs.add("bar");
                yield new InferenceAction.Request(
                    instance.getTaskType(),
                    instance.getInferenceEntityId(),
                    instance.getQuery(),
                    instance.getReturnDocuments(),
                    instance.getTopN(),
                    changedInputs,
                    instance.getTaskSettings(),
                    instance.getInputType(),
                    instance.getInferenceTimeout(),
                    false,
                    instance.getContext()
                );
            }
            case 3 -> {
                var taskSettings = new HashMap<>(instance.getTaskSettings());
                if (taskSettings.isEmpty()) {
                    taskSettings.put("foo", "bar");
                } else {
                    var keyToRemove = taskSettings.keySet().iterator().next();
                    taskSettings.remove(keyToRemove);
                }
                yield new InferenceAction.Request(
                    instance.getTaskType(),
                    instance.getInferenceEntityId(),
                    instance.getQuery(),
                    instance.getReturnDocuments(),
                    instance.getTopN(),
                    instance.getInput(),
                    taskSettings,
                    instance.getInputType(),
                    instance.getInferenceTimeout(),
                    false,
                    instance.getContext()
                );
            }
            case 4 -> {
                var nextInputType = InputType.values()[(instance.getInputType().ordinal() + 1) % InputType.values().length];
                yield new InferenceAction.Request(
                    instance.getTaskType(),
                    instance.getInferenceEntityId(),
                    instance.getQuery(),
                    instance.getReturnDocuments(),
                    instance.getTopN(),
                    instance.getInput(),
                    instance.getTaskSettings(),
                    nextInputType,
                    instance.getInferenceTimeout(),
                    false,
                    instance.getContext()
                );
            }
            case 5 -> new InferenceAction.Request(
                instance.getTaskType(),
                instance.getInferenceEntityId(),
                instance.getQuery() == null ? randomAlphaOfLength(10) : instance.getQuery() + randomAlphaOfLength(1),
                instance.getReturnDocuments(),
                instance.getTopN(),
                instance.getInput(),
                instance.getTaskSettings(),
                instance.getInputType(),
                instance.getInferenceTimeout(),
                false,
                instance.getContext()
            );
            case 6 -> {
                var newDuration = Duration.of(
                    instance.getInferenceTimeout().duration(),
                    instance.getInferenceTimeout().timeUnit().toChronoUnit()
                );
                var additionalTime = Duration.ofMillis(randomLongBetween(1, 2048));
                yield new InferenceAction.Request(
                    instance.getTaskType(),
                    instance.getInferenceEntityId(),
                    instance.getQuery(),
                    instance.getReturnDocuments(),
                    instance.getTopN(),
                    instance.getInput(),
                    instance.getTaskSettings(),
                    instance.getInputType(),
                    TimeValue.timeValueMillis(newDuration.plus(additionalTime).toMillis()),
                    false,
                    instance.getContext()
                );
            }
            case 7 -> {
                var newContext = new InferenceContext(instance.getContext().productUseCase() + randomAlphaOfLength(5));
                yield new InferenceAction.Request(
                    instance.getTaskType(),
                    instance.getInferenceEntityId(),
                    instance.getQuery(),
                    instance.getReturnDocuments(),
                    instance.getTopN(),
                    instance.getInput(),
                    instance.getTaskSettings(),
                    instance.getInputType(),
                    instance.getInferenceTimeout(),
                    instance.isStreaming(),
                    newContext
                );
            }
            default -> throw new UnsupportedOperationException();
        };
    }

    @Override
    protected InferenceAction.Request mutateInstanceForVersion(InferenceAction.Request instance, TransportVersion version) {
        InferenceAction.Request mutated;

        if (version.before(TransportVersions.V_8_12_0)) {
            mutated = new InferenceAction.Request(
                instance.getTaskType(),
                instance.getInferenceEntityId(),
                null,
                null,
                null,
                instance.getInput().subList(0, 1),
                instance.getTaskSettings(),
                InputType.UNSPECIFIED,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                false
            );
        } else if (version.before(TransportVersions.V_8_13_0)) {
            mutated = new InferenceAction.Request(
                instance.getTaskType(),
                instance.getInferenceEntityId(),
                null,
                null,
                null,
                instance.getInput(),
                instance.getTaskSettings(),
                InputType.UNSPECIFIED,
                InferenceAction.Request.DEFAULT_TIMEOUT,
                false
            );
        } else if (version.before(TransportVersions.V_8_13_0)
            && (instance.getInputType() == InputType.UNSPECIFIED
                || instance.getInputType() == InputType.CLASSIFICATION
                || instance.getInputType() == InputType.CLUSTERING)) {
                    mutated = new InferenceAction.Request(
                        instance.getTaskType(),
                        instance.getInferenceEntityId(),
                        null,
                        null,
                        null,
                        instance.getInput(),
                        instance.getTaskSettings(),
                        InputType.INGEST,
                        InferenceAction.Request.DEFAULT_TIMEOUT,
                        false
                    );
                } else if (version.before(TransportVersions.V_8_13_0)
                    && (instance.getInputType() == InputType.CLUSTERING || instance.getInputType() == InputType.CLASSIFICATION)) {
                        mutated = new InferenceAction.Request(
                            instance.getTaskType(),
                            instance.getInferenceEntityId(),
                            null,
                            null,
                            null,
                            instance.getInput(),
                            instance.getTaskSettings(),
                            InputType.UNSPECIFIED,
                            InferenceAction.Request.DEFAULT_TIMEOUT,
                            false
                        );
                    } else if (version.before(TransportVersions.V_8_14_0)) {
                        mutated = new InferenceAction.Request(
                            instance.getTaskType(),
                            instance.getInferenceEntityId(),
                            null,
                            null,
                            null,
                            instance.getInput(),
                            instance.getTaskSettings(),
                            instance.getInputType(),
                            InferenceAction.Request.DEFAULT_TIMEOUT,
                            false
                        );
                    } else if (version.before(TransportVersions.INFERENCE_CONTEXT)
                        && version.isPatchFrom(TransportVersions.INFERENCE_CONTEXT_8_X) == false) {
                            mutated = new InferenceAction.Request(
                                instance.getTaskType(),
                                instance.getInferenceEntityId(),
                                instance.getQuery(),
                                null,
                                null,
                                instance.getInput(),
                                instance.getTaskSettings(),
                                instance.getInputType(),
                                instance.getInferenceTimeout(),
                                false,
                                InferenceContext.EMPTY_INSTANCE
                            );
                        } else if (version.before(TransportVersions.RERANK_COMMON_OPTIONS_ADDED)
                            && version.isPatchFrom(TransportVersions.RERANK_COMMON_OPTIONS_ADDED_8_19) == false) {
                                mutated = new InferenceAction.Request(
                                    instance.getTaskType(),
                                    instance.getInferenceEntityId(),
                                    instance.getQuery(),
                                    null,
                                    null,
                                    instance.getInput(),
                                    instance.getTaskSettings(),
                                    instance.getInputType(),
                                    instance.getInferenceTimeout(),
                                    false,
                                    instance.getContext()
                                );
                            } else {
                                mutated = instance;
                            }

        // We always assume that a request has been rerouted, if it came from a node without adaptive rate limiting
        if (version.before(TransportVersions.INFERENCE_REQUEST_ADAPTIVE_RATE_LIMITING)) {
            mutated.setHasBeenRerouted(true);
        } else {
            mutated.setHasBeenRerouted(instance.hasBeenRerouted());
        }

        return mutated;
    }

    public void testWriteTo_WhenVersionIsOnAfterUnspecifiedAdded() throws IOException {
        InferenceAction.Request instance = new InferenceAction.Request(
            TaskType.TEXT_EMBEDDING,
            "model",
            null,
            null,
            null,
            List.of(),
            Map.of(),
            InputType.UNSPECIFIED,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            false
        );

        InferenceAction.Request deserializedInstance = copyWriteable(
            instance,
            getNamedWriteableRegistry(),
            instanceReader(),
            TransportVersions.V_8_13_0
        );

        assertThat(deserializedInstance.getInputType(), is(InputType.UNSPECIFIED));
    }

    public void testWriteTo_WhenVersionIsBeforeInputTypeAdded_ShouldSetInputTypeToUnspecified() throws IOException {
        var instance = new InferenceAction.Request(
            TaskType.TEXT_EMBEDDING,
            "model",
            null,
            null,
            null,
            List.of(),
            Map.of(),
            InputType.INGEST,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            false
        );

        InferenceAction.Request deserializedInstance = copyWriteable(
            instance,
            getNamedWriteableRegistry(),
            instanceReader(),
            TransportVersions.V_8_12_1
        );

        assertThat(deserializedInstance.getInputType(), is(InputType.UNSPECIFIED));
    }

    public void testWriteTo_WhenVersionIsBeforeAdaptiveRateLimiting_ShouldSetHasBeenReroutedToTrue() throws IOException {
        var instance = new InferenceAction.Request(
            TaskType.TEXT_EMBEDDING,
            "model",
            null,
            null,
            null,
            List.of("input"),
            Map.of(),
            InputType.UNSPECIFIED,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            false
        );

        InferenceAction.Request deserializedInstance = copyWriteable(
            instance,
            getNamedWriteableRegistry(),
            instanceReader(),
            TransportVersions.V_8_13_0
        );

        // Verify that hasBeenRerouted is true after deserializing a request coming from an older transport version
        assertTrue(deserializedInstance.hasBeenRerouted());
    }

    public void testWriteTo_WhenVersionIsBeforeInferenceContext_ShouldSetContextToEmptyContext() throws IOException {
        var instance = new InferenceAction.Request(
            TaskType.TEXT_EMBEDDING,
            "model",
            null,
            null,
            null,
            List.of("input"),
            Map.of(),
            InputType.UNSPECIFIED,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            false,
            new InferenceContext(randomAlphaOfLength(10))
        );

        InferenceAction.Request deserializedInstance = copyWriteable(
            instance,
            getNamedWriteableRegistry(),
            instanceReader(),
            TransportVersions.V_8_15_0
        );

        // Verify that context is empty after deserializing a request coming from an older transport version
        assertThat(deserializedInstance.getContext(), equalTo(InferenceContext.EMPTY_INSTANCE));
    }

    public void testGetInputTypeToWrite_ReturnsIngest_WhenInputTypeIsUnspecified_VersionBeforeUnspecifiedIntroduced() {
        assertThat(getInputTypeToWrite(InputType.UNSPECIFIED, TransportVersions.V_8_12_1), is(InputType.INGEST));
    }

    public void testGetInputTypeToWrite_ReturnsIngest_WhenInputTypeIsClassification_VersionBeforeUnspecifiedIntroduced() {
        assertThat(getInputTypeToWrite(InputType.CLASSIFICATION, TransportVersions.V_8_12_1), is(InputType.INGEST));
    }

    public void testGetInputTypeToWrite_ReturnsIngest_WhenInputTypeIsClustering_VersionBeforeUnspecifiedIntroduced() {
        assertThat(getInputTypeToWrite(InputType.CLUSTERING, TransportVersions.V_8_12_1), is(InputType.INGEST));
    }
}
