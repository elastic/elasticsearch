/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension.DEFAULT_EMBEDDING_DIMENSIONS;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class MockDenseInferenceServiceIT extends InferenceBaseRestTest {

    private String inferenceEntityId;

    @Before
    public void init() {
        inferenceEntityId = randomIdentifier();
    }

    @After
    public void shutdown() throws IOException {
        deleteModel(inferenceEntityId);
    }

    public void testMockService() throws IOException {
        var putModel = putModel(inferenceEntityId, mockTextEmbeddingServiceModelConfig(), TaskType.TEXT_EMBEDDING);
        var model = getModels(inferenceEntityId, TaskType.TEXT_EMBEDDING).get(0);

        for (var modelMap : List.of(putModel, model)) {
            assertEquals(inferenceEntityId, modelMap.get("inference_id"));
            assertEquals(TaskType.TEXT_EMBEDDING, TaskType.fromString((String) modelMap.get("task_type")));
            assertEquals("text_embedding_test_service", modelMap.get("service"));
        }

        List<String> input = List.of(randomAlphaOfLength(10));
        var inference = infer(inferenceEntityId, input);
        assertNonEmptyInferenceResults(inference, 1, TaskType.TEXT_EMBEDDING);
        // Same input should return the same result
        assertEquals(inference, infer(inferenceEntityId, input));
        // Different input values should not
        assertNotEquals(inference, infer(inferenceEntityId, randomValueOtherThan(input, () -> List.of(randomAlphaOfLength(10)))));
    }

    public void testMockServiceWithMultipleInputs() throws IOException {
        putModel(inferenceEntityId, mockTextEmbeddingServiceModelConfig(), TaskType.TEXT_EMBEDDING);

        // The response is randomly generated, the input can be anything
        var inference = infer(
            inferenceEntityId,
            TaskType.TEXT_EMBEDDING,
            List.of(randomAlphaOfLength(5), randomAlphaOfLength(10), randomAlphaOfLength(15))
        );

        assertNonEmptyInferenceResults(inference, 3, TaskType.TEXT_EMBEDDING);
    }

    public void testMockService_withEmbeddingTask() throws IOException {
        var putModel = putModel(inferenceEntityId, mockEmbeddingServiceModelConfig(), TaskType.EMBEDDING);
        var model = getModels(inferenceEntityId, TaskType.EMBEDDING).getFirst();

        for (var modelMap : List.of(putModel, model)) {
            assertEquals(inferenceEntityId, modelMap.get("inference_id"));
            assertEquals(TaskType.EMBEDDING, TaskType.fromString((String) modelMap.get("task_type")));
            assertEquals("text_embedding_test_service", modelMap.get("service"));
        }

        var input = List.of(InferenceString.ofText(randomAlphaOfLength(10)));
        var inference = embedding(inferenceEntityId, input);
        assertNonEmptyInferenceResults(inference, 1, TaskType.EMBEDDING);
        // Same input should return the same result
        assertEquals(inference, embedding(inferenceEntityId, input));
        // Different input values should not
        assertNotEquals(
            inference,
            embedding(inferenceEntityId, randomValueOtherThan(input, () -> List.of(InferenceString.ofText(randomAlphaOfLength(10)))))
        );
    }

    public void testMockServiceWithMultipleInputs_withEmbeddingTask() throws IOException {
        putModel(inferenceEntityId, mockEmbeddingServiceModelConfig(), TaskType.EMBEDDING);

        // The response is randomly generated, the input can be anything
        var inference = embedding(
            inferenceEntityId,
            List.of(
                new InferenceString(
                    DataType.IMAGE,
                    "data:image/jpeg;base64," + Base64.getEncoder().encodeToString(randomByteArrayOfLength(5))
                ),
                InferenceString.ofText(randomAlphaOfLength(10)),
                InferenceString.ofText(randomAlphaOfLength(15))
            )
        );

        assertNonEmptyInferenceResults(inference, 3, TaskType.EMBEDDING);
    }

    @SuppressWarnings("unchecked")
    public void testMockService_DoesNotReturnSecretsInGetResponse() throws IOException {
        var putModel = putModel(inferenceEntityId, mockTextEmbeddingServiceModelConfig(), TaskType.TEXT_EMBEDDING);
        var model = getModels(inferenceEntityId, TaskType.TEXT_EMBEDDING).get(0);

        var serviceSettings = (Map<String, Object>) model.get("service_settings");
        assertNull(serviceSettings.get("api_key"));
        assertNotNull(serviceSettings.get("model"));

        var putServiceSettings = (Map<String, Object>) putModel.get("service_settings");
        assertNull(putServiceSettings.get("api_key"));
        assertNotNull(putServiceSettings.get("model"));
    }

    public void testMockService_DimensionsNotSpecified_TextEmbedding() throws IOException {
        var putModel = putModel(inferenceEntityId, mockTextEmbeddingServiceModelConfig_NoDimensions(), TaskType.TEXT_EMBEDDING);
        var model = getModels(inferenceEntityId, TaskType.TEXT_EMBEDDING).get(0);

        for (var modelMap : List.of(putModel, model)) {
            assertEquals(inferenceEntityId, modelMap.get("inference_id"));
            assertEquals(TaskType.TEXT_EMBEDDING, TaskType.fromString((String) modelMap.get("task_type")));
            assertEquals("text_embedding_test_service", modelMap.get("service"));
            @SuppressWarnings("unchecked")
            Map<String, Object> serviceSettings = (Map<String, Object>) modelMap.get("service_settings");
            assertThat(serviceSettings.get("dimensions"), is(DEFAULT_EMBEDDING_DIMENSIONS));
        }

        var input = List.of(randomAlphaOfLength(10));
        var resultMap = infer(inferenceEntityId, input);
        @SuppressWarnings("unchecked")
        var embeddings = (List<Map<String, List<Float>>>) resultMap.get(DenseEmbeddingFloatResults.TEXT_EMBEDDING);
        assertThat(embeddings.getFirst().get(EmbeddingResults.EMBEDDING), hasSize(DEFAULT_EMBEDDING_DIMENSIONS));
    }

    public void testMockService_DimensionsNotSpecified_Embedding() throws IOException {
        var putModel = putModel(inferenceEntityId, mockEmbeddingServiceModelConfig_NoDimensions(), TaskType.EMBEDDING);
        var model = getModels(inferenceEntityId, TaskType.EMBEDDING).get(0);

        for (var modelMap : List.of(putModel, model)) {
            assertEquals(inferenceEntityId, modelMap.get("inference_id"));
            assertEquals(TaskType.EMBEDDING, TaskType.fromString((String) modelMap.get("task_type")));
            assertEquals("text_embedding_test_service", modelMap.get("service"));
            @SuppressWarnings("unchecked")
            Map<String, Object> serviceSettings = (Map<String, Object>) modelMap.get("service_settings");
            assertThat(serviceSettings.get("dimensions"), is(DEFAULT_EMBEDDING_DIMENSIONS));
        }

        var input = List.of(InferenceString.ofText(randomAlphaOfLength(10)));
        var resultMap = embedding(inferenceEntityId, input);
        @SuppressWarnings("unchecked")
        var embeddings = (List<Map<String, List<Float>>>) resultMap.get(GenericDenseEmbeddingFloatResults.EMBEDDINGS);
        assertThat(embeddings.getFirst().get(EmbeddingResults.EMBEDDING), hasSize(DEFAULT_EMBEDDING_DIMENSIONS));
    }
}
