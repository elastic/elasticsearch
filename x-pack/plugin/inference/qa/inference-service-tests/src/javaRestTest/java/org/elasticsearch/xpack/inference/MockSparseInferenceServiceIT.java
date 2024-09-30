/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.inference.TaskType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class MockSparseInferenceServiceIT extends InferenceBaseRestTest {

    @SuppressWarnings("unchecked")
    public void testMockService() throws IOException {
        String inferenceEntityId = "test-mock";
        var putModel = putModel(inferenceEntityId, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var model = getModels(inferenceEntityId, TaskType.SPARSE_EMBEDDING).get(0);

        for (var modelMap : List.of(putModel, model)) {
            assertEquals(inferenceEntityId, modelMap.get("inference_id"));
            assertEquals(TaskType.SPARSE_EMBEDDING, TaskType.fromString((String) modelMap.get("task_type")));
            assertEquals("test_service", modelMap.get("service"));
        }

        List<String> input = List.of(randomAlphaOfLength(10));
        var inference = inferOnMockService(inferenceEntityId, input);
        assertNonEmptyInferenceResults(inference, 1, TaskType.SPARSE_EMBEDDING);
        // Same input should return the same result
        assertEquals(inference, inferOnMockService(inferenceEntityId, input));
        // Different input values should not
        assertNotEquals(
            inference,
            inferOnMockService(inferenceEntityId, randomValueOtherThan(input, () -> List.of(randomAlphaOfLength(10))))
        );
    }

    public void testMockServiceWithMultipleInputs() throws IOException {
        String inferenceEntityId = "test-mock-with-multi-inputs";
        putModel(inferenceEntityId, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);

        // The response is randomly generated, the input can be anything
        var inference = inferOnMockService(
            inferenceEntityId,
            TaskType.SPARSE_EMBEDDING,
            List.of(randomAlphaOfLength(5), randomAlphaOfLength(10), randomAlphaOfLength(15))
        );

        assertNonEmptyInferenceResults(inference, 3, TaskType.SPARSE_EMBEDDING);
    }

    @SuppressWarnings("unchecked")
    public void testMockService_DoesNotReturnSecretsInGetResponse() throws IOException {
        String inferenceEntityId = "test-mock";
        var putModel = putModel(inferenceEntityId, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var model = getModels(inferenceEntityId, TaskType.SPARSE_EMBEDDING).get(0);

        var serviceSettings = (Map<String, Object>) model.get("service_settings");
        assertNull(serviceSettings.get("api_key"));
        assertNotNull(serviceSettings.get("model"));

        var putServiceSettings = (Map<String, Object>) putModel.get("service_settings");
        assertNull(putServiceSettings.get("api_key"));
        assertNotNull(putServiceSettings.get("model"));
    }

    @SuppressWarnings("unchecked")
    public void testMockService_DoesNotReturnHiddenField_InModelResponses() throws IOException {
        String inferenceEntityId = "test-mock";
        var putModel = putModel(inferenceEntityId, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var model = getModels(inferenceEntityId, TaskType.SPARSE_EMBEDDING).get(0);

        for (var modelMap : List.of(putModel, model)) {
            assertEquals(inferenceEntityId, modelMap.get("inference_id"));
            assertThat(modelMap.get("service_settings"), is(Map.of("model", "my_model")));
            assertEquals(TaskType.SPARSE_EMBEDDING, TaskType.fromString((String) modelMap.get("task_type")));
            assertEquals("test_service", modelMap.get("service"));
        }

        // The response is randomly generated, the input can be anything
        var inference = inferOnMockService(inferenceEntityId, List.of(randomAlphaOfLength(10)));
        assertNonEmptyInferenceResults(inference, 1, TaskType.SPARSE_EMBEDDING);
    }

    @SuppressWarnings("unchecked")
    public void testMockService_DoesReturnHiddenField_InModelResponses() throws IOException {
        String inferenceEntityId = "test-mock";
        var putModel = putModel(inferenceEntityId, mockSparseServiceModelConfig(null, true), TaskType.SPARSE_EMBEDDING);
        var model = getModels(inferenceEntityId, TaskType.SPARSE_EMBEDDING).get(0);

        for (var modelMap : List.of(putModel, model)) {
            assertEquals(inferenceEntityId, modelMap.get("inference_id"));
            assertThat(modelMap.get("service_settings"), is(Map.of("model", "my_model", "hidden_field", "my_hidden_value")));
            assertEquals(TaskType.SPARSE_EMBEDDING, TaskType.fromString((String) modelMap.get("task_type")));
            assertEquals("test_service", modelMap.get("service"));
        }

        // The response is randomly generated, the input can be anything
        var inference = inferOnMockService(inferenceEntityId, List.of(randomAlphaOfLength(10)));
        assertNonEmptyInferenceResults(inference, 1, TaskType.SPARSE_EMBEDDING);
    }
}
