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

public class MockRerankInferenceServiceIT extends InferenceBaseRestTest {

    @SuppressWarnings("unchecked")
    public void testMockService() throws IOException {
        String inferenceEntityId = "test-mock";
        var putModel = putModel(inferenceEntityId, mockRerankServiceModelConfig(), TaskType.RERANK);
        var model = getModels(inferenceEntityId, TaskType.RERANK).get(0);

        for (var modelMap : List.of(putModel, model)) {
            assertEquals(inferenceEntityId, modelMap.get("inference_id"));
            assertEquals(TaskType.RERANK, TaskType.fromString((String) modelMap.get("task_type")));
            assertEquals("test_reranking_service", modelMap.get("service"));
        }

        List<String> input = List.of(randomAlphaOfLength(10));
        var inference = infer(inferenceEntityId, input);
        assertNonEmptyInferenceResults(inference, 1, TaskType.RERANK);
        assertEquals(inference, infer(inferenceEntityId, input));
        assertNotEquals(inference, infer(inferenceEntityId, randomValueOtherThan(input, () -> List.of(randomAlphaOfLength(10)))));
    }

    public void testMockServiceWithMultipleInputs() throws IOException {
        String inferenceEntityId = "test-mock-with-multi-inputs";
        putModel(inferenceEntityId, mockRerankServiceModelConfig(), TaskType.RERANK);
        var queryParams = Map.of("timeout", "120s");

        var inference = infer(
            inferenceEntityId,
            TaskType.RERANK,
            List.of(randomAlphaOfLength(5), randomAlphaOfLength(10)),
            "What if?",
            queryParams
        );

        assertNonEmptyInferenceResults(inference, 2, TaskType.RERANK);
    }

    @SuppressWarnings("unchecked")
    public void testMockService_DoesNotReturnSecretsInGetResponse() throws IOException {
        String inferenceEntityId = "test-mock";
        var putModel = putModel(inferenceEntityId, mockRerankServiceModelConfig(), TaskType.RERANK);
        var model = getModels(inferenceEntityId, TaskType.RERANK).get(0);

        var serviceSettings = (Map<String, Object>) model.get("service_settings");
        assertNull(serviceSettings.get("api_key"));
        assertNotNull(serviceSettings.get("model_id"));

        var putServiceSettings = (Map<String, Object>) putModel.get("service_settings");
        assertNull(putServiceSettings.get("api_key"));
        assertNotNull(putServiceSettings.get("model_id"));
    }
}
