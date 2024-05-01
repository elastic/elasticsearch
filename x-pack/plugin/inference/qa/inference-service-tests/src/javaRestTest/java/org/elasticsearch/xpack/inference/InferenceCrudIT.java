/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file has been contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.TaskType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class InferenceCrudIT extends InferenceBaseRestTest {

    @SuppressWarnings("unchecked")
    public void testGet() throws IOException {
        for (int i = 0; i < 5; i++) {
            putModel("se_model_" + i, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        }
        for (int i = 0; i < 4; i++) {
            putModel("te_model_" + i, mockSparseServiceModelConfig(), TaskType.TEXT_EMBEDDING);
        }

        var getAllModels = getAllModels();
        assertThat(getAllModels, hasSize(9));

        var getSparseModels = getModels("_all", TaskType.SPARSE_EMBEDDING);
        assertThat(getSparseModels, hasSize(5));
        for (var sparseModel : getSparseModels) {
            assertEquals("sparse_embedding", sparseModel.get("task_type"));
        }

        var getDenseModels = getModels("_all", TaskType.TEXT_EMBEDDING);
        assertThat(getDenseModels, hasSize(4));
        for (var denseModel : getDenseModels) {
            assertEquals("text_embedding", denseModel.get("task_type"));
        }

        var singleModel = getModels("se_model_1", TaskType.SPARSE_EMBEDDING);
        assertThat(singleModel, hasSize(1));
        assertEquals("se_model_1", singleModel.get(0).get("model_id"));

        for (int i = 0; i < 5; i++) {
            deleteModel("se_model_" + i, TaskType.SPARSE_EMBEDDING);
        }
        for (int i = 0; i < 4; i++) {
            deleteModel("te_model_" + i, TaskType.TEXT_EMBEDDING);
        }
    }

    public void testGetModelWithWrongTaskType() throws IOException {
        putModel("sparse_embedding_model", mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var e = expectThrows(ResponseException.class, () -> getModels("sparse_embedding_model", TaskType.TEXT_EMBEDDING));
        assertThat(
            e.getMessage(),
            containsString("Requested task type [text_embedding] does not match the inference endpoint's task type [sparse_embedding]")
        );
    }

    public void testDeleteModelWithWrongTaskType() throws IOException {
        putModel("sparse_embedding_model", mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var e = expectThrows(ResponseException.class, () -> deleteModel("sparse_embedding_model", TaskType.TEXT_EMBEDDING));
        assertThat(
            e.getMessage(),
            containsString("Requested task type [text_embedding] does not match the inference endpoint's task type [sparse_embedding]")
        );
    }

    @SuppressWarnings("unchecked")
    public void testGetModelWithAnyTaskType() throws IOException {
        String inferenceEntityId = "sparse_embedding_model";
        putModel(inferenceEntityId, mockSparseServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var singleModel = getModels(inferenceEntityId, TaskType.ANY);
        assertEquals(inferenceEntityId, singleModel.get(0).get("model_id"));
        assertEquals(TaskType.SPARSE_EMBEDDING.toString(), singleModel.get(0).get("task_type"));
    }

    @SuppressWarnings("unchecked")
    public void testApisWithoutTaskType() throws IOException {
        String modelId = "no_task_type_in_url";
        putModel(modelId, mockSparseServiceModelConfig(TaskType.SPARSE_EMBEDDING));
        var singleModel = getModel(modelId);
        assertEquals(modelId, singleModel.get("model_id"));
        assertEquals(TaskType.SPARSE_EMBEDDING.toString(), singleModel.get("task_type"));

        var inference = inferOnMockService(modelId, List.of(randomAlphaOfLength(10)));
        assertNonEmptyInferenceResults(inference, 1, TaskType.SPARSE_EMBEDDING);
        deleteModel(modelId);
    }

    public void testSkipValidationAndStart() throws IOException {
        String openAiConfigWithBadApiKey = """
            {
                "service": "openai",
                "service_settings": {
                    "api_key": "XXXX"
                },
                "task_settings": {
                   "model": "text-embedding-ada-002"
                }
            }
            """;

        updateClusterSettings(Settings.builder().put("xpack.inference.skip_validate_and_start", true).build());

        // We would expect an error about the invalid API key if the validation occurred
        putModel("unvalidated", openAiConfigWithBadApiKey, TaskType.TEXT_EMBEDDING);
    }
}
