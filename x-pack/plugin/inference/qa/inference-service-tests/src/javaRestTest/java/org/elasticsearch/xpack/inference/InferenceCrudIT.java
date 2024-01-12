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
import org.elasticsearch.inference.TaskType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class InferenceCrudIT extends InferenceBaseRestTest {

    public void testElserCrud() throws IOException {

        String elserConfig = """
            {
              "service": "elser",
              "service_settings": {
                "num_allocations": 1,
                "num_threads": 1
              },
              "task_settings": {}
            }
            """;

        // Model already downloaded
        {
            String modelId = randomAlphaOfLength(10).toLowerCase();
            putModel(modelId, elserConfig, TaskType.SPARSE_EMBEDDING);
            var models = getModels(modelId, TaskType.SPARSE_EMBEDDING);
            assertThat(models.get("models").toString(), containsString("model_id=" + modelId));

            deleteModel(modelId, TaskType.SPARSE_EMBEDDING);
            expectThrows(ResponseException.class, () -> getModels(modelId, TaskType.SPARSE_EMBEDDING));
            models = getTrainedModel("_all");
            assertThat(models.toString(), not(containsString("deployment_id=" + modelId)));
        }

        // Model downloaded automatically & test infer
        {
            String modelId = randomAlphaOfLength(10).toLowerCase();
            putElserInference(modelId, TaskType.SPARSE_EMBEDDING);
            var models = getTrainedModel("_all");
            assertThat(models.toString(), containsString("deployment_id=" + modelId));

            Map<String, Object> results = inferOnMockService(
                modelId,
                TaskType.SPARSE_EMBEDDING,
                List.of("hello world", "this is the second document")
            );
            System.out.println(results.toString());
            assert (((Map) ((Map) ((List) results.get("sparse_embedding")).get(0)).get("embedding")).size() > 1); // there exists embeddings
            assert (((Map) ((List) results.get("sparse_embedding")).get(0)).size() == 2); // there are two sets of embeddings
        }
    }

    @SuppressWarnings("unchecked")
    public void testGet() throws IOException {
        for (int i = 0; i < 5; i++) {
            putModel("se_model_" + i, mockServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        }
        for (int i = 0; i < 4; i++) {
            putModel("te_model_" + i, mockServiceModelConfig(), TaskType.TEXT_EMBEDDING);
        }

        var getAllModels = (List<Map<String, Object>>) getAllModels().get("models");
        assertThat(getAllModels, hasSize(9));

        var getSparseModels = (List<Map<String, Object>>) getModels("_all", TaskType.SPARSE_EMBEDDING).get("models");
        assertThat(getSparseModels, hasSize(5));
        for (var sparseModel : getSparseModels) {
            assertEquals("sparse_embedding", sparseModel.get("task_type"));
        }

        var getDenseModels = (List<Map<String, Object>>) getModels("_all", TaskType.TEXT_EMBEDDING).get("models");
        assertThat(getDenseModels, hasSize(4));
        for (var denseModel : getDenseModels) {
            assertEquals("text_embedding", denseModel.get("task_type"));
        }

        var singleModel = (List<Map<String, Object>>) getModels("se_model_1", TaskType.SPARSE_EMBEDDING).get("models");
        assertThat(singleModel, hasSize(1));
        assertEquals("se_model_1", singleModel.get(0).get("model_id"));
    }

    public void testGetModelWithWrongTaskType() throws IOException {
        putModel("sparse_embedding_model", mockServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var e = expectThrows(ResponseException.class, () -> getModels("sparse_embedding_model", TaskType.TEXT_EMBEDDING));
        assertThat(
            e.getMessage(),
            containsString("Requested task type [text_embedding] does not match the model's task type [sparse_embedding]")
        );
    }

    @SuppressWarnings("unchecked")
    public void testGetModelWithAnyTaskType() throws IOException {
        String modelId = "sparse_embedding_model";
        putModel(modelId, mockServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var singleModel = (List<Map<String, Object>>) getModels(modelId, TaskType.ANY).get("models");
        System.out.println("MODEL" + singleModel);
        assertEquals(modelId, singleModel.get(0).get("model_id"));
        assertEquals(TaskType.SPARSE_EMBEDDING.toString(), singleModel.get(0).get("task_type"));
    }
}
