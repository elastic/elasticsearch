/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * This file was contributed to by a generative AI
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.inference.TaskType;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class InferenceCrudIT extends InferenceBaseRestTest {

    @After
    public void cleanup() throws Exception {
        waitForPendingTasks(client());
    }

    public void testPutAndDeleteModel() throws IOException {
        String modelId = "a_model_for_happy_case";
        createBasicModel(modelId);
        deleteModel(modelId, true);
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
        putModel("sparse_embedding_model_one", mockServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var e = expectThrows(ResponseException.class, () -> getModels("sparse_embedding_model_one", TaskType.TEXT_EMBEDDING));
        assertThat(
            e.getMessage(),
            containsString("Requested task type [text_embedding] does not match the model's task type [sparse_embedding]")
        );
    }

    @SuppressWarnings("unchecked")
    public void testGetModelWithAnyTaskType() throws IOException {
        String modelId = "sparse_embedding_model_two";
        putModel(modelId, mockServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        var singleModel = (List<Map<String, Object>>) getModels(modelId, TaskType.ANY).get("models");
        System.out.println("MODEL" + singleModel);
        assertEquals(modelId, singleModel.get(0).get("model_id"));
        assertEquals(TaskType.SPARSE_EMBEDDING.toString(), singleModel.get(0).get("task_type"));
    }

    public void testPutModelFailsWhenTrainedModelWithIdAlreadyExists() throws Exception {
        String modelId = "duplicate_model_id";
        createBasicModel(modelId);
        Request request = new Request("GET", "_ml/trained_models/_all");
        System.out.println("99598" + client().performRequest(request));
        startDeployment(modelId);

        var e = expectThrows(ResponseException.class, () -> putModel(modelId, mockServiceModelConfig(), TaskType.SPARSE_EMBEDDING));
        assertEquals("Trained machine learning model [duplicate_model_id] already exists", e.getMessage());

        // clean up
        deleteModel(modelId, true);
    }

}
