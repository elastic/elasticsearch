/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.inference.TaskType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;

public class InferenceCrudIT extends InferenceBaseRestTest {

    @SuppressWarnings("unchecked")
    public void testGet() throws IOException {
        for (int i = 0; i < 5; i++) {
            putModel("se_model_" + i, mockServiceModelConfig(), TaskType.SPARSE_EMBEDDING);
        }
        for (int i = 0; i < 4; i++) {
            putModel("te_model_" + i, mockServiceModelConfig(), TaskType.TEXT_EMBEDDING);
        }

        var getAllModels = (List<Map<String, Object>>) getModels("_all", TaskType.ANY).get("models");
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
}
