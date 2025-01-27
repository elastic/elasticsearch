/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file has been contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceFeature;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.getAllModels;
import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.getModels;
import static org.hamcrest.Matchers.hasSize;

public class InferenceGetModelsWithElasticInferenceServiceIT extends BaseMockEISAuthServerTest {

    public void testGetDefaultEndpoints() throws IOException {
        var allModels = getAllModels();
        var chatCompletionModels = getModels("_all", TaskType.CHAT_COMPLETION);

        if ((ElasticInferenceServiceFeature.DEPRECATED_ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled()
            || ElasticInferenceServiceFeature.ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG.isEnabled())) {
            assertThat(allModels, hasSize(4));
            assertThat(chatCompletionModels, hasSize(1));

            for (var model : chatCompletionModels) {
                assertEquals("chat_completion", model.get("task_type"));
            }
        } else {
            assertThat(allModels, hasSize(3));
            assertThat(chatCompletionModels, hasSize(0));
        }

    }
}
