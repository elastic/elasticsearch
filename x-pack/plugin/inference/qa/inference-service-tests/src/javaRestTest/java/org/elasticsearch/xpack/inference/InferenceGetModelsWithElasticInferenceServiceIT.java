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
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.getAllModels;
import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.getModels;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class InferenceGetModelsWithElasticInferenceServiceIT extends BaseMockEISAuthServerTest {

    /**
     * This is done before the class because I've run into issues where another class that extends {@link BaseMockEISAuthServerTest}
     * results in an authorization response not being queued up for the new Elasticsearch Node in time. When the node starts up, it
     * retrieves authorization. If the request isn't queued up when that happens the tests will fail. From my testing locally it seems
     * like the base class's static functionality to queue a response is only done once and not for each subclass.
     *
     * My understanding is that the @Before will be run after the node starts up and wouldn't be sufficient to handle
     * this scenario. That is why this needs to be @BeforeClass.
     */
    @BeforeClass
    public static void init() {
        // Ensure the mock EIS server has an authorized response ready
        mockEISServer.enqueueAuthorizeAllModelsResponse();
    }

    public void testGetDefaultEndpoints() throws IOException {
        var allModels = getAllModels();
        var chatCompletionModels = getModels("_all", TaskType.CHAT_COMPLETION);

        assertThat(allModels, hasSize(8));
        assertThat(chatCompletionModels, hasSize(2));

        for (var model : chatCompletionModels) {
            assertEquals("chat_completion", model.get("task_type"));
        }

        assertInferenceIdTaskType(allModels, ".rainbow-sprinkles-elastic", TaskType.CHAT_COMPLETION);
        assertInferenceIdTaskType(allModels, ".gp-llm-v2-chat_completion", TaskType.CHAT_COMPLETION);
        assertInferenceIdTaskType(allModels, ".elser-2-elastic", TaskType.SPARSE_EMBEDDING);
        assertInferenceIdTaskType(allModels, ".jina-embeddings-v3", TaskType.TEXT_EMBEDDING);
        assertInferenceIdTaskType(allModels, ".elastic-rerank-v1", TaskType.RERANK);
    }

    private static void assertInferenceIdTaskType(List<Map<String, Object>> models, String inferenceId, TaskType taskType) {
        var model = models.stream().filter(m -> m.get("inference_id").equals(inferenceId)).findFirst();
        assertTrue("could not find inference id: " + inferenceId, model.isPresent());
        assertThat(model.get().get("task_type"), is(taskType.toString()));
    }
}
