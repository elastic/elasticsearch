/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.action;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModelTests;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModelTests;

import static org.hamcrest.Matchers.is;

/**
 * {@link ModelStrategyFactory#getStrategy} dispatches on the model type, and — because a single
 * {@code ElasticInferenceServiceCompletionModel} class backs both {@link TaskType#COMPLETION} and
 * {@link TaskType#CHAT_COMPLETION} — on the model's task type as well. These tests pin that dispatch table, since choosing the wrong
 * completion strategy silently parses the response with the wrong entity rather than failing loudly.
 */
public class ModelStrategyFactoryTests extends ESTestCase {

    private static final String URL = "http://eis-gateway.com";
    private static final String MODEL_ID = "my-model-id";

    public void testGetStrategy_CompletionModel_WithCompletionTaskType() {
        var model = ElasticInferenceServiceCompletionModelTests.createModel(URL, MODEL_ID, TaskType.COMPLETION);

        var strategy = ModelStrategyFactory.getStrategy(model);

        assertThat(strategy.requestDescription(), is("Elastic Inference Service completion"));
    }

    public void testGetStrategy_CompletionModel_WithChatCompletionTaskType() {
        var model = ElasticInferenceServiceCompletionModelTests.createModel(URL, MODEL_ID, TaskType.CHAT_COMPLETION);

        var strategy = ModelStrategyFactory.getStrategy(model);

        assertThat(strategy.requestDescription(), is("Elastic Inference Service chat completion"));
    }

    public void testGetStrategy_CompletionModel_WithUnsupportedTaskType_Throws() {
        var model = ElasticInferenceServiceCompletionModelTests.createModel(URL, MODEL_ID, TaskType.SPARSE_EMBEDDING);

        var exception = expectThrows(IllegalArgumentException.class, () -> ModelStrategyFactory.getStrategy(model));

        assertThat(exception.getMessage(), is("No strategy found for completion model with task type: sparse_embedding"));
    }

    public void testGetStrategy_SparseEmbeddingsModel() {
        var model = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(URL, MODEL_ID);

        var strategy = ModelStrategyFactory.getStrategy(model);

        assertThat(strategy.requestDescription(), is("Elastic Inference Service sparse embeddings"));
    }

    public void testGetStrategy_RerankModel() {
        var model = ElasticInferenceServiceRerankModelTests.createModel(URL, MODEL_ID);

        var strategy = ModelStrategyFactory.getStrategy(model);

        assertThat(strategy.requestDescription(), is("Elastic Inference Service rerank"));
    }

    public void testGetStrategy_DenseEmbeddingsModel() {
        var model = ElasticInferenceServiceDenseEmbeddingsModelTests.createTextEmbeddingModel(URL, MODEL_ID);

        var strategy = ModelStrategyFactory.getStrategy(model);

        assertThat(strategy.requestDescription(), is("Elastic Inference Service dense embeddings"));
    }
}
