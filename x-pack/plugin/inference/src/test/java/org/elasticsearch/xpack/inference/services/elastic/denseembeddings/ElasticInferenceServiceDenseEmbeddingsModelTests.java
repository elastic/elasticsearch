/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.denseembeddings;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceDenseEmbeddingsModelTests extends ESTestCase {

    public void testUriCreation_TextEmbedding() {
        var model = createTextEmbeddingModel("http://eis-gateway.com", "my-model-id");

        assertThat(model.uri().toString(), is("http://eis-gateway.com/api/v1/embed/text/dense"));
    }

    public void testUriCreation_Embedding() {
        var model = createEmbeddingModel("http://eis-gateway.com", "my-model-id");

        assertThat(model.uri().toString(), is("http://eis-gateway.com/api/v1/embed/dense"));
    }

    public void testUriCreation_WithTrailingSlash_TextEmbedding() {
        var model = createTextEmbeddingModel("http://eis-gateway.com/", "my-model-id");

        assertThat(model.uri().toString(), is("http://eis-gateway.com/api/v1/embed/text/dense"));
    }

    public void testUriCreation_WithTrailingSlash_Embedding() {
        var model = createEmbeddingModel("http://eis-gateway.com/", "my-model-id");

        assertThat(model.uri().toString(), is("http://eis-gateway.com/api/v1/embed/dense"));
    }

    public static ElasticInferenceServiceDenseEmbeddingsModel createTextEmbeddingModel(String url, String modelId) {
        return createModel(TaskType.TEXT_EMBEDDING, url, modelId);
    }

    public static ElasticInferenceServiceDenseEmbeddingsModel createEmbeddingModel(String url, String modelId) {
        return createModel(TaskType.EMBEDDING, url, modelId);
    }

    public static ElasticInferenceServiceDenseEmbeddingsModel createModel(TaskType taskType, String url, String modelId) {
        return new ElasticInferenceServiceDenseEmbeddingsModel(
            "id",
            taskType,
            new ElasticInferenceServiceDenseEmbeddingsServiceSettings(modelId, SimilarityMeasure.COSINE, null, null),
            ElasticInferenceServiceComponents.of(url),
            ChunkingSettingsBuilder.DEFAULT_SETTINGS
        );
    }

    public static ElasticInferenceServiceDenseEmbeddingsModel createTextEmbeddingModel(
        String url,
        ElasticInferenceServiceDenseEmbeddingsServiceSettings serviceSettings,
        ChunkingSettings chunkingSettings
    ) {
        return createModel(TaskType.TEXT_EMBEDDING, url, serviceSettings, chunkingSettings);
    }

    public static ElasticInferenceServiceDenseEmbeddingsModel createEmbeddingModel(
        String url,
        ElasticInferenceServiceDenseEmbeddingsServiceSettings serviceSettings,
        ChunkingSettings chunkingSettings
    ) {
        return createModel(TaskType.EMBEDDING, url, serviceSettings, chunkingSettings);
    }

    public static ElasticInferenceServiceDenseEmbeddingsModel createModel(
        TaskType taskType,
        String url,
        ElasticInferenceServiceDenseEmbeddingsServiceSettings serviceSettings,
        ChunkingSettings chunkingSettings
    ) {
        return new ElasticInferenceServiceDenseEmbeddingsModel(
            "id",
            taskType,
            serviceSettings,
            ElasticInferenceServiceComponents.of(url),
            chunkingSettings
        );
    }
}
