/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsBuilder;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

public class ElasticInferenceServiceSparseEmbeddingsModelTests extends ESTestCase {

    public static ElasticInferenceServiceSparseEmbeddingsModel createModel(String url, String modelId) {
        return createModel(url, modelId, null, null);
    }

    public static ElasticInferenceServiceSparseEmbeddingsModel createModel(
        String url,
        String modelId,
        Integer maxInputTokens,
        Integer maxBatchSize
    ) {
        return createModel(url, modelId, maxInputTokens, maxBatchSize, ChunkingSettingsBuilder.DEFAULT_SETTINGS);
    }

    public static ElasticInferenceServiceSparseEmbeddingsModel createModel(
        String url,
        String modelId,
        Integer maxInputTokens,
        Integer maxBatchSize,
        ChunkingSettings chunkingSettings
    ) {
        return new ElasticInferenceServiceSparseEmbeddingsModel(
            "id",
            TaskType.SPARSE_EMBEDDING,
            new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, maxInputTokens, maxBatchSize),
            ElasticInferenceServiceComponents.of(url),
            chunkingSettings
        );
    }
}
