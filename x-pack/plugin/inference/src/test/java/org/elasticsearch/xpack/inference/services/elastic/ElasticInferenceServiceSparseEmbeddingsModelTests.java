/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;

public class ElasticInferenceServiceSparseEmbeddingsModelTests extends ESTestCase {

    public static ElasticInferenceServiceSparseEmbeddingsModel createModel(String url, String modelId) {
        return createModel(url, modelId, null);
    }

    public static ElasticInferenceServiceSparseEmbeddingsModel createModel(String url, String modelId, Integer maxInputTokens) {
        return new ElasticInferenceServiceSparseEmbeddingsModel(
            "id",
            TaskType.SPARSE_EMBEDDING,
            "service",
            new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, maxInputTokens, null),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            ElasticInferenceServiceComponents.of(url)
        );
    }
}
