/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

public class NvidiaEmbeddingsModelTests extends ESTestCase {
    public static NvidiaEmbeddingsModel createModel(String url, String apiKey, @Nullable String modelId) {
        return createModel(url, apiKey, modelId, 1234);
    }

    public static NvidiaEmbeddingsModel createModel(String url, String apiKey, @Nullable String modelId, int maxInputTokens) {
        return createModel(url, apiKey, modelId, maxInputTokens, 1536);
    }

    public static NvidiaEmbeddingsModel createModel(
        @Nullable String url,
        String apiKey,
        @Nullable String modelId,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions
    ) {
        return createModel(url, apiKey, modelId, maxInputTokens, dimensions, InputType.SEARCH, CohereTruncation.NONE);
    }

    public static NvidiaEmbeddingsModel createModel(
        @Nullable String url,
        String apiKey,
        @Nullable String modelId,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions,
        @Nullable InputType inputType,
        @Nullable CohereTruncation truncation
    ) {
        return new NvidiaEmbeddingsModel(
            "inferenceEntityId",
            TaskType.TEXT_EMBEDDING,
            "service",
            new NvidiaEmbeddingsServiceSettings(modelId, url, dimensions, SimilarityMeasure.DOT_PRODUCT, maxInputTokens, null),
            new NvidiaEmbeddingsTaskSettings(inputType, truncation),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

}
