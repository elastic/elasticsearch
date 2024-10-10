/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

public class GoogleAiStudioEmbeddingsModelTests extends ESTestCase {

    public static GoogleAiStudioEmbeddingsModel createModel(String model, String apiKey, String url) {
        return new GoogleAiStudioEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new GoogleAiStudioEmbeddingsServiceSettings(model, null, null, SimilarityMeasure.DOT_PRODUCT, null),
            EmptyTaskSettings.INSTANCE,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static GoogleAiStudioEmbeddingsModel createModel(String model, ChunkingSettings chunkingSettings, String apiKey, String url) {
        return new GoogleAiStudioEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new GoogleAiStudioEmbeddingsServiceSettings(model, null, null, SimilarityMeasure.DOT_PRODUCT, null),
            EmptyTaskSettings.INSTANCE,
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static GoogleAiStudioEmbeddingsModel createModel(
        String url,
        String model,
        String apiKey,
        Integer dimensions,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        return new GoogleAiStudioEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new GoogleAiStudioEmbeddingsServiceSettings(model, null, dimensions, similarityMeasure, null),
            EmptyTaskSettings.INSTANCE,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static GoogleAiStudioEmbeddingsModel createModel(
        String model,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions
    ) {
        return new GoogleAiStudioEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new GoogleAiStudioEmbeddingsServiceSettings(model, tokenLimit, dimensions, SimilarityMeasure.DOT_PRODUCT, null),
            EmptyTaskSettings.INSTANCE,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

}
