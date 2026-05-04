/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsRequestTaskSettingsTests.createRequestTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OpenAiEmbeddingsModelTests extends ESTestCase {

    public void testOverrideWith_OverridesUser() {
        var model = createModel("url", "org", "api_key", "model_name", null, TaskType.TEXT_EMBEDDING);
        var requestTaskSettingsMap = createRequestTaskSettingsMap("user_override");

        var overriddenModel = OpenAiEmbeddingsModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, is(createModel("url", "org", "api_key", "model_name", "user_override", TaskType.TEXT_EMBEDDING)));
    }

    public void testOverrideWith_EmptyMap() {
        var model = createModel("url", "org", "api_key", "model_name", null, TaskType.TEXT_EMBEDDING);

        var requestTaskSettingsMap = Map.<String, Object>of();

        var overriddenModel = OpenAiEmbeddingsModel.of(model, requestTaskSettingsMap);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_NullMap() {
        var model = createModel("url", "org", "api_key", "model_name", null, TaskType.TEXT_EMBEDDING);

        var overriddenModel = OpenAiEmbeddingsModel.of(model, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public static OpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        String inferenceEntityId,
        TaskType taskType
    ) {
        return new OpenAiEmbeddingsModel(
            inferenceEntityId,
            taskType,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, 1536, null, false, null),
            new OpenAiEmbeddingsTaskSettings(user, null),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static OpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        ChunkingSettings chunkingSettings,
        TaskType taskType
    ) {
        return new OpenAiEmbeddingsModel(
            "id",
            taskType,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, 1536, null, false, null),
            new OpenAiEmbeddingsTaskSettings(user, null),
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static OpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        TaskType taskType
    ) {
        return new OpenAiEmbeddingsModel(
            "id",
            taskType,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, 1536, null, false, null),
            new OpenAiEmbeddingsTaskSettings(user, null),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static OpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        @Nullable Integer tokenLimit,
        TaskType taskType
    ) {
        return new OpenAiEmbeddingsModel(
            "id",
            taskType,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, 1536, tokenLimit, false, null),
            new OpenAiEmbeddingsTaskSettings(user, null),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static OpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        TaskType taskType
    ) {
        return new OpenAiEmbeddingsModel(
            "id",
            taskType,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, dimensions, tokenLimit, false, null),
            new OpenAiEmbeddingsTaskSettings(user, null),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static OpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        @Nullable SimilarityMeasure similarityMeasure,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        TaskType taskType
    ) {
        return new OpenAiEmbeddingsModel(
            "id",
            taskType,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, similarityMeasure, dimensions, tokenLimit, dimensionsSetByUser, null),
            new OpenAiEmbeddingsTaskSettings(user, null),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
