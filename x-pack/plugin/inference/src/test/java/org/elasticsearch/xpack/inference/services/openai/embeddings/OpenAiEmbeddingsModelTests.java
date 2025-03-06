/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsRequestTaskSettingsTests.createRequestTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OpenAiEmbeddingsModelTests extends ESTestCase {

    public void testOverrideWith_OverridesUser() {
        var model = createModel("url", "org", "api_key", "model_name", null);
        var requestTaskSettingsMap = createRequestTaskSettingsMap("user_override");

        var overriddenModel = OpenAiEmbeddingsModel.of(model, requestTaskSettingsMap, null);

        assertThat(overriddenModel, is(createModel("url", "org", "api_key", "model_name", "user_override")));
    }

    public void testOverrideWith_EmptyMap() {
        var model = createModel("url", "org", "api_key", "model_name", null);

        var requestTaskSettingsMap = Map.<String, Object>of();

        var overriddenModel = OpenAiEmbeddingsModel.of(model, requestTaskSettingsMap, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_NullMap() {
        var model = createModel("url", "org", "api_key", "model_name", null);

        var overriddenModel = OpenAiEmbeddingsModel.of(model, null, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testThrowsError_WhenInputTypeSpecified() {
        var model = createModel("url", "org", "api_key", "model_name", null);

        var thrownException = expectThrows(ValidationException.class, () -> OpenAiEmbeddingsModel.of(model, Map.of(), InputType.SEARCH));
        assertThat(
            thrownException.getMessage(),
            CoreMatchers.is("Validation Failed: 1: Invalid value [search] received. [input_type] is not allowed;")
        );
    }

    public void testAcceptsInternalInputType() {
        var model = createModel("url", "org", "api_key", "model_name", null);
        var overriddenModel = OpenAiEmbeddingsModel.of(model, Map.of(), InputType.INTERNAL_SEARCH);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(model));
    }

    public void testAcceptsNullInputType() {
        var model = createModel("url", "org", "api_key", "model_name", null);
        var overriddenModel = OpenAiEmbeddingsModel.of(model, Map.of(), null);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(model));
    }

    public static OpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        String inferenceEntityId
    ) {
        return new OpenAiEmbeddingsModel(
            inferenceEntityId,
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, 1536, null, false, null),
            new OpenAiEmbeddingsTaskSettings(user),
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
        ChunkingSettings chunkingSettings
    ) {
        return new OpenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, 1536, null, false, null),
            new OpenAiEmbeddingsTaskSettings(user),
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static OpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user
    ) {
        return new OpenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, 1536, null, false, null),
            new OpenAiEmbeddingsTaskSettings(user),
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
        @Nullable Integer tokenLimit
    ) {
        return new OpenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, 1536, tokenLimit, false, null),
            new OpenAiEmbeddingsTaskSettings(user),
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
        @Nullable Integer dimensions
    ) {
        return new OpenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, dimensions, tokenLimit, false, null),
            new OpenAiEmbeddingsTaskSettings(user),
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
        boolean dimensionsSetByUser
    ) {
        return new OpenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, similarityMeasure, dimensions, tokenLimit, dimensionsSetByUser, null),
            new OpenAiEmbeddingsTaskSettings(user),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
