/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.AzureOpenAiSecretSettings;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsTaskSettingsTests.getAzureOpenAiRequestTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureOpenAiEmbeddingsModelTests extends ESTestCase {

    public void testOverrideWith_OverridesUser() {
        var model = createModel("resource", "deployment", "apiversion", null, "api_key", null, "id");
        var requestTaskSettingsMap = getAzureOpenAiRequestTaskSettingsMap("user_override");

        var overriddenModel = AzureOpenAiEmbeddingsModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, is(createModel("resource", "deployment", "apiversion", "user_override", "api_key", null, "id")));
    }

    public void testOverrideWith_EmptyMap() {
        var model = createModel("resource", "deployment", "apiversion", null, "api_key", null, "id");

        var requestTaskSettingsMap = Map.<String, Object>of();

        var overriddenModel = AzureOpenAiEmbeddingsModel.of(model, requestTaskSettingsMap);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_NullMap() {
        var model = createModel("resource", "deployment", "apiversion", null, "api_key", null, "id");

        var overriddenModel = AzureOpenAiEmbeddingsModel.of(model, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public static AzureOpenAiEmbeddingsModel createModel(
        String resourceName,
        String deploymentId,
        String apiVersion,
        String user,
        @Nullable String apiKey,
        @Nullable String entraId,
        String inferenceEntityId
    ) {
        var secureApiKey = apiKey != null ? new SecureString(apiKey.toCharArray()) : null;
        var secureEntraId = entraId != null ? new SecureString(entraId.toCharArray()) : null;
        return new AzureOpenAiEmbeddingsModel(
            inferenceEntityId,
            TaskType.TEXT_EMBEDDING,
            "service",
            new AzureOpenAiEmbeddingsServiceSettings(resourceName, deploymentId, apiVersion, null, false, null, false, null),
            new AzureOpenAiEmbeddingsTaskSettings(user),
            new AzureOpenAiSecretSettings(secureApiKey, secureEntraId)
        );
    }

    public static AzureOpenAiEmbeddingsModel createModel(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        Boolean dimensionsSetByUser,
        @Nullable String encodingFormat,
        Boolean encodingFormatSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable String user,
        @Nullable String apiKey,
        @Nullable String entraId,
        String inferenceEntityId
    ) {
        var secureApiKey = apiKey != null ? new SecureString(apiKey.toCharArray()) : null;
        var secureEntraId = entraId != null ? new SecureString(entraId.toCharArray()) : null;

        return new AzureOpenAiEmbeddingsModel(
            inferenceEntityId,
            TaskType.TEXT_EMBEDDING,
            "service",
            new AzureOpenAiEmbeddingsServiceSettings(
                resourceName,
                deploymentId,
                apiVersion,
                dimensions,
                dimensionsSetByUser,
                encodingFormat,
                encodingFormatSetByUser,
                maxInputTokens
            ),
            new AzureOpenAiEmbeddingsTaskSettings(user),
            new AzureOpenAiSecretSettings(secureApiKey, secureEntraId)
        );
    }

    /*
    public static AzureOpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user
    ) {
        return new AzureOpenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, 1536, null, false),
            new OpenAiEmbeddingsTaskSettings(user),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static AzureOpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        @Nullable Integer tokenLimit
    ) {
        return new AzureOpenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, 1536, tokenLimit, false),
            new OpenAiEmbeddingsTaskSettings(user),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static AzureOpenAiEmbeddingsModel createModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions
    ) {
        return new AzureOpenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, SimilarityMeasure.DOT_PRODUCT, dimensions, tokenLimit, false),
            new OpenAiEmbeddingsTaskSettings(user),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static AzureOpenAiEmbeddingsModel createModel(
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
        return new AzureOpenAiEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenAiEmbeddingsServiceSettings(modelName, url, org, similarityMeasure, dimensions, tokenLimit, dimensionsSetByUser),
            new OpenAiEmbeddingsTaskSettings(user),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
     */
}
