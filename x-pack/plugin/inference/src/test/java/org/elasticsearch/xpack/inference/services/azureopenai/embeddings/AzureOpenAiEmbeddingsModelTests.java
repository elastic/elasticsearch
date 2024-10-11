/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings;

import java.net.URISyntaxException;
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

    public void testCreateModel_FromUpdatedServiceSettings() {
        var model = createModel("resource", "deployment", "apiversion", "user", "api_key", null, "id");
        var updatedSettings = new AzureOpenAiEmbeddingsServiceSettings(
            "resource",
            "deployment",
            "override_apiversion",
            null,
            false,
            null,
            null,
            null
        );

        var overridenModel = new AzureOpenAiEmbeddingsModel(model, updatedSettings);

        assertThat(overridenModel, is(createModel("resource", "deployment", "override_apiversion", "user", "api_key", null, "id")));
    }

    public void testBuildUriString() throws URISyntaxException {
        var resource = "resource";
        var deploymentId = "deployment";
        var apiKey = "api key";
        var user = "user";
        var entraId = "entra id";
        var inferenceEntityId = "inference entity id";
        var apiVersion = "2024";

        var model = createModel(resource, deploymentId, apiVersion, user, apiKey, entraId, inferenceEntityId);

        assertThat(
            model.buildUriString().toString(),
            is("https://resource.openai.azure.com/openai/deployments/deployment/embeddings?api-version=2024")
        );
    }

    public static AzureOpenAiEmbeddingsModel createModelWithRandomValues() {
        return createModel(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
    }

    public static AzureOpenAiEmbeddingsModel createModel(
        String resourceName,
        String deploymentId,
        String apiVersion,
        String user,
        ChunkingSettings chunkingSettings,
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
            new AzureOpenAiEmbeddingsServiceSettings(resourceName, deploymentId, apiVersion, null, false, null, null, null),
            new AzureOpenAiEmbeddingsTaskSettings(user),
            chunkingSettings,
            new AzureOpenAiSecretSettings(secureApiKey, secureEntraId)
        );
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
            new AzureOpenAiEmbeddingsServiceSettings(resourceName, deploymentId, apiVersion, null, false, null, null, null),
            new AzureOpenAiEmbeddingsTaskSettings(user),
            null,
            new AzureOpenAiSecretSettings(secureApiKey, secureEntraId)
        );
    }

    public static AzureOpenAiEmbeddingsModel createModel(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable Integer dimensions,
        Boolean dimensionsSetByUser,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
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
                maxInputTokens,
                similarity,
                null
            ),
            new AzureOpenAiEmbeddingsTaskSettings(user),
            null,
            new AzureOpenAiSecretSettings(secureApiKey, secureEntraId)
        );
    }
}
