/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiEntraIdApiKeySecretsTests;
import org.junit.After;
import org.junit.Before;

import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiTaskSettingsTests.createRequestTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureOpenAiEmbeddingsModelTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() throws Exception {
        terminate(threadPool);
    }

    public void testOverrideWith_OverridesUser() {
        var model = createModel("resource", "deployment", "apiversion", null, "api_key", null, "id", threadPool);
        var requestTaskSettingsMap = createRequestTaskSettingsMap("user_override");

        var overriddenModel = AzureOpenAiEmbeddingsModel.of(model, requestTaskSettingsMap);

        assertThat(
            overriddenModel,
            is(createModel("resource", "deployment", "apiversion", "user_override", "api_key", null, "id", threadPool))
        );
    }

    public void testOverrideWith_EmptyMap() {
        var model = createModel("resource", "deployment", "apiversion", null, "api_key", null, "id", threadPool);

        var requestTaskSettingsMap = Map.<String, Object>of();

        var overriddenModel = AzureOpenAiEmbeddingsModel.of(model, requestTaskSettingsMap);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_NullMap() {
        var model = createModel("resource", "deployment", "apiversion", null, "api_key", null, "id", threadPool);

        var overriddenModel = AzureOpenAiEmbeddingsModel.of(model, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testCreateModel_FromUpdatedServiceSettings() {
        var model = createModel("resource", "deployment", "apiversion", "user", "api_key", null, "id", threadPool);
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

        assertThat(
            overridenModel,
            is(createModel("resource", "deployment", "override_apiversion", "user", "api_key", null, "id", threadPool))
        );
    }

    public void testBuildUriString() throws URISyntaxException {
        var resource = "resource";
        var deploymentId = "deployment";
        var apiKey = "api key";
        var user = "user";
        var entraId = "entra id";
        var inferenceEntityId = "inference entity id";
        var apiVersion = "2024";

        var model = createModel(resource, deploymentId, apiVersion, user, apiKey, entraId, inferenceEntityId, threadPool);

        assertThat(
            model.buildUriString().toString(),
            is("https://resource.openai.azure.com/openai/deployments/deployment/embeddings?api-version=2024")
        );
    }

    public static AzureOpenAiEmbeddingsModel createModelWithRandomValues(ThreadPool threadPool) {
        return createModel(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            threadPool
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
        String inferenceEntityId,
        ThreadPool threadPool
    ) {
        var secretSettings = AzureOpenAiEntraIdApiKeySecretsTests.createSecret(apiKey, entraId);
        var userToUse = user == null ? StatefulValue.<String>undefined() : StatefulValue.of(user);

        return new AzureOpenAiEmbeddingsModel(
            inferenceEntityId,
            TaskType.TEXT_EMBEDDING,
            "service",
            new AzureOpenAiEmbeddingsServiceSettings(resourceName, deploymentId, apiVersion, null, false, null, null, null),
            new AzureOpenAiEmbeddingsTaskSettings(userToUse, Headers.UNDEFINED_INSTANCE),
            chunkingSettings,
            secretSettings,
            threadPool
        );
    }

    public static AzureOpenAiEmbeddingsModel createModel(
        String resourceName,
        String deploymentId,
        String apiVersion,
        String user,
        @Nullable String apiKey,
        @Nullable String entraId,
        String inferenceEntityId,
        ThreadPool threadPool
    ) {
        var secretSettings = AzureOpenAiEntraIdApiKeySecretsTests.createSecret(apiKey, entraId);
        var userToUse = user == null ? StatefulValue.<String>undefined() : StatefulValue.of(user);

        return new AzureOpenAiEmbeddingsModel(
            inferenceEntityId,
            TaskType.TEXT_EMBEDDING,
            "service",
            new AzureOpenAiEmbeddingsServiceSettings(resourceName, deploymentId, apiVersion, null, false, null, null, null),
            new AzureOpenAiEmbeddingsTaskSettings(userToUse, Headers.UNDEFINED_INSTANCE),
            null,
            secretSettings,
            threadPool
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
        String inferenceEntityId,
        ThreadPool threadPool
    ) {
        var secretSettings = AzureOpenAiEntraIdApiKeySecretsTests.createSecret(apiKey, entraId);
        var userToUse = user == null ? StatefulValue.<String>undefined() : StatefulValue.of(user);

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
            new AzureOpenAiEmbeddingsTaskSettings(userToUse, Headers.UNDEFINED_INSTANCE),
            null,
            secretSettings,
            threadPool
        );
    }
}
