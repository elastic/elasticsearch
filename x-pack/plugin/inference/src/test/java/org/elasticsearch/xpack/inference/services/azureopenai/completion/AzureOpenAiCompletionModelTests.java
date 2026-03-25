/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.parser.Headers;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceFields;
import org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiEntraIdApiKeySecretsTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureOpenAiCompletionModelTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    public void testOverrideWith_UpdatedTaskSettings_OverridesUser() {
        var resource = "resource";
        var deploymentId = "deployment";
        var apiVersion = "api version";
        var apiKey = "api key";
        var entraId = "entra id";
        var inferenceEntityId = "inference entity id";

        var user = "user";
        var userOverride = "user override";

        var model = createCompletionModel(resource, deploymentId, apiVersion, user, apiKey, entraId, inferenceEntityId, threadPool);
        var requestTaskSettingsMap = taskSettingsMap(userOverride);
        var overriddenModel = AzureOpenAiCompletionModel.of(model, requestTaskSettingsMap);

        assertThat(
            overriddenModel,
            equalTo(createCompletionModel(resource, deploymentId, apiVersion, userOverride, apiKey, entraId, inferenceEntityId, threadPool))
        );
    }

    public void testOverrideWith_EmptyMap_OverridesNothing() {
        var model = createCompletionModel(
            "resource",
            "deployment",
            "api version",
            "user",
            "api key",
            "entra id",
            "inference entity id",
            threadPool
        );
        var requestTaskSettingsMap = Map.<String, Object>of();
        var overriddenModel = AzureOpenAiCompletionModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_NullMap_OverridesNothing() {
        var model = createCompletionModel(
            "resource",
            "deployment",
            "api version",
            "user",
            "api key",
            "entra id",
            "inference entity id",
            threadPool
        );
        var overriddenModel = AzureOpenAiCompletionModel.of(model, null);

        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_UpdatedServiceSettings_OverridesApiVersion() {
        var resource = "resource";
        var deploymentId = "deployment";
        var apiKey = "api key";
        var user = "user";
        var entraId = "entra id";
        var inferenceEntityId = "inference entity id";

        var apiVersion = "api version";
        var updatedApiVersion = "updated api version";

        var updatedServiceSettings = new AzureOpenAiCompletionServiceSettings(resource, deploymentId, updatedApiVersion, null);

        var model = createCompletionModel(resource, deploymentId, apiVersion, user, apiKey, entraId, inferenceEntityId, threadPool);
        var overriddenModel = new AzureOpenAiCompletionModel(model, updatedServiceSettings);

        assertThat(
            overriddenModel,
            is(createCompletionModel(resource, deploymentId, updatedApiVersion, user, apiKey, entraId, inferenceEntityId, threadPool))
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

        var model = createCompletionModel(resource, deploymentId, apiVersion, user, apiKey, entraId, inferenceEntityId, threadPool);

        assertThat(
            model.buildUriString().toString(),
            is("https://resource.openai.azure.com/openai/deployments/deployment/chat/completions?api-version=2024")
        );
    }

    public static AzureOpenAiCompletionModel createModelWithRandomValues(ThreadPool threadPool) {
        return createCompletionModel(
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

    public static AzureOpenAiCompletionModel createCompletionModel(
        String resourceName,
        String deploymentId,
        String apiVersion,
        String user,
        @Nullable String apiKey,
        @Nullable String entraId,
        String inferenceEntityId,
        ThreadPool threadPool
    ) {
        return createAzureOpenAiModelWithTaskType(
            resourceName,
            deploymentId,
            apiVersion,
            user,
            apiKey,
            entraId,
            inferenceEntityId,
            TaskType.COMPLETION,
            threadPool
        );
    }

    public static AzureOpenAiCompletionModel createChatCompletionModel(
        String resourceName,
        String deploymentId,
        String apiVersion,
        String user,
        @Nullable String apiKey,
        @Nullable String entraId,
        String inferenceEntityId,
        ThreadPool threadPool
    ) {
        return createAzureOpenAiModelWithTaskType(
            resourceName,
            deploymentId,
            apiVersion,
            user,
            apiKey,
            entraId,
            inferenceEntityId,
            TaskType.CHAT_COMPLETION,
            threadPool
        );
    }

    private static AzureOpenAiCompletionModel createAzureOpenAiModelWithTaskType(
        String resourceName,
        String deploymentId,
        String apiVersion,
        String user,
        @Nullable String apiKey,
        @Nullable String entraId,
        String inferenceEntityId,
        TaskType taskType,
        ThreadPool threadPool
    ) {
        var secretSettings = AzureOpenAiEntraIdApiKeySecretsTests.createSecret(apiKey, entraId);
        var userToUse = user == null ? StatefulValue.<String>undefined() : StatefulValue.of(user);

        return new AzureOpenAiCompletionModel(
            inferenceEntityId,
            taskType,
            "service",
            new AzureOpenAiCompletionServiceSettings(resourceName, deploymentId, apiVersion, null),
            new AzureOpenAiCompletionTaskSettings(userToUse, Headers.UNDEFINED_INSTANCE),
            secretSettings,
            threadPool
        );
    }

    private Map<String, Object> taskSettingsMap(String user) {
        Map<String, Object> taskSettingsMap = new HashMap<>();
        taskSettingsMap.put(AzureOpenAiServiceFields.USER, user);
        return taskSettingsMap;
    }

}
