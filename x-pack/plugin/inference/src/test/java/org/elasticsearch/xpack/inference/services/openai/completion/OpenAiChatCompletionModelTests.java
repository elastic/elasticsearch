/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionRequestTaskSettingsTests.getChatCompletionRequestTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OpenAiChatCompletionModelTests extends ESTestCase {

    public void testOverrideWith_OverridesUser() {
        var model = createCompletionModel("url", "org", "api_key", "model_name", null);
        var requestTaskSettingsMap = getChatCompletionRequestTaskSettingsMap("user_override");

        var overriddenModel = OpenAiChatCompletionModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, is(createCompletionModel("url", "org", "api_key", "model_name", "user_override")));
    }

    public void testOverrideWith_EmptyMap() {
        var model = createCompletionModel("url", "org", "api_key", "model_name", null);

        var requestTaskSettingsMap = Map.<String, Object>of();

        var overriddenModel = OpenAiChatCompletionModel.of(model, requestTaskSettingsMap);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_NullMap() {
        var model = createCompletionModel("url", "org", "api_key", "model_name", null);

        var overriddenModel = OpenAiChatCompletionModel.of(model, (Map<String, Object>) null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesModelId() {
        var model = createCompletionModel("url", "org", "api_key", "model_name", "user");
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "role", null, null)),
            "different_model",
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertThat(
            OpenAiChatCompletionModel.of(model, request),
            is(createCompletionModel("url", "org", "api_key", "different_model", "user"))
        );
    }

    public void testOverrideWith_UnifiedCompletionRequest_UsesModelFields_WhenRequestDoesNotOverride() {
        var model = createCompletionModel("url", "org", "api_key", "model_name", "user");
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "role", null, null)),
            null, // not overriding model
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertThat(OpenAiChatCompletionModel.of(model, request), is(createCompletionModel("url", "org", "api_key", "model_name", "user")));
    }

    public static OpenAiChatCompletionModel createCompletionModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user
    ) {
        return createModelWithTaskType(url, org, apiKey, modelName, user, TaskType.COMPLETION);
    }

    public static OpenAiChatCompletionModel createChatCompletionModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user
    ) {
        return createModelWithTaskType(url, org, apiKey, modelName, user, TaskType.CHAT_COMPLETION);
    }

    public static OpenAiChatCompletionModel createModelWithTaskType(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        TaskType taskType
    ) {
        return new OpenAiChatCompletionModel(
            "id",
            taskType,
            "service",
            new OpenAiChatCompletionServiceSettings(modelName, url, org, null, null),
            new OpenAiChatCompletionTaskSettings(user),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

}
