/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionRequestTaskSettingsTests.getChatCompletionRequestTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class LlamaChatCompletionModelTests extends ESTestCase {

    public void testOverrideWith_OverridesExistingUser() {
        var model = createCompletionModel("model_name", "url", "api_key", "user");
        var requestTaskSettingsMap = getChatCompletionRequestTaskSettingsMap("user_override");

        var overriddenModel = LlamaChatCompletionModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, is(createCompletionModel("model_name", "url", "api_key", "user_override")));
    }

    public void testOverrideWith_OverridesNullUser() {
        var model = createCompletionModel("model_name", "url", "api_key", null);
        var requestTaskSettingsMap = getChatCompletionRequestTaskSettingsMap("user_override");

        var overriddenModel = LlamaChatCompletionModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, is(createCompletionModel("model_name", "url", "api_key", "user_override")));
    }

    public void testOverrideWith_EmptyMap() {
        var model = createCompletionModel("model_name", "url", "api_key", null);

        var requestTaskSettingsMap = Map.<String, Object>of();

        var overriddenModel = LlamaChatCompletionModel.of(model, requestTaskSettingsMap);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_NullMap() {
        var model = createCompletionModel("model_name", "url", "api_key", null);

        var overriddenModel = LlamaChatCompletionModel.of(model, (Map<String, Object>) null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public static LlamaChatCompletionModel createCompletionModel(String modelId, String url, String apiKey, String user) {
        return new LlamaChatCompletionModel(
            "id",
            TaskType.COMPLETION,
            "llama",
            new LlamaChatCompletionServiceSettings(modelId, url, null),
            new OpenAiChatCompletionTaskSettings(user),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static LlamaChatCompletionModel createChatCompletionModel(String modelId, String url, String apiKey, String user) {
        return new LlamaChatCompletionModel(
            "id",
            TaskType.CHAT_COMPLETION,
            "llama",
            new LlamaChatCompletionServiceSettings(modelId, url, null),
            new OpenAiChatCompletionTaskSettings(user),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static LlamaChatCompletionModel createChatCompletionModelNoAuth(String modelId, String url, String user) {
        return new LlamaChatCompletionModel(
            "id",
            TaskType.CHAT_COMPLETION,
            "llama",
            new LlamaChatCompletionServiceSettings(modelId, url, null),
            new OpenAiChatCompletionTaskSettings(user),
            EmptySecretSettings.INSTANCE
        );
    }

    public void testOverrideWith_UnifiedCompletionRequest_KeepsSameModelId() {
        var model = createCompletionModel("model_name", "url", "api_key", "user");
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "role", null, null)),
            "model_name",
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = LlamaChatCompletionModel.of(model, request);

        assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesExistingModelId() {
        var model = createCompletionModel("model_name", "url", "api_key", "user");
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

        var overriddenModel = LlamaChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("different_model"));
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesNullModelId() {
        var model = createCompletionModel(null, "url", "api_key", "user");
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

        var overriddenModel = LlamaChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("different_model"));
    }

    public void testOverrideWith_UnifiedCompletionRequest_KeepsNullIfNoModelIdProvided() {
        var model = createCompletionModel(null, "url", "api_key", "user");
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "role", null, null)),
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = LlamaChatCompletionModel.of(model, request);

        assertNull(overriddenModel.getServiceSettings().modelId());
    }

    public void testOverrideWith_UnifiedCompletionRequest_UsesModelFields_WhenRequestDoesNotOverride() {
        var model = createCompletionModel("model_name", "url", "api_key", "user");
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

        var overriddenModel = LlamaChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("model_name"));
    }
}
