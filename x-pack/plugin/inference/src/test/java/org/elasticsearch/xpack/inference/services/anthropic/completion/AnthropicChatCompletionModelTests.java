/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AnthropicChatCompletionModelTests extends ESTestCase {

    public void testOverrideWith_OverridesMaxInput() {
        var model = createChatCompletionModel("url", "api_key", "model_name", 0);
        var requestTaskSettingsMap = AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, null, null, null);

        var overriddenModel = AnthropicChatCompletionModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, is(createChatCompletionModel("url", "api_key", "model_name", 1)));
    }

    public void testOverrideWith_EmptyMap() {
        var model = createChatCompletionModel("url", "api_key", "model_name", 0);

        var requestTaskSettingsMap = Map.<String, Object>of();

        var overriddenModel = AnthropicChatCompletionModel.of(model, requestTaskSettingsMap);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_NullMap() {
        var model = createChatCompletionModel("url", "api_key", "model_name", 0);

        var overriddenModel = AnthropicChatCompletionModel.of(model, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public static AnthropicChatCompletionModel createChatCompletionModel(String url, String apiKey, String modelName, int maxTokens) {
        return new AnthropicChatCompletionModel(
            "id",
            TaskType.COMPLETION,
            "service",
            url,
            new AnthropicChatCompletionServiceSettings(modelName, null),
            new AnthropicChatCompletionTaskSettings(maxTokens, null, null, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static AnthropicChatCompletionModel createChatCompletionModel(String apiKey, String modelName, int maxTokens) {
        return new AnthropicChatCompletionModel(
            "id",
            TaskType.COMPLETION,
            "service",
            new AnthropicChatCompletionServiceSettings(modelName, null),
            new AnthropicChatCompletionTaskSettings(maxTokens, null, null, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
