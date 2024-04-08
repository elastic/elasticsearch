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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionRequestTaskSettingsTests.getChatCompletionRequestTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OpenAiChatCompletionModelTests extends ESTestCase {

    public void testOverrideWith_OverridesUser() {
        var model = createChatCompletionModel("url", "org", "api_key", "model_name", null);
        var requestTaskSettingsMap = getChatCompletionRequestTaskSettingsMap("user_override");

        var overriddenModel = OpenAiChatCompletionModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, is(createChatCompletionModel("url", "org", "api_key", "model_name", "user_override")));
    }

    public void testOverrideWith_EmptyMap() {
        var model = createChatCompletionModel("url", "org", "api_key", "model_name", null);

        var requestTaskSettingsMap = Map.<String, Object>of();

        var overriddenModel = OpenAiChatCompletionModel.of(model, requestTaskSettingsMap);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_NullMap() {
        var model = createChatCompletionModel("url", "org", "api_key", "model_name", null);

        var overriddenModel = OpenAiChatCompletionModel.of(model, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public static OpenAiChatCompletionModel createChatCompletionModel(
        String url,
        @Nullable String org,
        String apiKey,
        String modelName,
        @Nullable String user
    ) {
        return new OpenAiChatCompletionModel(
            "id",
            TaskType.COMPLETION,
            "service",
            new OpenAiChatCompletionServiceSettings(new OpenAiServiceSettings(modelName, url, org), null),
            new OpenAiChatCompletionTaskSettings(user),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

}
