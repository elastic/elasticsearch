/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class NvidiaChatCompletionModelTests extends ESTestCase {
    public static NvidiaChatCompletionModel createCompletionModel(@Nullable String url, String apiKey, String modelName) {
        return createModelWithTaskType(url, apiKey, modelName, TaskType.COMPLETION);
    }

    public static NvidiaChatCompletionModel createChatCompletionModel(@Nullable String url, String apiKey, String modelName) {
        return createModelWithTaskType(url, apiKey, modelName, TaskType.CHAT_COMPLETION);
    }

    private static NvidiaChatCompletionModel createModelWithTaskType(
        @Nullable String url,
        String apiKey,
        String modelName,
        TaskType taskType
    ) {
        return new NvidiaChatCompletionModel(
            "inferenceEntityId",
            taskType,
            "service",
            new NvidiaChatCompletionServiceSettings(modelName, url, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public void testThrowsWhenModelIdIsNull() {
        expectThrows(NullPointerException.class, () -> createCompletionModel("url", "secret", null));
    }

    public void testThrowsWhenUrlIsInvalid() {
        var thrownException = expectThrows(IllegalArgumentException.class, () -> createCompletionModel("^^", "secret", "model_name"));
        assertThat(thrownException.getMessage(), containsString("unable to parse url [^^]"));
    }

    public void testOverrideWith_UnifiedCompletionRequest_KeepsSameModelId() {
        var model = createCompletionModel("url", "api_key", "model_name");
        var overriddenModel = NvidiaChatCompletionModel.of(model, "model_name");

        assertThat(overriddenModel, is(sameInstance(model)));
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesExistingModelId() {
        var model = createCompletionModel("url", "api_key", "model_name");
        var overriddenModel = NvidiaChatCompletionModel.of(model, "different_model");

        assertThat(overriddenModel.getServiceSettings().modelId(), is("different_model"));
    }

    public void testOverrideWith_UnifiedCompletionRequest_UsesModelFields_WhenRequestDoesNotOverride() {
        var model = createCompletionModel("url", "api_key", "model_name");
        var overriddenModel = NvidiaChatCompletionModel.of(model, null);

        assertThat(overriddenModel, is(model));
    }
}
