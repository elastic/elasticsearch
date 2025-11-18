/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class NvidiaChatCompletionModelTests extends ESTestCase {

    private static final String MODEL_VALUE = "model_name";
    private static final String API_KEY_VALUE = "test_api_key";
    private static final String URL_VALUE = "http://www.abc.com";
    private static final String INVALID_URL_VALUE = "^^^";
    private static final String ALTERNATE_MODEL_VALUE = "different_model";

    public static NvidiaChatCompletionModel createCompletionModel(String url, String apiKey, String modelName) {
        return createModelWithTaskType(url, apiKey, modelName, TaskType.COMPLETION);
    }

    public static NvidiaChatCompletionModel createChatCompletionModel(String url, String apiKey, String modelName) {
        return createModelWithTaskType(url, apiKey, modelName, TaskType.CHAT_COMPLETION);
    }

    private static NvidiaChatCompletionModel createModelWithTaskType(String url, String apiKey, String modelName, TaskType taskType) {
        return new NvidiaChatCompletionModel(
            "inferenceEntityId",
            taskType,
            "service",
            new NvidiaChatCompletionServiceSettings(modelName, url, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public void testOverrideWith_UnifiedCompletionRequest_KeepsSameModelId() {
        var model = createCompletionModel(URL_VALUE, API_KEY_VALUE, MODEL_VALUE);
        var overriddenModel = NvidiaChatCompletionModel.of(model, MODEL_VALUE);

        assertThat(overriddenModel, is(sameInstance(model)));
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesExistingModelId() {
        var model = createCompletionModel(URL_VALUE, API_KEY_VALUE, MODEL_VALUE);
        var overriddenModel = NvidiaChatCompletionModel.of(model, ALTERNATE_MODEL_VALUE);

        assertThat(overriddenModel.getServiceSettings().modelId(), is(ALTERNATE_MODEL_VALUE));
    }

    public void testOverrideWith_UnifiedCompletionRequest_UsesModelFields_WhenRequestDoesNotOverride() {
        var model = createCompletionModel(URL_VALUE, API_KEY_VALUE, MODEL_VALUE);
        var overriddenModel = NvidiaChatCompletionModel.of(model, null);

        assertThat(overriddenModel, is(sameInstance(model)));
    }

    public void testCreateModel_NoModelId_ThrowsException() {
        expectThrows(NullPointerException.class, () -> createCompletionModel(URL_VALUE, API_KEY_VALUE, null));
    }

    public void testCreateModel_NoUrl_DefaultUrl() {
        var model = createCompletionModel(null, API_KEY_VALUE, MODEL_VALUE);

        assertThat(model.getServiceSettings().uri(), is(nullValue()));
    }

    public void testCreateModel_InvalidUrl_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> createCompletionModel(INVALID_URL_VALUE, API_KEY_VALUE, MODEL_VALUE)
        );
        assertThat(thrownException.getMessage(), containsString("unable to parse url [^^^]"));
    }
}
