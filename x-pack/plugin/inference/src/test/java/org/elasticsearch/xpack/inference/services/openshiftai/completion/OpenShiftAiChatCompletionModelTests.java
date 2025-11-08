/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class OpenShiftAiChatCompletionModelTests extends ESTestCase {

    private static final String MODEL_ID = "model_name";
    private static final String API_KEY = "api_key";
    private static final String URL = "some_url";

    public static OpenShiftAiChatCompletionModel createCompletionModel(String url, String apiKey, String modelName) {
        return createModelWithTaskType(url, apiKey, modelName, TaskType.COMPLETION);
    }

    public static OpenShiftAiChatCompletionModel createChatCompletionModel(String url, String apiKey, String modelName) {
        return createModelWithTaskType(url, apiKey, modelName, TaskType.CHAT_COMPLETION);
    }

    public static OpenShiftAiChatCompletionModel createModelWithTaskType(String url, String apiKey, String modelName, TaskType taskType) {
        return new OpenShiftAiChatCompletionModel(
            "inferenceEntityId",
            taskType,
            "service",
            new OpenShiftAiChatCompletionServiceSettings(modelName, url, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public void testOverrideWith_UnifiedCompletionRequest_KeepsSameModelId() {
        var model = createCompletionModel(URL, API_KEY, MODEL_ID);
        var overriddenModel = OpenShiftAiChatCompletionModel.of(model, MODEL_ID);

        assertThat(overriddenModel, is(sameInstance(model)));
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesExistingModelId() {
        var model = createCompletionModel(URL, API_KEY, MODEL_ID);
        var overriddenModel = OpenShiftAiChatCompletionModel.of(model, "different_model");

        assertThat(overriddenModel.getServiceSettings().modelId(), is("different_model"));
    }

    public void testOverrideWith_UnifiedCompletionRequest_UsesModelFields_WhenRequestDoesNotOverride() {
        var model = createCompletionModel(URL, API_KEY, MODEL_ID);
        var overriddenModel = OpenShiftAiChatCompletionModel.of(model, null);

        assertThat(overriddenModel, is(sameInstance(model)));
    }

    public void testOverrideWith_UnifiedCompletionRequest_KeepsNullIfNoModelIdProvided() {
        var model = createCompletionModel(URL, API_KEY, null);
        var overriddenModel = OpenShiftAiChatCompletionModel.of(model, null);

        assertThat(overriddenModel, is(sameInstance(model)));
    }
}
