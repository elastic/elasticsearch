/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class OpenShiftAiChatCompletionModelTests extends ESTestCase {
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
        var model = createCompletionModel("url", "api_key", "model_name");
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "role", null, null)),
            "model_name", // same model
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = OpenShiftAiChatCompletionModel.of(model, request);

        assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesExistingModelId() {
        var model = createCompletionModel("url", "api_key", "model_name");
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "role", null, null)),
            "different_model", // overriding model
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = OpenShiftAiChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("different_model"));
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesNullModelId() {
        var model = createCompletionModel("url", "api_key", null);
        var request = new UnifiedCompletionRequest(
            List.of(new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString("hello"), "role", null, null)),
            "different_model", // overriding model
            null,
            null,
            null,
            null,
            null,
            null
        );

        var overriddenModel = OpenShiftAiChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("different_model"));
    }

    public void testOverrideWith_UnifiedCompletionRequest_KeepsNullIfNoModelIdProvided() {
        var model = createCompletionModel("url", "api_key", null);
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

        var overriddenModel = OpenShiftAiChatCompletionModel.of(model, request);

        assertNull(overriddenModel.getServiceSettings().modelId());
    }

    public void testOverrideWith_UnifiedCompletionRequest_UsesModelFields_WhenRequestDoesNotOverride() {
        var model = createCompletionModel("url", "api_key", "model_name");
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

        var overriddenModel = OpenShiftAiChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("model_name"));
    }
}
