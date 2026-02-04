/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class Ai21ChatCompletionModelTests extends ESTestCase {

    public static Ai21ChatCompletionModel createCompletionModel(String apiKey, String modelId) {
        return new Ai21ChatCompletionModel(
            "id",
            TaskType.COMPLETION,
            "service",
            new Ai21ChatCompletionServiceSettings(modelId, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static Ai21ChatCompletionModel createCompletionModel(String url, String apiKey, String modelId) {
        Ai21ChatCompletionModel ai21ChatCompletionModel = new Ai21ChatCompletionModel(
            "id",
            TaskType.COMPLETION,
            "service",
            new Ai21ChatCompletionServiceSettings(modelId, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
        ai21ChatCompletionModel.setURI(url);
        return ai21ChatCompletionModel;
    }

    public static Ai21ChatCompletionModel createChatCompletionModel(String apiKey, String modelId) {
        return new Ai21ChatCompletionModel(
            "id",
            TaskType.CHAT_COMPLETION,
            "service",
            new Ai21ChatCompletionServiceSettings(modelId, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static Ai21ChatCompletionModel createChatCompletionModel(String url, String apiKey, String modelId) {
        Ai21ChatCompletionModel ai21ChatCompletionModel = new Ai21ChatCompletionModel(
            "id",
            TaskType.CHAT_COMPLETION,
            "service",
            new Ai21ChatCompletionServiceSettings(modelId, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
        ai21ChatCompletionModel.setURI(url);
        return ai21ChatCompletionModel;
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesExistingModelId() {
        var model = createCompletionModel("api_key", "model_name");
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

        var overriddenModel = Ai21ChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("different_model"));
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesNullModelId() {
        var model = createCompletionModel("api_key", null);
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

        var overriddenModel = Ai21ChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("different_model"));
    }

    public void testOverrideWith_UnifiedCompletionRequest_KeepsNullIfNoModelIdProvided() {
        var model = createCompletionModel("api_key", null);
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

        var overriddenModel = Ai21ChatCompletionModel.of(model, request);

        assertNull(overriddenModel.getServiceSettings().modelId());
    }

    public void testOverrideWith_UnifiedCompletionRequest_UsesModelFields_WhenRequestDoesNotOverride() {
        var model = createCompletionModel("api_key", "model_name");
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

        var overriddenModel = Ai21ChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("model_name"));
    }
}
