/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.completions;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionModel;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class MistralChatCompletionModelTests extends ESTestCase {

    public static MistralChatCompletionModel createCompletionModel(String apiKey, String modelId) {
        return new MistralChatCompletionModel(
            "id",
            TaskType.COMPLETION,
            "service",
            new MistralChatCompletionServiceSettings(modelId, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static MistralChatCompletionModel createChatCompletionModel(String apiKey, String modelId) {
        return new MistralChatCompletionModel(
            "id",
            TaskType.CHAT_COMPLETION,
            "service",
            new MistralChatCompletionServiceSettings(modelId, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
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

        var overriddenModel = MistralChatCompletionModel.of(model, request);

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

        var overriddenModel = MistralChatCompletionModel.of(model, request);

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

        var overriddenModel = MistralChatCompletionModel.of(model, request);

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

        var overriddenModel = MistralChatCompletionModel.of(model, request);

        assertThat(overriddenModel.getServiceSettings().modelId(), is("model_name"));
    }
}
