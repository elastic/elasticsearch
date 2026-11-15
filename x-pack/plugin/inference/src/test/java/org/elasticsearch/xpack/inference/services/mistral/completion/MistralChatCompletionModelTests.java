/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.mistral.MistralConstants;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class MistralChatCompletionModelTests extends ESTestCase {

    private static final URI CUSTOM_URL = URI.create("https://custom.mistral.example.com/v1/chat/completions");

    public static MistralChatCompletionModel createCompletionModel(String apiKey, String modelId) {
        return new MistralChatCompletionModel(
            "id",
            TaskType.COMPLETION,
            "service",
            new MistralChatCompletionServiceSettings(modelId, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static MistralChatCompletionModel createCompletionModel(String url, String apiKey, String modelId) {
        MistralChatCompletionModel mistralChatCompletionModel = new MistralChatCompletionModel(
            "id",
            TaskType.COMPLETION,
            "service",
            new MistralChatCompletionServiceSettings(modelId, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
        mistralChatCompletionModel.setURI(url);
        return mistralChatCompletionModel;
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

    public static MistralChatCompletionModel createChatCompletionModel(String url, String apiKey, String modelId) {
        MistralChatCompletionModel mistralChatCompletionModel = new MistralChatCompletionModel(
            "id",
            TaskType.CHAT_COMPLETION,
            "service",
            new MistralChatCompletionServiceSettings(modelId, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
        mistralChatCompletionModel.setURI(url);
        return mistralChatCompletionModel;
    }

    /**
     * Creates a model whose service settings carry a custom url, as opposed to the helpers above that
     * overwrite the uri of an already built model.
     */
    private static MistralChatCompletionModel createModelWithUrlInServiceSettings(TaskType taskType, String modelId) {
        return new MistralChatCompletionModel(
            "id",
            taskType,
            "service",
            new MistralChatCompletionServiceSettings(modelId, CUSTOM_URL, null),
            new DefaultSecretSettings(new SecureString("api_key".toCharArray()))
        );
    }

    public void testUri_UsesDefaultUrl_WhenServiceSettingsHaveNoUrl() {
        var model = randomBoolean() ? createCompletionModel("api_key", "model_name") : createChatCompletionModel("api_key", "model_name");

        assertThat(model.uri(), is(URI.create(MistralConstants.API_COMPLETIONS_PATH)));
    }

    public void testUri_UsesUrlFromServiceSettings() {
        var model = createModelWithUrlInServiceSettings(randomFrom(TaskType.COMPLETION, TaskType.CHAT_COMPLETION), "model_name");

        assertThat(model.uri(), is(CUSTOM_URL));
    }

    public void testOverrideWith_UnifiedCompletionRequest_KeepsUrlFromServiceSettings() {
        var model = createModelWithUrlInServiceSettings(randomFrom(TaskType.COMPLETION, TaskType.CHAT_COMPLETION), "model_name");
        var request = new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentString("hello"), "role", null, null)),
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
        assertThat(overriddenModel.getServiceSettings().url(), is(CUSTOM_URL));
        assertThat(overriddenModel.uri(), is(CUSTOM_URL));
    }

    public void testOverrideWith_UnifiedCompletionRequest_OverridesExistingModelId() {
        var model = createCompletionModel("api_key", "model_name");
        var request = new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentString("hello"), "role", null, null)),
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
        var request = new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentString("hello"), "role", null, null)),
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
        var request = new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentString("hello"), "role", null, null)),
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
        var request = new UnifiedCompletionRequestBody(
            List.of(new Message(new ContentString("hello"), "role", null, null)),
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
