/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URISyntaxException;

import static org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureAiStudioChatCompletionModelTests extends ESTestCase {

    public void testOverrideWith_OverridesWithoutValues() {
        var model = createModel(
            "id",
            "target",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            1.0,
            2.0,
            false,
            512,
            null
        );
        var requestTaskSettingsMap = getTaskSettingsMap(null, null, null, null);
        var overriddenModel = AzureAiStudioChatCompletionModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, sameInstance(overriddenModel));
    }

    public void testOverrideWith_temperature() {
        var model = createModel(
            "id",
            "target",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            1.0,
            null,
            null,
            null,
            null
        );
        var requestTaskSettings = getTaskSettingsMap(0.5, null, null, null);
        var overriddenModel = AzureAiStudioChatCompletionModel.of(model, requestTaskSettings);
        assertThat(
            overriddenModel,
            is(
                createModel(
                    "id",
                    "target",
                    AzureAiStudioProvider.OPENAI,
                    AzureAiStudioEndpointType.TOKEN,
                    "apikey",
                    0.5,
                    null,
                    null,
                    null,
                    null
                )
            )
        );
    }

    public void testOverrideWith_topP() {
        var model = createModel(
            "id",
            "target",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            null,
            0.8,
            null,
            null,
            null
        );
        var requestTaskSettings = getTaskSettingsMap(null, 0.5, null, null);
        var overriddenModel = AzureAiStudioChatCompletionModel.of(model, requestTaskSettings);
        assertThat(
            overriddenModel,
            is(
                createModel(
                    "id",
                    "target",
                    AzureAiStudioProvider.OPENAI,
                    AzureAiStudioEndpointType.TOKEN,
                    "apikey",
                    null,
                    0.5,
                    null,
                    null,
                    null
                )
            )
        );
    }

    public void testOverrideWith_doSample() {
        var model = createModel(
            "id",
            "target",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            null,
            null,
            true,
            null,
            null
        );
        var requestTaskSettings = getTaskSettingsMap(null, null, false, null);
        var overriddenModel = AzureAiStudioChatCompletionModel.of(model, requestTaskSettings);
        assertThat(
            overriddenModel,
            is(
                createModel(
                    "id",
                    "target",
                    AzureAiStudioProvider.OPENAI,
                    AzureAiStudioEndpointType.TOKEN,
                    "apikey",
                    null,
                    null,
                    false,
                    null,
                    null
                )
            )
        );
    }

    public void testOverrideWith_maxNewTokens() {
        var model = createModel(
            "id",
            "target",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            null,
            null,
            null,
            512,
            null
        );
        var requestTaskSettings = getTaskSettingsMap(null, null, null, 128);
        var overriddenModel = AzureAiStudioChatCompletionModel.of(model, requestTaskSettings);
        assertThat(
            overriddenModel,
            is(
                createModel(
                    "id",
                    "target",
                    AzureAiStudioProvider.OPENAI,
                    AzureAiStudioEndpointType.TOKEN,
                    "apikey",
                    null,
                    null,
                    null,
                    128,
                    null
                )
            )
        );
    }

    public void testSetsProperUrlForOpenAITokenModel() throws URISyntaxException {
        var model = createModel("id", "http://testtarget.local", AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey");
        assertThat(model.getEndpointUri().toString(), is("http://testtarget.local"));
    }

    public void testSetsProperUrlForNonOpenAiTokenModel() throws URISyntaxException {
        var model = createModel("id", "http://testtarget.local", AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, "apikey");
        assertThat(model.getEndpointUri().toString(), is("http://testtarget.local/v1/chat/completions"));
    }

    public void testSetsProperUrlForRealtimeEndpointModel() throws URISyntaxException {
        var model = createModel(
            "id",
            "http://testtarget.local",
            AzureAiStudioProvider.MISTRAL,
            AzureAiStudioEndpointType.REALTIME,
            "apikey"
        );
        assertThat(model.getEndpointUri().toString(), is("http://testtarget.local"));
    }

    public static AzureAiStudioChatCompletionModel createModel(
        String id,
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey
    ) {
        return createModel(id, target, provider, endpointType, apiKey, null, null, null, null, null);
    }

    public static AzureAiStudioChatCompletionModel createModel(
        String id,
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey,
        @Nullable Double temperature,
        @Nullable Double topP,
        @Nullable Boolean doSample,
        @Nullable Integer maxNewTokens,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        return new AzureAiStudioChatCompletionModel(
            id,
            TaskType.COMPLETION,
            "azureaistudio",
            new AzureAiStudioChatCompletionServiceSettings(target, provider, endpointType, rateLimitSettings),
            new AzureAiStudioChatCompletionTaskSettings(temperature, topP, doSample, maxNewTokens),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
