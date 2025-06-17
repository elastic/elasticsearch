/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URISyntaxException;

import static org.elasticsearch.xpack.inference.services.azureaistudio.rerank.AzureAiStudioRerankTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureAiStudioRerankModelTests extends ESTestCase {

    public void testOverrideWith_OverridesWithoutValues() {
        var model = createModel("id", "target", AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey", true, 2, null);
        var requestTaskSettingsMap = getTaskSettingsMap(null, null);
        var overriddenModel = AzureAiStudioRerankModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, sameInstance(overriddenModel));
    }

    public void testOverrideWith_returnDocuments() {
        var model = createModel("id", "target", AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey", true, null, null);
        var requestTaskSettings = AzureAiStudioRerankTaskSettingsTests.getTaskSettingsMap(false, null);
        var overriddenModel = AzureAiStudioRerankModel.of(model, requestTaskSettings);

        assertThat(
            overriddenModel,
            is(createModel("id", "target", AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey", false, null, null))
        );
    }

    public void testOverrideWith_topN() {
        var model = createModel("id", "target", AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey", null, 2, null);
        var requestTaskSettings = AzureAiStudioRerankTaskSettingsTests.getTaskSettingsMap(null, 1);
        var overriddenModel = AzureAiStudioRerankModel.of(model, requestTaskSettings);
        assertThat(
            overriddenModel,
            is(createModel("id", "target", AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey", null, 1, null))
        );
    }

    public void testSetsProperUrlForOpenAITokenModel() throws URISyntaxException {
        var model = createModel("id", "http://testtarget.local", AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey");
        assertThat(model.getEndpointUri().toString(), is("http://testtarget.local"));
    }

    public void testSetsProperUrlForNonOpenAiTokenModel() throws URISyntaxException {
        var model = createModel("id", "http://testtarget.local", AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, "apikey");
        assertThat(model.getEndpointUri().toString(), is("http://testtarget.local/v1/rerank"));
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

    public static AzureAiStudioRerankModel createModel(
        String id,
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey
    ) {
        return createModel(id, target, provider, endpointType, apiKey, null, null, null);
    }

    public static AzureAiStudioRerankModel createModel(
        String id,
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey,
        @Nullable Boolean returnDocuments,
        @Nullable Integer topN,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        return new AzureAiStudioRerankModel(
            id,
            TaskType.RERANK,
            "azureaistudio",
            new AzureAiStudioRerankServiceSettings(target, provider, endpointType, rateLimitSettings),
            new AzureAiStudioRerankTaskSettings(returnDocuments, topN),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

}
