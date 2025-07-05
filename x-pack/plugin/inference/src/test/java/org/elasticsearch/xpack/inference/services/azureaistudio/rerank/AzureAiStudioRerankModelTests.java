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
    private static final String MODEL_ID = "id";
    private static final String TARGET_URI = "http://testtarget.local";
    private static final String API_KEY = "apikey";
    private static final Integer TOP_N = 1;
    private static final Integer TOP_N_OVERRIDE = 2;

    public void testOverrideWith_OverridesWithoutValues() {
        final var model = createModel(
            MODEL_ID,
            TARGET_URI,
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            API_KEY,
            true,
            TOP_N,
            null
        );
        final var requestTaskSettingsMap = getTaskSettingsMap(null, null);
        final var overriddenModel = AzureAiStudioRerankModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, sameInstance(overriddenModel));
    }

    public void testOverrideWith_returnDocuments() {
        final var model = createModel(
            MODEL_ID,
            TARGET_URI,
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            API_KEY,
            true,
            null,
            null
        );
        final var requestTaskSettings = AzureAiStudioRerankTaskSettingsTests.getTaskSettingsMap(false, null);
        final var overriddenModel = AzureAiStudioRerankModel.of(model, requestTaskSettings);

        assertThat(
            overriddenModel,
            is(createModel(MODEL_ID, TARGET_URI, AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, API_KEY, false, null, null))
        );
    }

    public void testOverrideWith_topN() {
        final var model = createModel(
            MODEL_ID,
            TARGET_URI,
            AzureAiStudioProvider.COHERE,
            AzureAiStudioEndpointType.TOKEN,
            API_KEY,
            null,
            TOP_N,
            null
        );
        final var requestTaskSettings = AzureAiStudioRerankTaskSettingsTests.getTaskSettingsMap(null, TOP_N_OVERRIDE);
        final var overriddenModel = AzureAiStudioRerankModel.of(model, requestTaskSettings);
        assertThat(
            overriddenModel,
            is(
                createModel(
                    MODEL_ID,
                    TARGET_URI,
                    AzureAiStudioProvider.COHERE,
                    AzureAiStudioEndpointType.TOKEN,
                    API_KEY,
                    null,
                    TOP_N_OVERRIDE,
                    null
                )
            )
        );
    }

    public void testSetsProperUrlForCohereTokenModel() throws URISyntaxException {
        final var model = createModel(MODEL_ID, TARGET_URI, AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, API_KEY);
        assertThat(model.getEndpointUri().toString(), is(TARGET_URI + "/v1/rerank"));
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
