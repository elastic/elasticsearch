/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URISyntaxException;

import static org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureAiStudioEmbeddingsModelTests extends ESTestCase {

    public void testOverrideWith_OverridesUser() {
        var model = createModel(
            "id",
            "http://testtarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            null,
            false,
            null,
            null,
            null,
            null
        );

        var requestTaskSettingsMap = getTaskSettingsMap("override_user");
        var overriddenModel = AzureAiStudioEmbeddingsModel.of(model, requestTaskSettingsMap);

        assertThat(
            overriddenModel,
            is(
                createModel(
                    "id",
                    "http://testtarget.local",
                    AzureAiStudioProvider.OPENAI,
                    AzureAiStudioEndpointType.TOKEN,
                    "apikey",
                    null,
                    false,
                    null,
                    null,
                    "override_user",
                    null
                )
            )
        );
    }

    public void testOverrideWith_OverridesWithoutValues() {
        var model = createModel(
            "id",
            "http://testtarget.local",
            AzureAiStudioProvider.OPENAI,
            AzureAiStudioEndpointType.TOKEN,
            "apikey",
            null,
            false,
            null,
            null,
            null,
            null
        );

        var requestTaskSettingsMap = getTaskSettingsMap(null);
        var overriddenModel = AzureAiStudioEmbeddingsModel.of(model, requestTaskSettingsMap);

        assertThat(overriddenModel, sameInstance(overriddenModel));
    }

    public void testSetsProperUrlForOpenAIModel() throws URISyntaxException {
        var model = createModel("id", "http://testtarget.local", AzureAiStudioProvider.OPENAI, AzureAiStudioEndpointType.TOKEN, "apikey");
        assertThat(model.getEndpointUri().toString(), is("http://testtarget.local"));
    }

    public void testSetsProperUrlForCohereModel() throws URISyntaxException {
        var model = createModel("id", "http://testtarget.local", AzureAiStudioProvider.COHERE, AzureAiStudioEndpointType.TOKEN, "apikey");
        assertThat(model.getEndpointUri().toString(), is("http://testtarget.local/v1/embeddings"));
    }

    public static AzureAiStudioEmbeddingsModel createModel(
        String inferenceId,
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey
    ) {
        return createModel(inferenceId, target, provider, endpointType, apiKey, null, false, null, null, null, null);
    }

    public static AzureAiStudioEmbeddingsModel createModel(
        String inferenceId,
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        ChunkingSettings chunkingSettings,
        String apiKey
    ) {
        return createModel(inferenceId, target, provider, endpointType, chunkingSettings, apiKey, null, false, null, null, null, null);
    }

    public static AzureAiStudioEmbeddingsModel createModel(
        String inferenceId,
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        ChunkingSettings chunkingSettings,
        String apiKey,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable String user,
        RateLimitSettings rateLimitSettings
    ) {
        return new AzureAiStudioEmbeddingsModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            "azureaistudio",
            new AzureAiStudioEmbeddingsServiceSettings(
                target,
                provider,
                endpointType,
                dimensions,
                dimensionsSetByUser,
                maxTokens,
                similarity,
                rateLimitSettings
            ),
            new AzureAiStudioEmbeddingsTaskSettings(user),
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static AzureAiStudioEmbeddingsModel createModel(
        String inferenceId,
        String target,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType,
        String apiKey,
        @Nullable Integer dimensions,
        boolean dimensionsSetByUser,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable String user,
        RateLimitSettings rateLimitSettings
    ) {
        return new AzureAiStudioEmbeddingsModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            "azureaistudio",
            new AzureAiStudioEmbeddingsServiceSettings(
                target,
                provider,
                endpointType,
                dimensions,
                dimensionsSetByUser,
                maxTokens,
                similarity,
                rateLimitSettings
            ),
            new AzureAiStudioEmbeddingsTaskSettings(user),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
