/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIService.VALID_INPUT_TYPE_VALUES;
import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class JinaAIEmbeddingsModelTests extends ESTestCase {

    public void testConstructor_usesDefaultUrlWhenNull() {
        var model = createModel(null, randomAlphaOfLength(10), randomAlphaOfLength(10));
        assertThat(model.uri().toString(), is("https://api.jina.ai/v1/embeddings"));
    }

    public void testConstructor_usesUrlWhenSpecified() {
        String url = "some_URL";
        var model = createModel(url, randomAlphaOfLength(10), randomAlphaOfLength(10));
        assertThat(model.uri().toString(), is(url));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEmpty() {
        var model = createModel(
            null,
            "modelName",
            new JinaAIEmbeddingsTaskSettings(randomFrom(VALID_INPUT_TYPE_VALUES), randomBoolean()),
            "api_key"
        );

        var overriddenModel = JinaAIEmbeddingsModel.of(model, Map.of());
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreNull() {
        var model = createModel(
            null,
            "modelName",
            new JinaAIEmbeddingsTaskSettings(randomFrom(VALID_INPUT_TYPE_VALUES), randomBoolean()),
            "api_key"
        );

        var overriddenModel = JinaAIEmbeddingsModel.of(model, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEqual() {
        JinaAIEmbeddingsTaskSettings taskSettings = new JinaAIEmbeddingsTaskSettings(randomFrom(VALID_INPUT_TYPE_VALUES), randomBoolean());
        var model = createModel(null, "modelName", taskSettings, "api_key");

        var overriddenModel = JinaAIEmbeddingsModel.of(
            model,
            getTaskSettingsMap(taskSettings.getInputType(), taskSettings.getLateChunking())
        );
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_SetsInputType_FromRequestTaskSettings_IfValid_OverridingStoredTaskSettings() {
        String modelName = "modelName";
        String apiKey = "api_key";
        var model = createModel(null, modelName, new JinaAIEmbeddingsTaskSettings(InputType.INGEST, true), apiKey);

        var overriddenModel = JinaAIEmbeddingsModel.of(model, getTaskSettingsMap(InputType.SEARCH));
        var expectedModel = createModel(null, modelName, new JinaAIEmbeddingsTaskSettings(InputType.SEARCH, true), apiKey);
        assertThat(overriddenModel, is(expectedModel));
    }

    public void testOf_SetsLateChunking_FromRequestTaskSettings() {
        String modelName = "modelName";
        String apiKey = "api_key";
        var model = createModel(null, modelName, new JinaAIEmbeddingsTaskSettings(InputType.INGEST, true), apiKey);

        var overriddenModel = JinaAIEmbeddingsModel.of(model, getTaskSettingsMap(InputType.INGEST, false));
        var expectedModel = createModel(null, modelName, new JinaAIEmbeddingsTaskSettings(InputType.INGEST, false), apiKey);
        assertThat(overriddenModel, is(expectedModel));
    }

    /**
     * Returns a model with empty task settings, service settings and chunking settings
     */
    public static JinaAIEmbeddingsModel createModel(String url, String modelName, String apiKey) {
        return createModel(url, modelName, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, apiKey);
    }

    /**
    * Returns a model with empty service settings and chunking settings
    */
    public static JinaAIEmbeddingsModel createModel(
        @Nullable String url,
        String modelName,
        JinaAIEmbeddingsTaskSettings taskSettings,
        String apiKey
    ) {
        var serviceSettings = getEmbeddingServiceSettings(modelName, null, null, null, null, null, false);
        return createModel(url, serviceSettings, taskSettings, null, apiKey);
    }

    /**
     * Returns a model with empty service settings
     */
    public static JinaAIEmbeddingsModel createModel(
        @Nullable String url,
        String modelName,
        JinaAIEmbeddingsTaskSettings taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        String apiKey
    ) {
        var serviceSettings = getEmbeddingServiceSettings(modelName, null, null, null, null, null, false);
        return createModel(url, serviceSettings, taskSettings, chunkingSettings, apiKey);
    }

    /**
     * Convenience method that only sets fields used in constructing the request sent to Jina
     */
    public static JinaAIEmbeddingsModel createModel(
        @Nullable String url,
        String modelName,
        @Nullable JinaAIEmbeddingType embeddingType,
        JinaAIEmbeddingsTaskSettings taskSettings,
        String apiKey,
        @Nullable Integer dimensions
    ) {
        var serviceSettings = getEmbeddingServiceSettings(modelName, null, null, dimensions, null, embeddingType, dimensions != null);
        return createModel(url, serviceSettings, taskSettings, null, apiKey);
    }

    public static JinaAIEmbeddingsModel createModel(
        @Nullable String url,
        String modelName,
        @Nullable RateLimitSettings rateLimitSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType,
        JinaAIEmbeddingsTaskSettings taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        String apiKey,
        boolean dimensionsSetByUser
    ) {
        var serviceSettings = getEmbeddingServiceSettings(
            modelName,
            rateLimitSettings,
            similarity,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser
        );
        return createModel(url, serviceSettings, taskSettings, chunkingSettings, apiKey);
    }

    public static JinaAIEmbeddingsModel createModel(
        @Nullable String url,
        JinaAIEmbeddingsServiceSettings serviceSettings,
        JinaAIEmbeddingsTaskSettings taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        String apiKey
    ) {
        return new JinaAIEmbeddingsModel(
            "id",
            serviceSettings,
            taskSettings,
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray())),
            url
        );
    }

    public static JinaAIEmbeddingsServiceSettings getEmbeddingServiceSettings(
        String modelName,
        @Nullable RateLimitSettings rateLimitSettings,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable JinaAIEmbeddingType embeddingType,
        boolean dimensionsSetByUser
    ) {
        return new JinaAIEmbeddingsServiceSettings(
            new JinaAIServiceSettings(modelName, rateLimitSettings),
            similarity,
            dimensions,
            maxInputTokens,
            embeddingType,
            dimensionsSetByUser
        );
    }
}
