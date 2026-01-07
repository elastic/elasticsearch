/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsTaskSettingsTests.buildTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class NvidiaEmbeddingsModelTests extends ESTestCase {

    private static final String URL_VALUE = "http://www.abc.com";
    private static final String URL_DEFAULT_VALUE = "https://integrate.api.nvidia.com/v1/embeddings";
    private static final String API_KEY_VALUE = "test_api_key";
    private static final String MODEL_VALUE = "some_model";
    private static final InputType INPUT_TYPE_INITIAL_ELASTIC_VALUE = InputType.INGEST;
    private static final Truncation TRUNCATE_INITIAL_ELASTIC_VALUE = Truncation.START;
    private static final InputType INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE = InputType.SEARCH;
    private static final Truncation TRUNCATE_OVERRIDDEN_ELASTIC_VALUE = Truncation.END;

    public static NvidiaEmbeddingsModel createEmbeddingsModel(
        @Nullable String url,
        String apiKey,
        @Nullable String modelId,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions,
        @Nullable InputType inputType,
        @Nullable Truncation truncation,
        @Nullable ChunkingSettings chunkingSettings
    ) {
        return new NvidiaEmbeddingsModel(
            "inferenceEntityId",
            TaskType.TEXT_EMBEDDING,
            "service",
            new NvidiaEmbeddingsServiceSettings(modelId, url, dimensions, SimilarityMeasure.DOT_PRODUCT, maxInputTokens, null),
            new NvidiaEmbeddingsTaskSettings(inputType, truncation),
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public void testOverrideWith_SameParams_KeepsSameModel() {
        testOverrideWith_KeepsSameModel(buildTaskSettingsMap(INPUT_TYPE_INITIAL_ELASTIC_VALUE, TRUNCATE_INITIAL_ELASTIC_VALUE));
    }

    public void testOverrideWith_EmptyParams_KeepsSameModel() {
        testOverrideWith_KeepsSameModel(buildTaskSettingsMap(null, null));
    }

    private static void testOverrideWith_KeepsSameModel(Map<String, Object> taskSettings) {
        var model = createEmbeddingsModel(
            URL_VALUE,
            API_KEY_VALUE,
            MODEL_VALUE,
            null,
            null,
            INPUT_TYPE_INITIAL_ELASTIC_VALUE,
            TRUNCATE_INITIAL_ELASTIC_VALUE,
            null
        );
        var overriddenModel = NvidiaEmbeddingsModel.of(model, taskSettings);
        assertThat(overriddenModel, is(sameInstance(model)));
    }

    public void testOverride_OverridesAllTaskSettings() {
        test_OverriddenParams(
            buildTaskSettingsMap(INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE, TRUNCATE_OVERRIDDEN_ELASTIC_VALUE),
            INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE,
            TRUNCATE_OVERRIDDEN_ELASTIC_VALUE
        );
    }

    public void testOverride_OverridesOnlyTruncation() {
        test_OverriddenParams(
            buildTaskSettingsMap(null, TRUNCATE_OVERRIDDEN_ELASTIC_VALUE),
            INPUT_TYPE_INITIAL_ELASTIC_VALUE,
            TRUNCATE_OVERRIDDEN_ELASTIC_VALUE
        );
    }

    public void testOverride_OverridesOnlyTopN() {
        test_OverriddenParams(
            buildTaskSettingsMap(INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE, null),
            INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE,
            TRUNCATE_INITIAL_ELASTIC_VALUE
        );
    }

    public void testOverride_OverridesNullValues() {
        var model = createEmbeddingsModel(URL_VALUE, API_KEY_VALUE, MODEL_VALUE, null, null, null, null, null);
        var overriddenModel = NvidiaEmbeddingsModel.of(
            model,
            buildTaskSettingsMap(INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE, TRUNCATE_OVERRIDDEN_ELASTIC_VALUE)
        );

        assertThat(overriddenModel.getTaskSettings().getInputType(), is(INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE));
        assertThat(overriddenModel.getTaskSettings().getTruncation(), is(TRUNCATE_OVERRIDDEN_ELASTIC_VALUE));
    }

    private static void test_OverriddenParams(Map<String, Object> taskSettings, InputType expectedInputType, Truncation expectedTruncate) {
        var model = createEmbeddingsModel(
            URL_VALUE,
            API_KEY_VALUE,
            MODEL_VALUE,
            null,
            null,
            INPUT_TYPE_INITIAL_ELASTIC_VALUE,
            TRUNCATE_INITIAL_ELASTIC_VALUE,
            null
        );
        var overriddenModel = NvidiaEmbeddingsModel.of(model, taskSettings);

        assertThat(overriddenModel.getTaskSettings().getInputType(), is(expectedInputType));
        assertThat(overriddenModel.getTaskSettings().getTruncation(), is(expectedTruncate));
    }

    public void testCreateModel_NoUrl_DefaultUrl() {
        var model = createEmbeddingsModel(
            null,
            API_KEY_VALUE,
            MODEL_VALUE,
            null,
            null,
            INPUT_TYPE_INITIAL_ELASTIC_VALUE,
            TRUNCATE_INITIAL_ELASTIC_VALUE,
            null
        );

        assertThat(model.getServiceSettings().uri().toString(), is(URL_DEFAULT_VALUE));
    }

}
