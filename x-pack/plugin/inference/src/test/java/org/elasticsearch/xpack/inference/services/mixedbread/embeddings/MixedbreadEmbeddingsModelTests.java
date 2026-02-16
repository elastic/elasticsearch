/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.mixedbread.TestUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class MixedbreadEmbeddingsModelTests extends ESTestCase {
    private static final Integer MAX_INPUT_TOKENS = 3;
    private static final Integer DIMENSIONS = 4;

    public void testConstructor_usesDefaultUrlWhenNull() {
        var model = createModel(
            null,
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            null,
            null,
            TestUtils.PROMPT_INITIAL_VALUE,
            TestUtils.NORMALIZED_INITIAL_VALUE,
            null,
            null
        );

        assertThat(model.getServiceSettings().uri().toString(), is(TestUtils.DEFAULT_EMBEDDINGS_URL));
    }

    public void testConstructor_usesUrlWhenSpecified() {
        var model = createModel(
            TestUtils.CUSTOM_URL,
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            null,
            null,
            TestUtils.PROMPT_INITIAL_VALUE,
            TestUtils.NORMALIZED_INITIAL_VALUE,
            null,
            null
        );
        assertThat(model.getServiceSettings().uri().toString(), is(TestUtils.CUSTOM_URL));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEmpty() {
        var model = createModel(
            TestUtils.CUSTOM_URL,
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            MAX_INPUT_TOKENS,
            DIMENSIONS,
            TestUtils.PROMPT_INITIAL_VALUE,
            TestUtils.NORMALIZED_INITIAL_VALUE,
            null,
            null
        );
        var overriddenModel = MixedbreadEmbeddingsModel.of(model, Map.of());
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreNull() {
        var model = createModel(
            TestUtils.CUSTOM_URL,
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            MAX_INPUT_TOKENS,
            DIMENSIONS,
            TestUtils.PROMPT_INITIAL_VALUE,
            TestUtils.NORMALIZED_INITIAL_VALUE,
            null,
            null
        );
        var overriddenModel = MixedbreadEmbeddingsModel.of(model, null);
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEqual() {
        var model = createModel(
            TestUtils.CUSTOM_URL,
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            MAX_INPUT_TOKENS,
            DIMENSIONS,
            TestUtils.PROMPT_INITIAL_VALUE,
            TestUtils.NORMALIZED_INITIAL_VALUE,
            null,
            null
        );
        var overriddenModel = MixedbreadEmbeddingsModel.of(
            model,
            MixedbreadEmbeddingsTaskSettingsTests.getTaskSettingsMap(TestUtils.PROMPT_INITIAL_VALUE, TestUtils.NORMALIZED_INITIAL_VALUE)
        );
        assertThat(overriddenModel, sameInstance(model));
    }

    public void testOf_SetsPrompt_FromRequestTaskSettings_OverridingStoredTaskSettings() {
        var model = createModel(
            TestUtils.CUSTOM_URL,
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            MAX_INPUT_TOKENS,
            DIMENSIONS,
            TestUtils.PROMPT_INITIAL_VALUE,
            null,
            null,
            null
        );
        var overriddenModel = MixedbreadEmbeddingsModel.of(
            model,
            MixedbreadEmbeddingsTaskSettingsTests.getTaskSettingsMap(TestUtils.PROMPT_OVERRIDDEN_VALUE, null)
        );
        var expectedModel = createModel(
            TestUtils.CUSTOM_URL,
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            MAX_INPUT_TOKENS,
            DIMENSIONS,
            TestUtils.PROMPT_OVERRIDDEN_VALUE,
            null,
            null,
            null
        );
        assertThat(overriddenModel, is(expectedModel));
    }

    public void testOf_SetsNormalized_FromRequestTaskSettings() {
        var model = createModel(
            TestUtils.CUSTOM_URL,
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            MAX_INPUT_TOKENS,
            DIMENSIONS,
            null,
            TestUtils.NORMALIZED_INITIAL_VALUE,
            null,
            null
        );
        var overriddenModel = MixedbreadEmbeddingsModel.of(
            model,
            MixedbreadEmbeddingsTaskSettingsTests.getTaskSettingsMap(null, TestUtils.NORMALIZED_OVERRIDDEN_VALUE)
        );
        var expectedModel = createModel(
            TestUtils.CUSTOM_URL,
            TestUtils.API_KEY,
            TestUtils.MODEL_ID,
            MAX_INPUT_TOKENS,
            DIMENSIONS,
            null,
            TestUtils.NORMALIZED_OVERRIDDEN_VALUE,
            null,
            null
        );
        assertThat(overriddenModel, is(expectedModel));
    }

    public static MixedbreadEmbeddingsModel createModel(
        @Nullable String url,
        String apiKey,
        @Nullable String modelId,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions,
        @Nullable String prompt,
        @Nullable Boolean normalized,
        @Nullable SimilarityMeasure similarity,
        @Nullable ChunkingSettings chunkingSettings
    ) {
        return new MixedbreadEmbeddingsModel(
            "inferenceEntityId",
            new MixedbreadEmbeddingsServiceSettings(
                modelId,
                url,
                dimensions,
                similarity,
                maxInputTokens,
                null,
                TestUtils.DIMENSIONS_SET_BY_USER_TRUE
            ),
            new MixedbreadEmbeddingsTaskSettings(prompt, normalized),
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
