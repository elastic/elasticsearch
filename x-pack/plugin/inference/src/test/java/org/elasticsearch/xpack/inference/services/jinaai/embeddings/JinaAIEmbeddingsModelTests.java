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
import org.hamcrest.MatcherAssert;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.jinaai.embeddings.JinaAIEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;

public class JinaAIEmbeddingsModelTests extends ESTestCase {

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEmpty_AndInputTypeIsInvalid() {
        var model = createModel("url", "api_key", null, null, "model", JinaAIEmbeddingType.FLOAT);

        var overriddenModel = JinaAIEmbeddingsModel.of(model, Map.of(), InputType.UNSPECIFIED);
        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreNull_AndInputTypeIsInvalid() {
        var model = createModel("url", "api_key", null, null, "model", JinaAIEmbeddingType.FLOAT);

        var overriddenModel = JinaAIEmbeddingsModel.of(model, null, InputType.UNSPECIFIED);
        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_SetsInputTypeToIngest_WhenTheFieldIsNullInModelTaskSettings_AndNullInRequestTaskSettings() {
        var model = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings((InputType) null),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );

        var overriddenModel = JinaAIEmbeddingsModel.of(model, getTaskSettingsMap(null), InputType.INGEST);
        var expectedModel = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_SetsInputType_FromRequest_IfValid_OverridingStoredTaskSettings() {
        var model = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );

        var overriddenModel = JinaAIEmbeddingsModel.of(model, getTaskSettingsMap(null), InputType.SEARCH);
        var expectedModel = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings(InputType.SEARCH),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_SetsInputType_FromRequest_IfValid_OverridingRequestTaskSettings() {
        var model = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings((InputType) null),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );

        var overriddenModel = JinaAIEmbeddingsModel.of(model, getTaskSettingsMap(InputType.INGEST), InputType.SEARCH);
        var expectedModel = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings(InputType.SEARCH),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_OverridesInputType_WithRequestTaskSettingsSearch_WhenRequestInputTypeIsInvalid() {
        var model = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );

        var overriddenModel = JinaAIEmbeddingsModel.of(model, getTaskSettingsMap(InputType.SEARCH), InputType.UNSPECIFIED);
        var expectedModel = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings(InputType.SEARCH),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotSetInputType_FromRequest_IfInputTypeIsInvalid() {
        var model = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings((InputType) null),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );

        var overriddenModel = JinaAIEmbeddingsModel.of(model, getTaskSettingsMap(null), InputType.UNSPECIFIED);
        var expectedModel = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings((InputType) null),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotSetInputType_WhenRequestTaskSettingsIsNull_AndRequestInputTypeIsInvalid() {
        var model = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );

        var overriddenModel = JinaAIEmbeddingsModel.of(model, getTaskSettingsMap(null), InputType.UNSPECIFIED);
        var expectedModel = createModel(
            "url",
            "api_key",
            new JinaAIEmbeddingsTaskSettings(InputType.INGEST),
            null,
            null,
            "model",
            JinaAIEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public static JinaAIEmbeddingsModel createModel(
        String url,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable String model,
        @Nullable JinaAIEmbeddingType embeddingType
    ) {
        return createModel(url, apiKey, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, tokenLimit, null, model, embeddingType);
    }

    public static JinaAIEmbeddingsModel createModel(
        String url,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model,
        @Nullable JinaAIEmbeddingType embeddingType
    ) {
        return createModel(url, apiKey, JinaAIEmbeddingsTaskSettings.EMPTY_SETTINGS, tokenLimit, dimensions, model, embeddingType);
    }

    public static JinaAIEmbeddingsModel createModel(
        String url,
        String apiKey,
        JinaAIEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model,
        @Nullable JinaAIEmbeddingType embeddingType
    ) {
        return new JinaAIEmbeddingsModel(
            "id",
            "service",
            new JinaAIEmbeddingsServiceSettings(
                new JinaAIServiceSettings(url, model, null),
                SimilarityMeasure.DOT_PRODUCT,
                dimensions,
                tokenLimit,
                embeddingType
            ),
            taskSettings,
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static JinaAIEmbeddingsModel createModel(
        String url,
        String apiKey,
        JinaAIEmbeddingsTaskSettings taskSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model,
        @Nullable JinaAIEmbeddingType embeddingType
    ) {
        return new JinaAIEmbeddingsModel(
            "id",
            "service",
            new JinaAIEmbeddingsServiceSettings(
                new JinaAIServiceSettings(url, model, null),
                SimilarityMeasure.DOT_PRODUCT,
                dimensions,
                tokenLimit,
                embeddingType
            ),
            taskSettings,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static JinaAIEmbeddingsModel createModel(
        String url,
        String apiKey,
        JinaAIEmbeddingsTaskSettings taskSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model,
        @Nullable SimilarityMeasure similarityMeasure,
        @Nullable JinaAIEmbeddingType embeddingType
    ) {
        return new JinaAIEmbeddingsModel(
            "id",
            "service",
            new JinaAIEmbeddingsServiceSettings(
                new JinaAIServiceSettings(url, model, null),
                similarityMeasure,
                dimensions,
                tokenLimit,
                embeddingType
            ),
            taskSettings,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
