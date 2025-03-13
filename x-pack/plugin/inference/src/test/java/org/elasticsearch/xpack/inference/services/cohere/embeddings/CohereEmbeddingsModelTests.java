/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.hamcrest.MatcherAssert;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;

public class CohereEmbeddingsModelTests extends ESTestCase {

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEmpty_AndInputTypeIsInvalid() {
        var model = createModel("url", "api_key", null, null, null);

        var overriddenModel = CohereEmbeddingsModel.of(model, Map.of(), InputType.UNSPECIFIED);
        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreNull_AndInputTypeIsInvalid() {
        var model = createModel("url", "api_key", null, null, null);

        var overriddenModel = CohereEmbeddingsModel.of(model, null, InputType.UNSPECIFIED);
        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_SetsInputTypeToIngest_WhenTheFieldIsNullInModelTaskSettings_AndNullInRequestTaskSettings() {
        var model = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(null, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );

        var overriddenModel = CohereEmbeddingsModel.of(model, getTaskSettingsMap(null, null), InputType.INGEST);
        var expectedModel = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(InputType.INGEST, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_SetsInputType_FromRequest_IfValid_OverridingStoredTaskSettings() {
        var model = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(InputType.INGEST, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );

        var overriddenModel = CohereEmbeddingsModel.of(model, getTaskSettingsMap(null, null), InputType.SEARCH);
        var expectedModel = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_SetsInputType_FromRequest_IfValid_OverridingRequestTaskSettings() {
        var model = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(null, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );

        var overriddenModel = CohereEmbeddingsModel.of(model, getTaskSettingsMap(InputType.INGEST, null), InputType.SEARCH);
        var expectedModel = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_OverridesInputType_WithRequestTaskSettingsSearch_WhenRequestInputTypeIsInvalid() {
        var model = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(InputType.INGEST, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );

        var overriddenModel = CohereEmbeddingsModel.of(model, getTaskSettingsMap(InputType.SEARCH, null), InputType.UNSPECIFIED);
        var expectedModel = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(InputType.SEARCH, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotSetInputType_FromRequest_IfInputTypeIsInvalid() {
        var model = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(null, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );

        var overriddenModel = CohereEmbeddingsModel.of(model, getTaskSettingsMap(null, null), InputType.UNSPECIFIED);
        var expectedModel = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(null, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotSetInputType_WhenRequestTaskSettingsIsNull_AndRequestInputTypeIsInvalid() {
        var model = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(InputType.INGEST, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );

        var overriddenModel = CohereEmbeddingsModel.of(model, getTaskSettingsMap(null, null), InputType.UNSPECIFIED);
        var expectedModel = createModel(
            "url",
            "api_key",
            new CohereEmbeddingsTaskSettings(InputType.INGEST, null),
            null,
            null,
            "model",
            CohereEmbeddingType.FLOAT
        );
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public static CohereEmbeddingsModel createModel(
        String url,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable String model,
        @Nullable CohereEmbeddingType embeddingType
    ) {
        return createModel(url, apiKey, CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, tokenLimit, null, model, embeddingType);
    }

    public static CohereEmbeddingsModel createModel(
        String url,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        @Nullable String model,
        @Nullable CohereEmbeddingType embeddingType
    ) {
        return createModel(url, apiKey, CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, tokenLimit, dimensions, model, embeddingType);
    }

    public static CohereEmbeddingsModel createModel(
        String url,
        String apiKey,
        CohereEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        @Nullable String model,
        @Nullable CohereEmbeddingType embeddingType
    ) {
        return new CohereEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new CohereEmbeddingsServiceSettings(
                new CohereServiceSettings(url, SimilarityMeasure.DOT_PRODUCT, dimensions, tokenLimit, model, null),
                Objects.requireNonNullElse(embeddingType, CohereEmbeddingType.FLOAT)
            ),
            taskSettings,
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static CohereEmbeddingsModel createModel(
        String url,
        String apiKey,
        CohereEmbeddingsTaskSettings taskSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        @Nullable String model,
        @Nullable CohereEmbeddingType embeddingType
    ) {
        return new CohereEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new CohereEmbeddingsServiceSettings(
                new CohereServiceSettings(url, SimilarityMeasure.DOT_PRODUCT, dimensions, tokenLimit, model, null),
                Objects.requireNonNullElse(embeddingType, CohereEmbeddingType.FLOAT)
            ),
            taskSettings,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static CohereEmbeddingsModel createModel(
        String url,
        String apiKey,
        CohereEmbeddingsTaskSettings taskSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        @Nullable String model,
        @Nullable CohereEmbeddingType embeddingType,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        return new CohereEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new CohereEmbeddingsServiceSettings(
                new CohereServiceSettings(url, similarityMeasure, dimensions, tokenLimit, model, null),
                Objects.requireNonNullElse(embeddingType, CohereEmbeddingType.FLOAT)
            ),
            taskSettings,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
