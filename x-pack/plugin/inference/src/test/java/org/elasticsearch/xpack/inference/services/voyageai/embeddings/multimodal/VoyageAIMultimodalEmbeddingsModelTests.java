/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;

public class VoyageAIMultimodalEmbeddingsModelTests extends ESTestCase {

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEmpty() {
        var model = createModel("url", "api_key", null, null, "voyage-multimodal-3");

        var overriddenModel = VoyageAIMultimodalEmbeddingsModel.of(model, Map.of());
        assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreNull() {
        var model = createModel("url", "api_key", null, null, "voyage-multimodal-3");

        var overriddenModel = VoyageAIMultimodalEmbeddingsModel.of(model, null);
        assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_SetsInputType_FromRequestTaskSettings_IfValid_OverridingStoredTaskSettings() {
        var model = createModel(
            "url",
            "api_key",
            new VoyageAIMultimodalEmbeddingsTaskSettings(InputType.INGEST, null),
            null,
            null,
            "voyage-multimodal-3"
        );

        var overriddenModel = VoyageAIMultimodalEmbeddingsModel.of(model, getTaskSettingsMap(InputType.SEARCH));
        var expectedModel = createModel(
            "url",
            "api_key",
            new VoyageAIMultimodalEmbeddingsTaskSettings(InputType.SEARCH, null),
            null,
            null,
            "voyage-multimodal-3"
        );
        assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotOverrideInputType_WhenRequestTaskSettingsIsNull() {
        var model = createModel(
            "url",
            "api_key",
            new VoyageAIMultimodalEmbeddingsTaskSettings(InputType.INGEST, null),
            null,
            null,
            "voyage-multimodal-3"
        );

        var overriddenModel = VoyageAIMultimodalEmbeddingsModel.of(model, getTaskSettingsMap(null));
        var expectedModel = createModel(
            "url",
            "api_key",
            new VoyageAIMultimodalEmbeddingsTaskSettings(InputType.INGEST, null),
            null,
            null,
            "voyage-multimodal-3"
        );
        assertThat(overriddenModel, is(expectedModel));
    }

    public static VoyageAIMultimodalEmbeddingsModel createModel(
        String url,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable String model
    ) {
        return createModel(url, apiKey, VoyageAIMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS, tokenLimit, null, model);
    }

    public static VoyageAIMultimodalEmbeddingsModel createModel(
        String url,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model
    ) {
        return createModel(url, apiKey, VoyageAIMultimodalEmbeddingsTaskSettings.EMPTY_SETTINGS, tokenLimit, dimensions, model);
    }

    public static VoyageAIMultimodalEmbeddingsModel createModel(
        String url,
        String apiKey,
        VoyageAIMultimodalEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model
    ) {
        return new VoyageAIMultimodalEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new VoyageAIMultimodalEmbeddingsServiceSettings(
                new VoyageAIServiceSettings(model, null),
                null,
                SimilarityMeasure.DOT_PRODUCT,
                dimensions,
                tokenLimit,
                false
            ),
            taskSettings,
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static VoyageAIMultimodalEmbeddingsModel createModel(
        String url,
        String apiKey,
        VoyageAIMultimodalEmbeddingsTaskSettings taskSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model
    ) {
        return new VoyageAIMultimodalEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new VoyageAIMultimodalEmbeddingsServiceSettings(
                new VoyageAIServiceSettings(model, null),
                null,
                SimilarityMeasure.DOT_PRODUCT,
                dimensions,
                tokenLimit,
                false
            ),
            taskSettings,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static VoyageAIMultimodalEmbeddingsModel createModel(
        String url,
        String apiKey,
        VoyageAIMultimodalEmbeddingsTaskSettings taskSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        return new VoyageAIMultimodalEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new VoyageAIMultimodalEmbeddingsServiceSettings(
                new VoyageAIServiceSettings(model, null),
                null,
                similarityMeasure,
                dimensions,
                tokenLimit,
                false
            ),
            taskSettings,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
