/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.voyageai.VoyageAIServiceSettings;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.text.VoyageAIEmbeddingType;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual.VoyageAIContextualEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;

public class VoyageAIContextualEmbeddingsModelTests extends ESTestCase {

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreEmpty() {
        var model = createModel("url", "api_key", null, null, "voyage-context-3");

        var overriddenModel = VoyageAIContextualEmbeddingsModel.of(model, Map.of());
        assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_DoesNotOverrideAndModelRemainsEqual_WhenSettingsAreNull() {
        var model = createModel("url", "api_key", null, null, "voyage-context-3");

        var overriddenModel = VoyageAIContextualEmbeddingsModel.of(model, null);
        assertThat(overriddenModel, is(model));
    }

    public void testOverrideWith_SetsInputType_FromRequestTaskSettings_IfValid_OverridingStoredTaskSettings() {
        var model = createModel(
            "url",
            "api_key",
            new VoyageAIContextualEmbeddingsTaskSettings(InputType.INGEST),
            null,
            null,
            "voyage-context-3"
        );

        var overriddenModel = VoyageAIContextualEmbeddingsModel.of(model, getTaskSettingsMap(InputType.SEARCH));
        var expectedModel = createModel(
            "url",
            "api_key",
            new VoyageAIContextualEmbeddingsTaskSettings(InputType.SEARCH),
            null,
            null,
            "voyage-context-3"
        );
        assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotOverrideInputType_WhenRequestTaskSettingsIsNull() {
        var model = createModel(
            "url",
            "api_key",
            new VoyageAIContextualEmbeddingsTaskSettings(InputType.INGEST),
            null,
            null,
            "voyage-context-3"
        );

        var overriddenModel = VoyageAIContextualEmbeddingsModel.of(model, getTaskSettingsMap(null));
        var expectedModel = createModel(
            "url",
            "api_key",
            new VoyageAIContextualEmbeddingsTaskSettings(InputType.INGEST),
            null,
            null,
            "voyage-context-3"
        );
        assertThat(overriddenModel, is(expectedModel));
    }

    public static VoyageAIContextualEmbeddingsModel createModel(
        String url,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable String model
    ) {
        return createModel(url, apiKey, VoyageAIContextualEmbeddingsTaskSettings.EMPTY_SETTINGS, tokenLimit, null, model);
    }

    public static VoyageAIContextualEmbeddingsModel createModel(
        String url,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model
    ) {
        return createModel(url, apiKey, VoyageAIContextualEmbeddingsTaskSettings.EMPTY_SETTINGS, tokenLimit, dimensions, model);
    }

    public static VoyageAIContextualEmbeddingsModel createModel(
        String url,
        String apiKey,
        VoyageAIContextualEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model
    ) {
        return new VoyageAIContextualEmbeddingsModel(
            "id",
            "service",
            url,
            new VoyageAIContextualEmbeddingsServiceSettings(
                new VoyageAIServiceSettings(model, null),
                VoyageAIContextualEmbeddingType.FLOAT,
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

    public static VoyageAIContextualEmbeddingsModel createModel(
        String url,
        String apiKey,
        VoyageAIContextualEmbeddingsTaskSettings taskSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model
    ) {
        return new VoyageAIContextualEmbeddingsModel(
            "id",
            "service",
            url,
            new VoyageAIContextualEmbeddingsServiceSettings(
                new VoyageAIServiceSettings(model, null),
                VoyageAIContextualEmbeddingType.FLOAT,
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

    public static VoyageAIContextualEmbeddingsModel createModel(
        String url,
        String apiKey,
        VoyageAIContextualEmbeddingsTaskSettings taskSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model,
        VoyageAIContextualEmbeddingType embeddingType
    ) {
        return new VoyageAIContextualEmbeddingsModel(
            "id",
            "service",
            url,
            new VoyageAIContextualEmbeddingsServiceSettings(
                new VoyageAIServiceSettings(model, null),
                embeddingType,
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

    @SuppressWarnings("unused")
    public static VoyageAIContextualEmbeddingsModel createModel(
        String url,
        String apiKey,
        VoyageAIContextualEmbeddingsTaskSettings taskSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model,
        VoyageAIEmbeddingType embeddingType
    ) {
        return new VoyageAIContextualEmbeddingsModel(
            "id",
            "service",
            url,
            new VoyageAIContextualEmbeddingsServiceSettings(
                new VoyageAIServiceSettings(model, null),
                VoyageAIContextualEmbeddingType.FLOAT,
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

    public static VoyageAIContextualEmbeddingsModel createModel(
        String url,
        String apiKey,
        VoyageAIContextualEmbeddingsTaskSettings taskSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        String model,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        return new VoyageAIContextualEmbeddingsModel(
            "id",
            "service",
            url,
            new VoyageAIContextualEmbeddingsServiceSettings(
                new VoyageAIServiceSettings(model, null),
                VoyageAIContextualEmbeddingType.FLOAT,
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
