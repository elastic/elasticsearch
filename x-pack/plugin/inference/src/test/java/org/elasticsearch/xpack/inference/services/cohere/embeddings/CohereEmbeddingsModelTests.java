/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.SimilarityMeasure;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.hamcrest.MatcherAssert;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettingsTests.getTaskSettingsMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class CohereEmbeddingsModelTests extends ESTestCase {

    public void testOverrideWith_OverridesModel() {
        var model = createModel("url", "api_key", null);

        var overriddenModel = model.overrideWith(getTaskSettingsMap("model", null, null, null));
        var expectedModel = createModel("url", "api_key", new CohereEmbeddingsTaskSettings("model", null, null, null), null, null);
        MatcherAssert.assertThat(overriddenModel, is(expectedModel));
    }

    public void testOverrideWith_DoesNotOverride_WhenSettingsAreEmpty() {
        var model = createModel("url", "api_key", null);

        var overriddenModel = model.overrideWith(Map.of());
        MatcherAssert.assertThat(overriddenModel, sameInstance(model));
    }

    public void testOverrideWith_DoesNotOverride_WhenSettingsAreNull() {
        var model = createModel("url", "api_key", null);

        var overriddenModel = model.overrideWith(null);
        MatcherAssert.assertThat(overriddenModel, sameInstance(model));
    }

    public static CohereEmbeddingsModel createModel(String url, String apiKey, @Nullable Integer tokenLimit) {
        return createModel(url, apiKey, CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, tokenLimit, null);
    }

    public static CohereEmbeddingsModel createModel(String url, String apiKey, @Nullable Integer tokenLimit, @Nullable Integer dimensions) {
        return createModel(url, apiKey, CohereEmbeddingsTaskSettings.EMPTY_SETTINGS, tokenLimit, dimensions);
    }

    public static CohereEmbeddingsModel createModel(
        String url,
        String apiKey,
        CohereEmbeddingsTaskSettings taskSettings,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions
    ) {
        return new CohereEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new CohereServiceSettings(url, SimilarityMeasure.DOT_PRODUCT, dimensions, tokenLimit),
            taskSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
