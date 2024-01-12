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

public class CohereEmbeddingsModelTests extends ESTestCase {

    public static CohereEmbeddingsModel createModel(String url, String apiKey, @Nullable Integer tokenLimit) {
        return new CohereEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new CohereServiceSettings(url, SimilarityMeasure.DOT_PRODUCT, 1536, tokenLimit),
            CohereEmbeddingsTaskSettings.EMPTY_SETTINGS,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static CohereEmbeddingsModel createModel(String url, String apiKey, @Nullable Integer tokenLimit, @Nullable Integer dimensions) {
        return new CohereEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new CohereServiceSettings(url, SimilarityMeasure.DOT_PRODUCT, dimensions, tokenLimit),
            CohereEmbeddingsTaskSettings.EMPTY_SETTINGS,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
