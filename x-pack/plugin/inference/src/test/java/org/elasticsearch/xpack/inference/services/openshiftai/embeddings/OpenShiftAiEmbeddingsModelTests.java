/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

public class OpenShiftAiEmbeddingsModelTests extends ESTestCase {

    public static OpenShiftAiEmbeddingsModel createModel(String url, String apiKey, @Nullable String modelId) {
        return createModel(url, apiKey, modelId, 1234);
    }

    public static OpenShiftAiEmbeddingsModel createModel(String url, String apiKey, @Nullable String modelId, int maxInputTokens) {
        return new OpenShiftAiEmbeddingsModel(
            "inferenceEntityId",
            TaskType.TEXT_EMBEDDING,
            "service",
            new OpenShiftAiEmbeddingsServiceSettings(modelId, url, 1536, SimilarityMeasure.DOT_PRODUCT, maxInputTokens, null, false),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
