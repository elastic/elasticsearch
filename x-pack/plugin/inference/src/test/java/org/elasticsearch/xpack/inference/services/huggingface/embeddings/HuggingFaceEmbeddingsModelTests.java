/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.embeddings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.containsString;

public class HuggingFaceEmbeddingsModelTests extends ESTestCase {

    public void testThrowsURISyntaxException_ForInvalidUrl() {
        var thrownException = expectThrows(IllegalArgumentException.class, () -> createModel("^^", "secret"));
        assertThat(thrownException.getMessage(), containsString("unable to parse url [^^]"));
    }

    public static HuggingFaceEmbeddingsModel createModel(String url, String apiKey) {
        return new HuggingFaceEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new HuggingFaceServiceSettings(url),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static HuggingFaceEmbeddingsModel createModel(String url, String apiKey, int tokenLimit) {
        return new HuggingFaceEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new HuggingFaceServiceSettings(createUri(url), null, null, tokenLimit, null),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static HuggingFaceEmbeddingsModel createModel(String url, String apiKey, int tokenLimit, int dimensions) {
        return new HuggingFaceEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new HuggingFaceServiceSettings(createUri(url), null, dimensions, tokenLimit, null),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static HuggingFaceEmbeddingsModel createModel(
        String url,
        String apiKey,
        int tokenLimit,
        int dimensions,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        return new HuggingFaceEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new HuggingFaceServiceSettings(createUri(url), similarityMeasure, dimensions, tokenLimit, null),
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
