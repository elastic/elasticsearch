/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.embeddings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;

public class MistralEmbeddingModelTests extends ESTestCase {
    public static MistralEmbeddingsModel createModel(String inferenceId, String model, String apiKey) {
        return createModel(inferenceId, model, apiKey, null, null, null, null);
    }

    public static MistralEmbeddingsModel createModel(
        String inferenceId,
        String model,
        ChunkingSettings chunkingSettings,
        String apiKey,
        @Nullable Integer dimensions,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarity,
        RateLimitSettings rateLimitSettings
    ) {
        return new MistralEmbeddingsModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            "mistral",
            new MistralEmbeddingsServiceSettings(model, dimensions, maxTokens, similarity, rateLimitSettings),
            EmptyTaskSettings.INSTANCE,
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static MistralEmbeddingsModel createModel(
        String inferenceId,
        String model,
        String apiKey,
        @Nullable Integer dimensions,
        @Nullable Integer maxTokens,
        @Nullable SimilarityMeasure similarity,
        RateLimitSettings rateLimitSettings
    ) {
        return new MistralEmbeddingsModel(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            "mistral",
            new MistralEmbeddingsServiceSettings(model, dimensions, maxTokens, similarity, rateLimitSettings),
            EmptyTaskSettings.INSTANCE,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public void testThrowsError_WhenInputTypeSpecified() {
        var model = createModel("id", "model", "api_key");

        var thrownException = expectThrows(ValidationException.class, () -> MistralEmbeddingsModel.of(model, Map.of(), InputType.SEARCH));
        assertThat(thrownException.getMessage(), is("Validation Failed: 1: Invalid value [search] received. [input_type] is not allowed;"));
    }

    public void testAcceptsInternalInputType() {
        var model = createModel("id", "model", "api_key");
        var overriddenModel = MistralEmbeddingsModel.of(model, Map.of(), InputType.INTERNAL_SEARCH);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(model));
    }

    public void testAcceptsNullInputType() {
        var model = createModel("id", "model", "api_key");
        var overriddenModel = MistralEmbeddingsModel.of(model, Map.of(), null);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(model));
    }
}
