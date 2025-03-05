/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.embeddings;

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
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;

import static org.elasticsearch.xpack.inference.services.googleaistudio.embeddings.GoogleAiStudioEmbeddingsModel.MODEL_ID_WITH_TASK_TYPE;

public class GoogleAiStudioEmbeddingsModelTests extends ESTestCase {

    public static GoogleAiStudioEmbeddingsModel createModel(String model, String apiKey, String url) {
        return new GoogleAiStudioEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new GoogleAiStudioEmbeddingsServiceSettings(model, null, null, SimilarityMeasure.DOT_PRODUCT, null),
            EmptyTaskSettings.INSTANCE,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static GoogleAiStudioEmbeddingsModel createModel(String model, ChunkingSettings chunkingSettings, String apiKey, String url) {
        return new GoogleAiStudioEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new GoogleAiStudioEmbeddingsServiceSettings(model, null, null, SimilarityMeasure.DOT_PRODUCT, null),
            EmptyTaskSettings.INSTANCE,
            chunkingSettings,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static GoogleAiStudioEmbeddingsModel createModel(
        String url,
        String model,
        String apiKey,
        Integer dimensions,
        @Nullable SimilarityMeasure similarityMeasure
    ) {
        return new GoogleAiStudioEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            url,
            new GoogleAiStudioEmbeddingsServiceSettings(model, null, dimensions, similarityMeasure, null),
            EmptyTaskSettings.INSTANCE,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static GoogleAiStudioEmbeddingsModel createModel(
        String model,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions
    ) {
        return new GoogleAiStudioEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new GoogleAiStudioEmbeddingsServiceSettings(model, tokenLimit, dimensions, SimilarityMeasure.DOT_PRODUCT, null),
            EmptyTaskSettings.INSTANCE,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }

    public static GoogleAiStudioEmbeddingsModel createModel(
        String model,
        String apiKey,
        @Nullable Integer tokenLimit,
        @Nullable Integer dimensions,
        @Nullable InputType inputType
    ) {
        return new GoogleAiStudioEmbeddingsModel(
            "id",
            TaskType.TEXT_EMBEDDING,
            "service",
            new GoogleAiStudioEmbeddingsServiceSettings(model, tokenLimit, dimensions, SimilarityMeasure.DOT_PRODUCT, null),
            EmptyTaskSettings.INSTANCE,
            null,
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray())),
            inputType
        );
    }

    public void testInputTypeValid_WhenModelIdAllowsTaskType() {
        var model = createModel(MODEL_ID_WITH_TASK_TYPE, "api_key", null, null);
        var overriddenModel = GoogleAiStudioEmbeddingsModel.of(model, InputType.SEARCH);
        var expectedModel = createModel(MODEL_ID_WITH_TASK_TYPE, "api_key", null, null, InputType.SEARCH);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(expectedModel));
    }

    public void testInputTypeInternal_WhenModelIdAllowsTaskType() {
        var model = createModel(MODEL_ID_WITH_TASK_TYPE, "api_key", null, null);
        var overriddenModel = GoogleAiStudioEmbeddingsModel.of(model, InputType.INTERNAL_SEARCH);
        var expectedModel = createModel(MODEL_ID_WITH_TASK_TYPE, "api_key", null, null, InputType.INTERNAL_SEARCH);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(expectedModel));
    }

    public void testInputTypeNull_WhenModelIdAllowsTaskType() {
        var model = createModel(MODEL_ID_WITH_TASK_TYPE, "api_key", null, null);
        var overriddenModel = GoogleAiStudioEmbeddingsModel.of(model, null);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(model));
    }

    public void testInputTypeUnspecified_WhenModelIdAllowsTaskType() {
        var model = createModel(MODEL_ID_WITH_TASK_TYPE, "api_key", null, null);
        var overriddenModel = GoogleAiStudioEmbeddingsModel.of(model, InputType.UNSPECIFIED);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(model));
    }

    public void testThrowsError_WhenInputTypeSpecified_WhenModelIdDoesNotAllowTaskType() {
        var model = createModel("model", "api_key", null, null);

        var thrownException = expectThrows(ValidationException.class, () -> GoogleAiStudioEmbeddingsModel.of(model, InputType.SEARCH));
        assertThat(
            thrownException.getMessage(),
            CoreMatchers.is("Validation Failed: 1: Invalid value [search] received. [input_type] is not allowed;")
        );
    }

    public void testInputTypeInternal_WhenModelIdDoesNotAllowTaskType() {
        var model = createModel("model", "api_key", null, null);
        var overriddenModel = GoogleAiStudioEmbeddingsModel.of(model, InputType.INTERNAL_SEARCH);
        var expectedModel = createModel(MODEL_ID_WITH_TASK_TYPE, "api_key", null, null, InputType.INTERNAL_SEARCH);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(expectedModel));
    }

    public void testInputTypeNull_WhenModelIdDoesNotAllowTaskType() {
        var model = createModel("model", "api_key", null, null);
        var overriddenModel = GoogleAiStudioEmbeddingsModel.of(model, null);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(model));
    }

    public void testInputTypeUnspecified_WhenModelIdDoesNotAllowTaskType() {
        var model = createModel("model", "api_key", null, null);
        var overriddenModel = GoogleAiStudioEmbeddingsModel.of(model, InputType.UNSPECIFIED);
        MatcherAssert.assertThat(overriddenModel, Matchers.is(model));
    }
}
