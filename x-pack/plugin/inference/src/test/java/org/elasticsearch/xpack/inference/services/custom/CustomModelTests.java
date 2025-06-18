/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.apache.http.HttpHeaders;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.custom.response.CustomResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.ErrorResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.TextEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CustomModelTests extends ESTestCase {
    private static final String taskSettingsKey = "test_taskSettings_key";
    private static final String taskSettingsValue = "test_taskSettings_value";

    private static final String secretSettingsKey = "test_secret_key";
    private static final SecureString secretSettingsValue = new SecureString("test_secret_value".toCharArray());
    private static final String url = "http://www.abc.com";

    public void testOverride_DoesNotModifiedFields_TaskSettingsIsEmpty() {
        var model = createModel(
            "service",
            TaskType.TEXT_EMBEDDING,
            CustomServiceSettingsTests.createRandom(),
            CustomTaskSettingsTests.createRandom(),
            CustomSecretSettingsTests.createRandom()
        );

        var overriddenModel = CustomModel.of(model, Map.of());
        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public void testOverride() {
        var model = createModel(
            "service",
            TaskType.TEXT_EMBEDDING,
            CustomServiceSettingsTests.createRandom(),
            new CustomTaskSettings(Map.of("key", "value")),
            CustomSecretSettingsTests.createRandom()
        );

        var overriddenModel = CustomModel.of(
            model,
            new HashMap<>(Map.of(CustomTaskSettings.PARAMETERS, new HashMap<>(Map.of("key", "different_value"))))
        );
        MatcherAssert.assertThat(
            overriddenModel,
            is(
                createModel(
                    "service",
                    TaskType.TEXT_EMBEDDING,
                    model.getServiceSettings(),
                    new CustomTaskSettings(Map.of("key", "different_value")),
                    model.getSecretSettings()
                )
            )
        );
    }

    public static CustomModel createModel(
        String inferenceId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets
    ) {
        return new CustomModel(inferenceId, taskType, CustomService.NAME, serviceSettings, taskSettings, secrets, null);
    }

    public static CustomModel createModel(
        String inferenceId,
        TaskType taskType,
        CustomServiceSettings serviceSettings,
        CustomTaskSettings taskSettings,
        @Nullable CustomSecretSettings secretSettings
    ) {
        return new CustomModel(inferenceId, taskType, CustomService.NAME, serviceSettings, taskSettings, secretSettings);
    }

    public static CustomModel getTestModel() {
        return getTestModel(TaskType.TEXT_EMBEDDING, new TextEmbeddingResponseParser("$.result.embeddings[*].embedding"));
    }

    public static CustomModel getTestModel(TaskType taskType, CustomResponseParser responseParser) {
        return getTestModel(taskType, responseParser, url);
    }

    public static CustomModel getTestModel(TaskType taskType, CustomResponseParser responseParser, String url) {
        var inferenceId = "inference_id";
        Integer dims = 1536;
        Integer maxInputTokens = 512;
        Map<String, String> headers = Map.of(HttpHeaders.AUTHORIZATION, "${" + secretSettingsKey + "}");
        String requestContentString = "\"input\":\"${input}\"";

        CustomServiceSettings serviceSettings = new CustomServiceSettings(
            new CustomServiceSettings.TextEmbeddingSettings(
                SimilarityMeasure.DOT_PRODUCT,
                dims,
                maxInputTokens,
                DenseVectorFieldMapper.ElementType.FLOAT
            ),
            url,
            headers,
            QueryParameters.EMPTY,
            requestContentString,
            responseParser,
            new RateLimitSettings(10_000),
            new ErrorResponseParser("$.error.message", inferenceId)
        );

        CustomTaskSettings taskSettings = new CustomTaskSettings(Map.of(taskSettingsKey, taskSettingsValue));
        CustomSecretSettings secretSettings = new CustomSecretSettings(Map.of(secretSettingsKey, secretSettingsValue));

        return CustomModelTests.createModel(inferenceId, taskType, serviceSettings, taskSettings, secretSettings);
    }
}
