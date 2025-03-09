/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import io.netty.handler.codec.http.HttpMethod;

import org.apache.http.HttpHeaders;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.custom.CustomUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class CustomModelTests extends ESTestCase {
    public static String taskSettingsKey = "test_taskSettings_key";
    public static String taskSettingsValue = "test_taskSettings_value";

    public static String secretSettingsKey = "test_secret_key";
    public static String secretSettingsValue = "test_secret_value";
    public static String url = "http://www.abc.com";
    public static String path = "/endpoint";

    public void testOverride() {
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

    public static CustomModel createModel(
        String modelId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets
    ) {
        return new CustomModel(modelId, taskType, CustomUtils.SERVICE_NAME, serviceSettings, taskSettings, secrets, null);
    }

    public static CustomModel createModel(
        String modelId,
        TaskType taskType,
        CustomServiceSettings serviceSettings,
        CustomTaskSettings taskSettings,
        @Nullable CustomSecretSettings secretSettings
    ) {
        return new CustomModel(modelId, taskType, CustomUtils.SERVICE_NAME, serviceSettings, taskSettings, secretSettings);
    }

    public static CustomModel getTestModel() {
        TaskType taskType = TaskType.TEXT_EMBEDDING;
        Map<String, Object> jsonParserMap = new HashMap<>(
            Map.of(CustomServiceSettings.TEXT_EMBEDDING_PARSER_EMBEDDINGS, "$.result.embeddings[*].embedding")
        );
        return getTestModel(taskType, jsonParserMap);
    }

    public static CustomModel getTestModel(TaskType taskType, Map<String, Object> jsonParserMap) {
        // service settings
        Integer dims = 1536;
        Integer maxInputTokens = 512;
        String description = "test fromMap";
        String version = "v1";
        String serviceType = taskType.toString();
        String method = HttpMethod.POST.name();
        String queryString = "?query=${" + taskSettingsKey + "}";
        Map<String, Object> headers = Map.of(HttpHeaders.AUTHORIZATION, "${" + secretSettingsKey + "}");
        String requestFormat = CustomServiceSettings.REQUEST_FORMAT_STRING;
        String requestContentString = "\"input\":\"${input}\"";

        ResponseJsonParser responseJsonParser = new ResponseJsonParser(taskType, jsonParserMap, new ValidationException());

        CustomServiceSettings serviceSettings = new CustomServiceSettings(
            SimilarityMeasure.DOT_PRODUCT,
            dims,
            maxInputTokens,
            description,
            version,
            serviceType,
            url,
            path,
            method,
            queryString,
            headers,
            requestFormat,
            null,
            requestContentString,
            responseJsonParser,
            new RateLimitSettings(10_000)
        );

        // task settings
        CustomTaskSettings taskSettings = new CustomTaskSettings(Map.of(taskSettingsKey, taskSettingsValue), false);

        // secret settings
        CustomSecretSettings secretSettings = new CustomSecretSettings(Map.of(secretSettingsKey, secretSettingsValue));

        return CustomModelTests.createModel("service", taskType, serviceSettings, taskSettings, secretSettings);
    }
}
