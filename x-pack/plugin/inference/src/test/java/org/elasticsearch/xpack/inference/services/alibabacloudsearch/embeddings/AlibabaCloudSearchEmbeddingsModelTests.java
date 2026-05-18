/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.AlibabaCloudSearchUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests;
import org.hamcrest.MatcherAssert;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchEmbeddingsModelTests extends ESTestCase {
    public void testOverride() {
        AlibabaCloudSearchEmbeddingsTaskSettings taskSettings = AlibabaCloudSearchEmbeddingsTaskSettingsTests.createRandom();
        var model = createModel(
            "service",
            TaskType.TEXT_EMBEDDING,
            AlibabaCloudSearchEmbeddingsServiceSettingsTests.createRandom(),
            taskSettings,
            DefaultSecretSettingsTests.createRandom()
        );

        var overriddenModel = AlibabaCloudSearchEmbeddingsModel.of(model, Map.of());
        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public static AlibabaCloudSearchEmbeddingsModel createModel(
        String modelId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets
    ) {
        return new AlibabaCloudSearchEmbeddingsModel(
            modelId,
            taskType,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            serviceSettings,
            taskSettings,
            null,
            secrets,
            null
        );
    }

    public static AlibabaCloudSearchEmbeddingsModel createModel(
        String modelId,
        TaskType taskType,
        AlibabaCloudSearchEmbeddingsServiceSettings serviceSettings,
        AlibabaCloudSearchEmbeddingsTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        return new AlibabaCloudSearchEmbeddingsModel(
            modelId,
            taskType,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            serviceSettings,
            taskSettings,
            null,
            secretSettings
        );
    }
}
