/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.alibabacloudsearch.AlibabaCloudSearchUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests;
import org.hamcrest.MatcherAssert;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchSparseModelTests extends ESTestCase {
    public void testOverride() {
        AlibabaCloudSearchSparseTaskSettings taskSettings = AlibabaCloudSearchSparseTaskSettingsTests.createRandom();
        var model = createModel(
            "service",
            TaskType.TEXT_EMBEDDING,
            AlibabaCloudSearchSparseServiceSettingsTests.createRandom(),
            taskSettings,
            DefaultSecretSettingsTests.createRandom()
        );

        var overriddenModel = AlibabaCloudSearchSparseModel.of(model, Map.of(), taskSettings.getInputType());
        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public static AlibabaCloudSearchSparseModel createModel(
        String modelId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets
    ) {
        return new AlibabaCloudSearchSparseModel(
            modelId,
            taskType,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            serviceSettings,
            taskSettings,
            secrets,
            null
        );
    }

    public static AlibabaCloudSearchSparseModel createModel(
        String modelId,
        TaskType taskType,
        AlibabaCloudSearchSparseServiceSettings serviceSettings,
        AlibabaCloudSearchSparseTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        return new AlibabaCloudSearchSparseModel(
            modelId,
            taskType,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            serviceSettings,
            taskSettings,
            secretSettings
        );
    }
}
