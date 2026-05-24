/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.AlibabaCloudSearchUtils;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.hamcrest.MatcherAssert;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchCompletionModelTests extends ESTestCase {
    public void testOverride() {
        AlibabaCloudSearchCompletionTaskSettings taskSettings = AlibabaCloudSearchCompletionTaskSettingsTests.createRandom();
        var model = createModel(
            "service",
            TaskType.COMPLETION,
            AlibabaCloudSearchCompletionServiceSettingsTests.createRandom(),
            taskSettings,
            null
        );

        var overriddenModel = AlibabaCloudSearchCompletionModel.of(model, Map.of());
        MatcherAssert.assertThat(overriddenModel, is(model));
    }

    public static AlibabaCloudSearchCompletionModel createModel(
        String modelId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets
    ) {
        return new AlibabaCloudSearchCompletionModel(
            modelId,
            taskType,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            serviceSettings,
            taskSettings,
            secrets,
            null
        );
    }

    public static AlibabaCloudSearchCompletionModel createModel(
        String modelId,
        TaskType taskType,
        AlibabaCloudSearchCompletionServiceSettings serviceSettings,
        AlibabaCloudSearchCompletionTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        return new AlibabaCloudSearchCompletionModel(
            modelId,
            taskType,
            AlibabaCloudSearchUtils.SERVICE_NAME,
            serviceSettings,
            taskSettings,
            secretSettings
        );
    }
}
