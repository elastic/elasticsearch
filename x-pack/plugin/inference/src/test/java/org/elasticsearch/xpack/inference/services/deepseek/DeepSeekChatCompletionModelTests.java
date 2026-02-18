/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.hamcrest.MatcherAssert;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class DeepSeekChatCompletionModelTests extends ESTestCase {
    private static final URI TEST_URL = ServiceUtils.createUri("https://www.test.com");
    private static final URI INITIAL_TEST_URL = ServiceUtils.createUri("https://www.initial-test.com");
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(ServiceFields.URL, TEST_URL.toString(), ServiceFields.MODEL_ID, TEST_MODEL_ID)
        );

        var serviceSettings = createInitialDeepSeekServiceSettings().updateServiceSettings(settingsMap, TaskType.COMPLETION);

        MatcherAssert.assertThat(serviceSettings, is(new DeepSeekChatCompletionModel.DeepSeekServiceSettings(TEST_MODEL_ID, TEST_URL)));
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = createInitialDeepSeekServiceSettings().updateServiceSettings(new HashMap<>(), TaskType.COMPLETION);

        MatcherAssert.assertThat(serviceSettings, is(createInitialDeepSeekServiceSettings()));
    }

    private static DeepSeekChatCompletionModel.DeepSeekServiceSettings createInitialDeepSeekServiceSettings() {
        return new DeepSeekChatCompletionModel.DeepSeekServiceSettings(INITIAL_TEST_MODEL_ID, INITIAL_TEST_URL);
    }
}
