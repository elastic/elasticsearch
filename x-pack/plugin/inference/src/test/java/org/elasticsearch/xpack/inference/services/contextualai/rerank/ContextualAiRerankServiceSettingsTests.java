/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class ContextualAiRerankServiceSettingsTests extends ESTestCase {

    private static final String TEST_URL = "https://www.test.com";
    private static final String INITIAL_TEST_URL = "https://www.initial-test.com";
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.URL,
                TEST_URL,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var serviceSettings = new ContextualAiRerankServiceSettings(
            ServiceUtils.createUri(INITIAL_TEST_URL),
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        ).updateServiceSettings(settingsMap, TaskType.RERANK);

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new ContextualAiRerankServiceSettings(
                    ServiceUtils.createUri(TEST_URL),
                    TEST_MODEL_ID,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = new ContextualAiRerankServiceSettings(
            ServiceUtils.createUri(INITIAL_TEST_URL),
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)

        ).updateServiceSettings(new HashMap<>(), TaskType.RERANK);

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new ContextualAiRerankServiceSettings(
                    ServiceUtils.createUri(INITIAL_TEST_URL),
                    INITIAL_TEST_MODEL_ID,
                    new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)

                )
            )
        );
    }
}
