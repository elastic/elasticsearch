/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.rerank;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class HuggingFaceRerankServiceSettingsTests extends ESTestCase {

    private static final URI TEST_URI = ServiceUtils.createUri("https://test-uri.com");
    private static final URI INITIAL_TEST_URI = ServiceUtils.createUri("https://initial-test-uri.com");
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.URL,
                TEST_URI.toString(),
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var serviceSettings = createInitialServiceSettings().updateServiceSettings(settingsMap, TaskType.RERANK);

        assertThat(serviceSettings, is(new HuggingFaceRerankServiceSettings(TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = createInitialServiceSettings().updateServiceSettings(new HashMap<>(), TaskType.RERANK);

        assertThat(serviceSettings, is(createInitialServiceSettings()));
    }

    private static HuggingFaceRerankServiceSettings createInitialServiceSettings() {
        return new HuggingFaceRerankServiceSettings(INITIAL_TEST_URI, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));
    }
}
