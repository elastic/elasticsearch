/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class IbmWatsonxRerankServiceSettingsTests extends ESTestCase {

    private static final URI TEST_URI = ServiceUtils.createUri("https://test-uri.com");
    private static final URI INITIAL_TEST_URI = ServiceUtils.createUri("https://initial-test-uri.com");
    private static final String TEST_API_VERSION = "test-api-version";
    private static final String INITIAL_TEST_API_VERSION = "initial-test-api-version";
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final String TEST_PROJECT_ID = "test-project-id";
    private static final String INITIAL_TEST_PROJECT_ID = "initial-test-project-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.URL,
                TEST_URI.toString(),
                IbmWatsonxServiceFields.API_VERSION,
                TEST_API_VERSION,
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                IbmWatsonxServiceFields.PROJECT_ID,
                TEST_PROJECT_ID,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var serviceSettings = createInitialServiceSettings().updateServiceSettings(settingsMap, TaskType.COMPLETION);

        assertThat(serviceSettings, is(createInitialSettings()));
    }

    private static IbmWatsonxRerankServiceSettings createInitialSettings() {
        return new IbmWatsonxRerankServiceSettings(
            TEST_URI,
            TEST_API_VERSION,
            TEST_MODEL_ID,
            TEST_PROJECT_ID,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = createInitialServiceSettings().updateServiceSettings(new HashMap<>(), TaskType.COMPLETION);

        assertThat(serviceSettings, is(createInitialServiceSettings()));
    }

    private static IbmWatsonxRerankServiceSettings createInitialServiceSettings() {
        return new IbmWatsonxRerankServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_API_VERSION,
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_PROJECT_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
    }

}
