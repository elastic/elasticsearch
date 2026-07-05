/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AlibabaCloudSearchCompletionServiceSettings> {

    private static final String TEST_SERVICE_ID = "test-service-id";
    private static final String INITIAL_TEST_SERVICE_ID = "initial-test-service-id";
    private static final String TEST_HOST = "test-host";
    private static final String INITIAL_TEST_HOST = "initial-test-host";
    private static final String TEST_WORKSPACE_NAME = "test-workspace-name";
    private static final String INITIAL_TEST_WORKSPACE_NAME = "initial-test-workspace-name";
    private static final String TEST_HTTP_SCHEMA = "https";
    private static final String INITIAL_TEST_HTTP_SCHEMA = "http";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public static AlibabaCloudSearchCompletionServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        return new AlibabaCloudSearchCompletionServiceSettings(commonSettings);
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new AlibabaCloudSearchCompletionServiceSettings(
            new AlibabaCloudSearchServiceSettings(
                INITIAL_TEST_SERVICE_ID,
                INITIAL_TEST_HOST,
                INITIAL_TEST_WORKSPACE_NAME,
                INITIAL_TEST_HTTP_SCHEMA,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
            )
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(
                Map.of(
                    AlibabaCloudSearchServiceSettings.HOST,
                    TEST_HOST,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    TEST_SERVICE_ID,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    TEST_WORKSPACE_NAME,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    TEST_HTTP_SCHEMA,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
                )
            )
        );

        assertThat(
            updatedServiceSettings,
            is(
                new AlibabaCloudSearchCompletionServiceSettings(
                    new AlibabaCloudSearchServiceSettings(
                        INITIAL_TEST_SERVICE_ID,
                        INITIAL_TEST_HOST,
                        INITIAL_TEST_WORKSPACE_NAME,
                        TEST_HTTP_SCHEMA,
                        new RateLimitSettings(TEST_RATE_LIMIT)
                    )
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new AlibabaCloudSearchCompletionServiceSettings(
            new AlibabaCloudSearchServiceSettings(
                INITIAL_TEST_SERVICE_ID,
                INITIAL_TEST_HOST,
                INITIAL_TEST_WORKSPACE_NAME,
                INITIAL_TEST_HTTP_SCHEMA,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
            )
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testFromMap_Success() {
        var serviceSettings = AlibabaCloudSearchCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AlibabaCloudSearchServiceSettings.HOST,
                    TEST_HOST,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    TEST_SERVICE_ID,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    TEST_WORKSPACE_NAME,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    TEST_HTTP_SCHEMA,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
                )
            ),
            null
        );

        assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchCompletionServiceSettings(
                    new AlibabaCloudSearchServiceSettings(
                        TEST_SERVICE_ID,
                        TEST_HOST,
                        TEST_WORKSPACE_NAME,
                        TEST_HTTP_SCHEMA,
                        new RateLimitSettings(TEST_RATE_LIMIT)
                    )
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<AlibabaCloudSearchCompletionServiceSettings> instanceReader() {
        return AlibabaCloudSearchCompletionServiceSettings::new;
    }

    @Override
    protected AlibabaCloudSearchCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AlibabaCloudSearchCompletionServiceSettings mutateInstance(AlibabaCloudSearchCompletionServiceSettings instance)
        throws IOException {
        return new AlibabaCloudSearchCompletionServiceSettings(
            randomValueOtherThan(instance.getCommonSettings(), AlibabaCloudSearchServiceSettingsTests::createRandom)
        );
    }

    public static Map<String, Object> getServiceSettingsMap(String serviceId, String host, String workspaceName) {
        var map = new HashMap<String, Object>();
        map.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, serviceId);
        map.put(AlibabaCloudSearchServiceSettings.HOST, host);
        map.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, workspaceName);
        return map;
    }

    @Override
    protected AlibabaCloudSearchCompletionServiceSettings mutateInstanceForVersion(
        AlibabaCloudSearchCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
