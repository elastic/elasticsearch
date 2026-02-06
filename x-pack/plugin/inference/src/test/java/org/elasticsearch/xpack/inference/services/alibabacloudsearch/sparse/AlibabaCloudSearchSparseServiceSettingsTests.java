/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchSparseServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AlibabaCloudSearchSparseServiceSettings> {

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

    public static AlibabaCloudSearchSparseServiceSettings createRandom() {
        var commonSettings = AlibabaCloudSearchServiceSettingsTests.createRandom();
        return new AlibabaCloudSearchSparseServiceSettings(commonSettings);
    }

    public void testUpdateServiceSettings_AllFields_Success() {
        var serviceSettings = new AlibabaCloudSearchSparseServiceSettings(
            new AlibabaCloudSearchServiceSettings(
                INITIAL_TEST_SERVICE_ID,
                INITIAL_TEST_HOST,
                INITIAL_TEST_WORKSPACE_NAME,
                INITIAL_TEST_HTTP_SCHEMA,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
            )
        ).updateServiceSettings(
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
            TaskType.COMPLETION
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchSparseServiceSettings(
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

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = new AlibabaCloudSearchSparseServiceSettings(
            new AlibabaCloudSearchServiceSettings(
                INITIAL_TEST_SERVICE_ID,
                INITIAL_TEST_HOST,
                INITIAL_TEST_WORKSPACE_NAME,
                INITIAL_TEST_HTTP_SCHEMA,
                new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
            )
        ).updateServiceSettings(new HashMap<>(), TaskType.CHAT_COMPLETION);

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchSparseServiceSettings(
                    new AlibabaCloudSearchServiceSettings(
                        INITIAL_TEST_SERVICE_ID,
                        INITIAL_TEST_HOST,
                        INITIAL_TEST_WORKSPACE_NAME,
                        INITIAL_TEST_HTTP_SCHEMA,
                        new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
                    )
                )
            )
        );
    }

    public void testFromMap_Success() {
        var serviceSettings = AlibabaCloudSearchSparseServiceSettings.fromMap(
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

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchSparseServiceSettings(
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
    protected Writeable.Reader<AlibabaCloudSearchSparseServiceSettings> instanceReader() {
        return AlibabaCloudSearchSparseServiceSettings::new;
    }

    @Override
    protected AlibabaCloudSearchSparseServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AlibabaCloudSearchSparseServiceSettings mutateInstance(AlibabaCloudSearchSparseServiceSettings instance) throws IOException {
        var serviceId = instance.modelId();
        var host = instance.getCommonSettings().getHost();
        var workspaceName = instance.getCommonSettings().getWorkspaceName();
        var httpSchema = instance.getCommonSettings().getHttpSchema();
        var rateLimitSettings = instance.getCommonSettings().rateLimitSettings();

        switch (between(0, 3)) {
            case 0 -> serviceId = randomValueOtherThan(serviceId, () -> randomAlphaOfLength(8));
            case 1 -> host = randomValueOtherThan(host, () -> randomAlphaOfLength(8));
            case 2 -> workspaceName = randomValueOtherThan(workspaceName, () -> randomAlphaOfLength(8));
            case 3 -> httpSchema = Objects.equals(httpSchema, "http") ? "https" : "http";
            // TODO: check why rate limit settings are not included in equals and hashcode methods
            // case 4 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AlibabaCloudSearchSparseServiceSettings(
            new AlibabaCloudSearchServiceSettings(serviceId, host, workspaceName, httpSchema, rateLimitSettings)
        );
    }

    public static Map<String, Object> getServiceSettingsMap(String serviceId, String host, String workspaceName) {
        return AlibabaCloudSearchServiceSettingsTests.getServiceSettingsMap(serviceId, host, workspaceName);
    }

    @Override
    protected AlibabaCloudSearchSparseServiceSettings mutateInstanceForVersion(
        AlibabaCloudSearchSparseServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
