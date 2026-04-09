/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchServiceSettingsTests extends AbstractBWCWireSerializationTestCase<AlibabaCloudSearchServiceSettings> {
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

    /**
     * The created settings can have a url set to null.
     */
    public static AlibabaCloudSearchServiceSettings createRandom() {
        var model = randomAlphaOfLength(15);
        var host = randomAlphaOfLength(15);
        var workspaceName = randomAlphaOfLength(10);
        var httpSchema = randomBoolean() ? "https" : "http";
        return new AlibabaCloudSearchServiceSettings(model, host, workspaceName, httpSchema, RateLimitSettingsTests.createRandom());
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new AlibabaCloudSearchServiceSettings(
            INITIAL_TEST_SERVICE_ID,
            INITIAL_TEST_HOST,
            INITIAL_TEST_WORKSPACE_NAME,
            INITIAL_TEST_HTTP_SCHEMA,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            new HashMap<>(
                Map.of(
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    TEST_SERVICE_ID,
                    AlibabaCloudSearchServiceSettings.HOST,
                    TEST_HOST,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    TEST_WORKSPACE_NAME,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    TEST_HTTP_SCHEMA,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
                )
            ),
            new ValidationException()
        );

        assertThat(
            updatedServiceSettings,
            is(
                new AlibabaCloudSearchServiceSettings(
                    INITIAL_TEST_SERVICE_ID,
                    INITIAL_TEST_HOST,
                    INITIAL_TEST_WORKSPACE_NAME,
                    TEST_HTTP_SCHEMA,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new AlibabaCloudSearchServiceSettings(
            INITIAL_TEST_SERVICE_ID,
            INITIAL_TEST_HOST,
            INITIAL_TEST_WORKSPACE_NAME,
            INITIAL_TEST_HTTP_SCHEMA,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>(), new ValidationException());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testFromMap_Success() {
        var serviceSettings = AlibabaCloudSearchServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    TEST_SERVICE_ID,
                    AlibabaCloudSearchServiceSettings.HOST,
                    TEST_HOST,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    TEST_WORKSPACE_NAME,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    TEST_HTTP_SCHEMA
                )
            ),
            null,
            new ValidationException()
        );

        assertThat(
            serviceSettings,
            is(new AlibabaCloudSearchServiceSettings(TEST_SERVICE_ID, TEST_HOST, TEST_WORKSPACE_NAME, TEST_HTTP_SCHEMA, null))
        );
    }

    public void testFromMap_WithRateLimit() {
        var serviceSettings = AlibabaCloudSearchServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    TEST_SERVICE_ID,
                    AlibabaCloudSearchServiceSettings.HOST,
                    TEST_HOST,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    TEST_WORKSPACE_NAME,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    TEST_HTTP_SCHEMA,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
                )
            ),
            null,
            new ValidationException()
        );

        assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchServiceSettings(
                    TEST_SERVICE_ID,
                    TEST_HOST,
                    TEST_WORKSPACE_NAME,
                    TEST_HTTP_SCHEMA,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testXContent() throws IOException {
        var entity = new AlibabaCloudSearchServiceSettings(
            TEST_SERVICE_ID,
            TEST_HOST,
            TEST_WORKSPACE_NAME,
            TEST_HTTP_SCHEMA,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"service_id":"%s","host":"%s","workspace":"%s","http_schema":"%s","rate_limit":{"requests_per_minute":%d}}""",
                    TEST_SERVICE_ID,
                    TEST_HOST,
                    TEST_WORKSPACE_NAME,
                    TEST_HTTP_SCHEMA,
                    TEST_RATE_LIMIT
                )
            )
        );
    }

    public void testValidateHttpSchema_InvalidSchema_AddsValidationError() {
        var validationException = new ValidationException();
        AlibabaCloudSearchServiceSettings.validateHttpSchema("invalid-http-schema", validationException);
        assertThat(
            validationException.getMessage(),
            is("Validation Failed: 1: Invalid value for [http_schema]. Must be one of [https, http];")
        );
    }

    public void testValidateHttpSchema_HttpsSchema_Success() {
        var validationException = new ValidationException();
        AlibabaCloudSearchServiceSettings.validateHttpSchema("https", validationException);
        assertThat(validationException.validationErrors(), is(emptyCollectionOf(String.class)));
    }

    public void testValidateHttpSchema_HttpSchema_Success() {
        var validationException = new ValidationException();
        AlibabaCloudSearchServiceSettings.validateHttpSchema("http", validationException);
        assertThat(validationException.validationErrors(), is(emptyCollectionOf(String.class)));
    }

    @Override
    protected Writeable.Reader<AlibabaCloudSearchServiceSettings> instanceReader() {
        return AlibabaCloudSearchServiceSettings::new;
    }

    @Override
    protected AlibabaCloudSearchServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AlibabaCloudSearchServiceSettings mutateInstance(AlibabaCloudSearchServiceSettings instance) throws IOException {
        var serviceId = instance.modelId();
        var host = instance.getHost();
        var workspaceName = instance.getWorkspaceName();
        var httpSchema = instance.getHttpSchema();
        var rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 3)) {
            case 0 -> serviceId = randomValueOtherThan(serviceId, () -> randomAlphaOfLength(8));
            case 1 -> host = randomValueOtherThan(host, () -> randomAlphaOfLength(8));
            case 2 -> workspaceName = randomValueOtherThan(workspaceName, () -> randomAlphaOfLength(8));
            case 3 -> httpSchema = Objects.equals(httpSchema, "http") ? "https" : "http";
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AlibabaCloudSearchServiceSettings(serviceId, host, workspaceName, httpSchema, rateLimitSettings);
    }

    public static Map<String, Object> getServiceSettingsMap(String serviceId, String host, String workspaceName) {
        var map = new HashMap<String, Object>();
        map.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, serviceId);
        map.put(AlibabaCloudSearchServiceSettings.HOST, host);
        map.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, workspaceName);
        return map;
    }

    @Override
    protected AlibabaCloudSearchServiceSettings mutateInstanceForVersion(
        AlibabaCloudSearchServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
