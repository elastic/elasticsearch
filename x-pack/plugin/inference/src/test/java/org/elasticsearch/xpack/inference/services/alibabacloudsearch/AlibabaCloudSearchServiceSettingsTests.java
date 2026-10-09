/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
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

import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchServiceSettingsTests extends AbstractBWCWireSerializationTestCase<AlibabaCloudSearchServiceSettings> {
    private static final String TEST_SERVICE_ID = "test-service-id";
    private static final String TEST_HOST = "test-host";
    private static final String TEST_WORKSPACE_NAME = "test-workspace-name";
    private static final String TEST_HTTP_SCHEMA = "https";
    private static final int TEST_RATE_LIMIT = 20;

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

    public void testValidateHttpSchema_InvalidSchema_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> AlibabaCloudSearchServiceSettings.validateHttpSchema("invalid-http-schema")
        );
        assertThat(thrownException.getMessage(), is("Invalid value for [http_schema]. Must be one of [https, http]"));
    }

    public void testValidateHttpSchema_ValidOrAbsentSchema_Success() {
        AlibabaCloudSearchServiceSettings.validateHttpSchema("https");
        AlibabaCloudSearchServiceSettings.validateHttpSchema("http");
        AlibabaCloudSearchServiceSettings.validateHttpSchema(null);
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

        switch (between(0, 4)) {
            case 0 -> serviceId = randomValueOtherThan(serviceId, () -> randomAlphaOfLength(8));
            case 1 -> host = randomValueOtherThan(host, () -> randomAlphaOfLength(8));
            case 2 -> workspaceName = randomValueOtherThan(workspaceName, () -> randomAlphaOfLength(8));
            case 3 -> httpSchema = Objects.equals(httpSchema, "http") ? "https" : "http";
            case 4 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
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
