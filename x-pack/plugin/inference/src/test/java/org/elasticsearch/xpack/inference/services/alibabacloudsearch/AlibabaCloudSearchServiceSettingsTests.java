/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AlibabaCloudSearchServiceSettingsTests extends AbstractWireSerializingTestCase<AlibabaCloudSearchServiceSettings> {
    /**
     * The created settings can have a url set to null.
     */
    public static AlibabaCloudSearchServiceSettings createRandom() {
        var url = randomBoolean() ? randomAlphaOfLength(15) : null;
        return createRandom(url);
    }

    public static AlibabaCloudSearchServiceSettings createRandom(String url) {
        var model = randomAlphaOfLength(15);
        String host = randomAlphaOfLength(15);
        String workspaceName = randomAlphaOfLength(10);
        String httpSchema = "https";
        return new AlibabaCloudSearchServiceSettings(
            ServiceUtils.createOptionalUri(url),
            model,
            host,
            workspaceName,
            httpSchema,
            RateLimitSettingsTests.createRandom()
        );
    }

    public void testFromMap() throws URISyntaxException {
        var url = "https://www.abc.com";
        var model = "model";
        var host = "host";
        var workspaceName = "default";
        var httpSchema = "https";
        var serviceSettings = AlibabaCloudSearchServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    model,
                    AlibabaCloudSearchServiceSettings.HOST,
                    host,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    workspaceName,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    httpSchema
                )
            ),
            null
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(new AlibabaCloudSearchServiceSettings(ServiceUtils.createUri(url), model, host, workspaceName, httpSchema, null))
        );
    }

    public void testFromMap_WithRateLimit() {
        var url = "https://www.abc.com";
        var model = "model";
        var host = "host";
        var workspaceName = "default";
        var httpSchema = "https";
        var serviceSettings = AlibabaCloudSearchServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.URL,
                    url,
                    AlibabaCloudSearchServiceSettings.SERVICE_ID,
                    model,
                    AlibabaCloudSearchServiceSettings.HOST,
                    host,
                    AlibabaCloudSearchServiceSettings.WORKSPACE_NAME,
                    workspaceName,
                    AlibabaCloudSearchServiceSettings.HTTP_SCHEMA_NAME,
                    httpSchema,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3))
                )
            ),
            null
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new AlibabaCloudSearchServiceSettings(
                    ServiceUtils.createUri(url),
                    model,
                    host,
                    workspaceName,
                    httpSchema,
                    new RateLimitSettings(3)
                )
            )
        );
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AlibabaCloudSearchServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, "")),
                ConfigurationParseContext.PERSISTENT
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    ServiceFields.URL
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var url = "https://www.abc^.com";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AlibabaCloudSearchServiceSettings.fromMap(
                new HashMap<>(Map.of(ServiceFields.URL, url)),
                ConfigurationParseContext.PERSISTENT
            )
        );

        MatcherAssert.assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s]", url, ServiceFields.URL)
            )
        );
    }

    public void testXContent() throws IOException {
        var entity = new AlibabaCloudSearchServiceSettings(null, "model_id_name", "host_name", "workspace_name", null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"service_id":"model_id_name","host":"host_name","workspace":"workspace_name","rate_limit":{"requests_per_minute":1000}}"""));
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
        return null;
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String url, String serviceId, String host, String workspaceName) {
        var map = new HashMap<String, Object>();
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        map.put(AlibabaCloudSearchServiceSettings.SERVICE_ID, serviceId);
        map.put(AlibabaCloudSearchServiceSettings.HOST, host);
        map.put(AlibabaCloudSearchServiceSettings.WORKSPACE_NAME, workspaceName);
        return map;
    }
}
