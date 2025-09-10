/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.ENDPOINT_TYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TARGET_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType.TOKEN;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider.COHERE;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioRerankServiceSettingsTests extends AbstractBWCWireSerializationTestCase<AzureAiStudioRerankServiceSettings> {
    private static final String TARGET_URI = "http://testtarget.local";

    public void testFromMap_Request_CreatesSettingsCorrectly() {
        final var serviceSettings = AzureAiStudioRerankServiceSettings.fromMap(
            createRequestSettingsMap(TARGET_URI, COHERE.name(), TOKEN.name()),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new AzureAiStudioRerankServiceSettings(TARGET_URI, COHERE, TOKEN, null)));
    }

    public void testFromMap_RequestWithRateLimit_CreatesSettingsCorrectly() {
        final var settingsMap = createRequestSettingsMap(TARGET_URI, COHERE.name(), TOKEN.name());
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 3)));

        final var serviceSettings = AzureAiStudioRerankServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new AzureAiStudioRerankServiceSettings(TARGET_URI, COHERE, TOKEN, new RateLimitSettings(3))));
    }

    public void testFromMap_Persistent_CreatesSettingsCorrectly() {
        final var serviceSettings = AzureAiStudioRerankServiceSettings.fromMap(
            createRequestSettingsMap(TARGET_URI, COHERE.name(), TOKEN.name()),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new AzureAiStudioRerankServiceSettings(TARGET_URI, COHERE, TOKEN, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        final var settings = new AzureAiStudioRerankServiceSettings(TARGET_URI, COHERE, TOKEN, new RateLimitSettings(3));
        final XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        final String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"target":"http://testtarget.local","provider":"cohere","endpoint_type":"token",""" + """
            "rate_limit":{"requests_per_minute":3}}"""));
    }

    public void testToFilteredXContent_WritesAllValues() throws IOException {
        final var settings = new AzureAiStudioRerankServiceSettings(TARGET_URI, COHERE, TOKEN, new RateLimitSettings(3));
        final XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        final var filteredXContent = settings.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        final String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"target":"http://testtarget.local","provider":"cohere","endpoint_type":"token",""" + """
            "rate_limit":{"requests_per_minute":3}}"""));
    }

    public static HashMap<String, Object> createRequestSettingsMap(String target, String provider, String endpointType) {
        return new HashMap<>(Map.of(TARGET_FIELD, target, PROVIDER_FIELD, provider, ENDPOINT_TYPE_FIELD, endpointType));
    }

    @Override
    protected Writeable.Reader<AzureAiStudioRerankServiceSettings> instanceReader() {
        return AzureAiStudioRerankServiceSettings::new;
    }

    @Override
    protected AzureAiStudioRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureAiStudioRerankServiceSettings mutateInstance(AzureAiStudioRerankServiceSettings instance) throws IOException {
        return randomValueOtherThan(instance, AzureAiStudioRerankServiceSettingsTests::createRandom);
    }

    @Override
    protected AzureAiStudioRerankServiceSettings mutateInstanceForVersion(
        AzureAiStudioRerankServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static AzureAiStudioRerankServiceSettings createRandom() {
        return new AzureAiStudioRerankServiceSettings(
            randomAlphaOfLength(10),
            randomFrom(AzureAiStudioProvider.values()),
            randomFrom(AzureAiStudioEndpointType.values()),
            RateLimitSettingsTests.createRandom()
        );
    }
}
