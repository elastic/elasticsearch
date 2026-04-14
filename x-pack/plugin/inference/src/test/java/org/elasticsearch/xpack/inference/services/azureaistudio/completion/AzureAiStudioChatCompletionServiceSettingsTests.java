/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.ENDPOINT_TYPE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.PROVIDER_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TARGET_FIELD;
import static org.hamcrest.Matchers.is;

public class AzureAiStudioChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AzureAiStudioChatCompletionServiceSettings> {
    private static final String TEST_TARGET = "http://sometarget.local";
    private static final String INITIAL_TEST_TARGET = "http://initialtarget.local";
    private static final AzureAiStudioProvider TEST_PROVIDER = AzureAiStudioProvider.OPENAI;
    private static final AzureAiStudioProvider INITIAL_TEST_PROVIDER = AzureAiStudioProvider.MISTRAL;
    private static final AzureAiStudioEndpointType TEST_ENDPOINT_TYPE = AzureAiStudioEndpointType.TOKEN;
    private static final AzureAiStudioEndpointType INITIAL_TEST_ENDPOINT_TYPE = AzureAiStudioEndpointType.REALTIME;
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final int DEFAULT_RATE_LIMIT = 240;

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new AzureAiStudioChatCompletionServiceSettings(
            INITIAL_TEST_TARGET,
            INITIAL_TEST_PROVIDER,
            INITIAL_TEST_ENDPOINT_TYPE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            createRequestSettingsMap(TEST_TARGET, TEST_PROVIDER.toString(), TEST_ENDPOINT_TYPE.toString(), TEST_RATE_LIMIT)
        );

        assertThat(
            updatedServiceSettings,
            is(
                new AzureAiStudioChatCompletionServiceSettings(
                    INITIAL_TEST_TARGET,
                    INITIAL_TEST_PROVIDER,
                    INITIAL_TEST_ENDPOINT_TYPE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new AzureAiStudioChatCompletionServiceSettings(
            INITIAL_TEST_TARGET,
            INITIAL_TEST_PROVIDER,
            INITIAL_TEST_ENDPOINT_TYPE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testFromMap_Request_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var serviceSettings = AzureAiStudioChatCompletionServiceSettings.fromMap(
            createRequestSettingsMap(TEST_TARGET, TEST_PROVIDER.toString(), TEST_ENDPOINT_TYPE.toString(), null),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioChatCompletionServiceSettings(
                    TEST_TARGET,
                    TEST_PROVIDER,
                    TEST_ENDPOINT_TYPE,
                    new RateLimitSettings(DEFAULT_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_Request_AllFields_CreatesSettingsCorrectly() {
        var settingsMap = createRequestSettingsMap(TEST_TARGET, TEST_PROVIDER.toString(), TEST_ENDPOINT_TYPE.toString(), TEST_RATE_LIMIT);

        var serviceSettings = AzureAiStudioChatCompletionServiceSettings.fromMap(settingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new AzureAiStudioChatCompletionServiceSettings(
                    TEST_TARGET,
                    TEST_PROVIDER,
                    TEST_ENDPOINT_TYPE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var settings = new AzureAiStudioChatCompletionServiceSettings(
            TEST_TARGET,
            TEST_PROVIDER,
            TEST_ENDPOINT_TYPE,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        settings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
              "target": "%s",
              "provider": "%s",
              "endpoint_type": "%s",
              "rate_limit": {
                "requests_per_minute": %d
              }
            }
            """, TEST_TARGET, TEST_PROVIDER, TEST_ENDPOINT_TYPE, TEST_RATE_LIMIT))));
    }

    public void testToFilteredXContent_WritesAllValues() throws IOException {
        var settings = new AzureAiStudioChatCompletionServiceSettings(
            TEST_TARGET,
            TEST_PROVIDER,
            TEST_ENDPOINT_TYPE,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        var filteredXContent = settings.getFilteredXContentObject();
        filteredXContent.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
              "target": "%s",
              "provider": "%s",
              "endpoint_type": "%s",
              "rate_limit": {
                "requests_per_minute": %d
              }
            }
            """, TEST_TARGET, TEST_PROVIDER, TEST_ENDPOINT_TYPE, TEST_RATE_LIMIT))));
    }

    public static HashMap<String, Object> createRequestSettingsMap(
        String target,
        String provider,
        String endpointType,
        @Nullable Integer rateLimit
    ) {
        var result = new HashMap<String, Object>();
        result.put(TARGET_FIELD, target);
        result.put(PROVIDER_FIELD, provider);
        result.put(ENDPOINT_TYPE_FIELD, endpointType);
        if (rateLimit != null) {
            result.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return result;
    }

    @Override
    protected Writeable.Reader<AzureAiStudioChatCompletionServiceSettings> instanceReader() {
        return AzureAiStudioChatCompletionServiceSettings::new;
    }

    @Override
    protected AzureAiStudioChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AzureAiStudioChatCompletionServiceSettings mutateInstance(AzureAiStudioChatCompletionServiceSettings instance)
        throws IOException {
        var target = instance.target();
        var provider = instance.provider();
        var endpointType = instance.endpointType();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(3)) {
            case 0 -> target = randomValueOtherThan(target, () -> randomAlphaOfLength(10));
            case 1 -> provider = randomValueOtherThan(provider, () -> randomFrom(AzureAiStudioProvider.values()));
            case 2 -> endpointType = randomValueOtherThan(endpointType, () -> randomFrom(AzureAiStudioEndpointType.values()));
            case 3 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AzureAiStudioChatCompletionServiceSettings(target, provider, endpointType, rateLimitSettings);
    }

    @Override
    protected AzureAiStudioChatCompletionServiceSettings mutateInstanceForVersion(
        AzureAiStudioChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static AzureAiStudioChatCompletionServiceSettings createRandom() {
        return new AzureAiStudioChatCompletionServiceSettings(
            randomAlphaOfLength(10),
            randomFrom(AzureAiStudioProvider.values()),
            randomFrom(AzureAiStudioEndpointType.values()),
            RateLimitSettingsTests.createRandom()
        );
    }
}
