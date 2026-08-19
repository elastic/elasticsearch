/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AnthropicChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    AnthropicChatCompletionServiceSettings> {
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    private static final String TEST_URL = "https://custom.anthropic.example.com/v1/messages";

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new AnthropicChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(getServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT));

        assertThat(
            updatedServiceSettings,
            is(new AnthropicChatCompletionServiceSettings(INITIAL_TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new AnthropicChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_PreservesUrl() {
        var url = URI.create(TEST_URL);
        var originalServiceSettings = new AnthropicChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            url,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(getServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT));

        assertThat(
            updatedServiceSettings,
            is(new AnthropicChatCompletionServiceSettings(INITIAL_TEST_MODEL_ID, url, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_UrlCannotBeChanged() {
        var originalUrl = URI.create(TEST_URL);
        var originalServiceSettings = new AnthropicChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            originalUrl,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            getServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT, "https://other.example.com/v1/messages")
        );

        assertThat(
            updatedServiceSettings,
            is(new AnthropicChatCompletionServiceSettings(INITIAL_TEST_MODEL_ID, originalUrl, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_AllFields_Success() {
        var serviceSettings = AnthropicChatCompletionServiceSettings.fromMap(
            getServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new AnthropicChatCompletionServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testFromMap_WithUrl_Success() {
        var map = getServiceSettingsMap(TEST_MODEL_ID, TEST_RATE_LIMIT);
        map.put(ServiceFields.URL, TEST_URL);

        var serviceSettings = AnthropicChatCompletionServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(
            serviceSettings,
            is(new AnthropicChatCompletionServiceSettings(TEST_MODEL_ID, URI.create(TEST_URL), new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingModelId_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> AnthropicChatCompletionServiceSettings.fromMap(
                getServiceSettingsMap(null, TEST_RATE_LIMIT),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];")
        );
    }

    public void testFromMap_MissingRateLimit_Success() {
        var serviceSettings = AnthropicChatCompletionServiceSettings.fromMap(
            getServiceSettingsMap(TEST_MODEL_ID, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new AnthropicChatCompletionServiceSettings(TEST_MODEL_ID, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new AnthropicChatCompletionServiceSettings(TEST_MODEL_ID, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":50}}""", TEST_MODEL_ID)));
    }

    public void testToXContent_WritesAllValues_WithCustomRateLimit() throws IOException {
        var serviceSettings = new AnthropicChatCompletionServiceSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":%d}}""", TEST_MODEL_ID, TEST_RATE_LIMIT)));
    }

    public void testToXContent_WritesUrl_WhenSet() throws IOException {
        var serviceSettings = new AnthropicChatCompletionServiceSettings(
            TEST_MODEL_ID,
            URI.create(TEST_URL),
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","url":"%s","rate_limit":{"requests_per_minute":%d}}""", TEST_MODEL_ID, TEST_URL, TEST_RATE_LIMIT)));
    }

    @Override
    protected Writeable.Reader<AnthropicChatCompletionServiceSettings> instanceReader() {
        return AnthropicChatCompletionServiceSettings::new;
    }

    @Override
    protected AnthropicChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected AnthropicChatCompletionServiceSettings mutateInstance(AnthropicChatCompletionServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var url = instance.url();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomIntBetween(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> url = randomValueOtherThan(
                url,
                () -> randomBoolean() ? null : createUri("https://" + randomAlphaOfLength(8) + ".example")
            );
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AnthropicChatCompletionServiceSettings(modelId, url, rateLimitSettings);
    }

    private static AnthropicChatCompletionServiceSettings createRandom() {
        var url = randomBoolean() ? null : createUri("https://" + randomAlphaOfLength(8) + ".example");
        return new AnthropicChatCompletionServiceSettings(randomAlphaOfLength(8), url, RateLimitSettingsTests.createRandom());
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String modelId, @Nullable Integer rateLimit) {
        return getServiceSettingsMap(modelId, rateLimit, null);
    }

    public static Map<String, Object> getServiceSettingsMap(@Nullable String modelId, @Nullable Integer rateLimit, @Nullable String url) {
        var map = new HashMap<String, Object>();

        if (modelId != null) {
            map.put(ServiceFields.MODEL_ID, modelId);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }

        return map;
    }

    @Override
    protected AnthropicChatCompletionServiceSettings mutateInstanceForVersion(
        AnthropicChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        if (version.supports(AnthropicChatCompletionServiceSettings.ANTHROPIC_COMPLETION_URL_ADDED)) {
            return instance;
        }

        return new AnthropicChatCompletionServiceSettings(instance.modelId(), null, instance.rateLimitSettings());
    }
}
