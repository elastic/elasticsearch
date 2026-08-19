/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class FireworksAiChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    FireworksAiChatCompletionServiceSettings> {

    private static final String TEST_MODEL_ID = "accounts/fireworks/models/llama-v3p1-8b-instruct";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    private static final URI TEST_URI = ServiceUtils.createUri("https://api.fireworks.ai/inference/v1/chat/completions");
    private static final URI INITIAL_TEST_URI = ServiceUtils.createUri("https://www.initial-test.com");

    private static final int TEST_RATE_LIMIT = 1000;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final int DEFAULT_RATE_LIMIT = 6000;

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new FireworksAiChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI, TEST_RATE_LIMIT)
        );

        assertThat(
            updatedServiceSettings,
            is(
                new FireworksAiChatCompletionServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_URI,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new FireworksAiChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var serviceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(serviceSettings, is(originalServiceSettings));
    }

    public void testFromMap_AllFields_Success() {
        var serviceSettings = FireworksAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI, TEST_RATE_LIMIT),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new FireworksAiChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingModelId_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiChatCompletionServiceSettings.fromMap(
                buildServiceSettingsMap(null, TEST_URI, TEST_RATE_LIMIT),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];")
        );
    }

    public void testFromMap_MissingUrl_DefaultsToNull() {
        var serviceSettings = FireworksAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, TEST_RATE_LIMIT),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new FireworksAiChatCompletionServiceSettings(TEST_MODEL_ID, null, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingRateLimit_Success() {
        var serviceSettings = FireworksAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new FireworksAiChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URI, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = FireworksAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI, TEST_RATE_LIMIT),
            ConfigurationParseContext.PERSISTENT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "url": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_URI, TEST_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_DoesNotWriteUrl_WhenNull() throws IOException {
        var serviceSettings = FireworksAiChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null),
            ConfigurationParseContext.PERSISTENT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, DEFAULT_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<FireworksAiChatCompletionServiceSettings> instanceReader() {
        return FireworksAiChatCompletionServiceSettings::new;
    }

    @Override
    protected FireworksAiChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected FireworksAiChatCompletionServiceSettings mutateInstance(FireworksAiChatCompletionServiceSettings instance)
        throws IOException {
        var modelId = instance.modelId();
        var uri = instance.uri();
        var rateLimitSettings = instance.rateLimitSettings();

        switch (randomInt(2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> createUri(randomAlphaOfLength(5)));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new FireworksAiChatCompletionServiceSettings(modelId, uri, rateLimitSettings);
    }

    @Override
    protected FireworksAiChatCompletionServiceSettings mutateInstanceForVersion(
        FireworksAiChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static FireworksAiChatCompletionServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);
        return new FireworksAiChatCompletionServiceSettings(
            modelId,
            randomBoolean() ? createUri(randomAlphaOfLength(15)) : null,
            RateLimitSettingsTests.createRandom()
        );
    }

    private static HashMap<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable URI uri,
        @Nullable Integer rateLimit
    ) {
        HashMap<String, Object> map = new HashMap<>();
        if (modelId != null) {
            map.put(ServiceFields.MODEL_ID, modelId);
        }
        if (uri != null) {
            map.put(ServiceFields.URL, uri.toString());
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }
}
