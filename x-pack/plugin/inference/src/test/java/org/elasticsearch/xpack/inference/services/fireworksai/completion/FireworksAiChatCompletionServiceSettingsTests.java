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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class FireworksAiChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    FireworksAiChatCompletionServiceSettings> {

    private static final String MODEL_ID = "accounts/fireworks/models/llama-v3p1-8b-instruct";
    private static final String CORRECT_URL = "https://api.fireworks.ai/inference/v1/chat/completions";
    private static final int RATE_LIMIT = 1000;

    public void testFromMap_AllFields_Success() {
        var serviceSettings = FireworksAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    MODEL_ID,
                    ServiceFields.URL,
                    CORRECT_URL,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new FireworksAiChatCompletionServiceSettings(MODEL_ID, createUri(CORRECT_URL), new RateLimitSettings(RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingModelId_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> FireworksAiChatCompletionServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.URL,
                        CORRECT_URL,
                        RateLimitSettings.FIELD_NAME,
                        new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                    )
                ),
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
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    MODEL_ID,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new FireworksAiChatCompletionServiceSettings(MODEL_ID, null, new RateLimitSettings(RATE_LIMIT))));
    }

    public void testFromMap_MissingRateLimit_Success() {
        var serviceSettings = FireworksAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID, ServiceFields.URL, CORRECT_URL)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new FireworksAiChatCompletionServiceSettings(MODEL_ID, createUri(CORRECT_URL), null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = FireworksAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    MODEL_ID,
                    ServiceFields.URL,
                    CORRECT_URL,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace("""
            {
                "model_id": "accounts/fireworks/models/llama-v3p1-8b-instruct",
                "url": "https://api.fireworks.ai/inference/v1/chat/completions",
                "rate_limit": {
                    "requests_per_minute": 1000
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_DoesNotWriteUrl_WhenNull() throws IOException {
        var serviceSettings = FireworksAiChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID)),
            ConfigurationParseContext.PERSISTENT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace("""
            {
                "model_id": "accounts/fireworks/models/llama-v3p1-8b-instruct",
                "rate_limit": {
                    "requests_per_minute": 6000
                }
            }
            """);

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

    public static Map<String, Object> getServiceSettingsMap(String model, String url) {
        var map = new HashMap<String, Object>();
        map.put(ServiceFields.MODEL_ID, model);
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        return map;
    }
}
