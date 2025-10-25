/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.completion;

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

public class HuggingFaceChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    HuggingFaceChatCompletionServiceSettings> {

    public static final String MODEL_ID = "some model";
    public static final String CORRECT_URL = "https://www.elastic.co";
    public static final int RATE_LIMIT = 2;

    public void testFromMap_AllFields_Success() {
        var serviceSettings = HuggingFaceChatCompletionServiceSettings.fromMap(
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
            is(new HuggingFaceChatCompletionServiceSettings(MODEL_ID, createUri(CORRECT_URL), new RateLimitSettings(RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingModelId_Success() {
        var serviceSettings = HuggingFaceChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
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
            is(new HuggingFaceChatCompletionServiceSettings(null, createUri(CORRECT_URL), new RateLimitSettings(RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingRateLimit_Success() {
        var serviceSettings = HuggingFaceChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_ID, ServiceFields.URL, CORRECT_URL)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new HuggingFaceChatCompletionServiceSettings(MODEL_ID, createUri(CORRECT_URL), null)));
    }

    public void testFromMap_MissingUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceChatCompletionServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        MODEL_ID,
                        RateLimitSettings.FIELD_NAME,
                        new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                    )
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format("Validation Failed: 1: [service_settings] does not contain the required setting [url];", ServiceFields.URL)
            )
        );
    }

    public void testFromMap_EmptyUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceChatCompletionServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        MODEL_ID,
                        ServiceFields.URL,
                        "",
                        RateLimitSettings.FIELD_NAME,
                        new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                    )
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value empty string. [%s] must be a non-empty string;",
                    ServiceFields.URL
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsException() {
        String invalidUrl = "https://www.elastic^^co";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceChatCompletionServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        MODEL_ID,
                        ServiceFields.URL,
                        invalidUrl,
                        RateLimitSettings.FIELD_NAME,
                        new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                    )
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid url [%s] received for field [%s]",
                    invalidUrl,
                    ServiceFields.URL
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = HuggingFaceChatCompletionServiceSettings.fromMap(
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
                "model_id": "some model",
                "url": "https://www.elastic.co",
                "rate_limit": {
                    "requests_per_minute": 2
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit() throws IOException {
        var serviceSettings = HuggingFaceChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.URL, CORRECT_URL)),
            ConfigurationParseContext.PERSISTENT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace("""
            {
                "url": "https://www.elastic.co",
                "rate_limit": {
                    "requests_per_minute": 3000
                }
            }
            """);
        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<HuggingFaceChatCompletionServiceSettings> instanceReader() {
        return HuggingFaceChatCompletionServiceSettings::new;
    }

    @Override
    protected HuggingFaceChatCompletionServiceSettings createTestInstance() {
        return createRandomWithNonNullUrl();
    }

    @Override
    protected HuggingFaceChatCompletionServiceSettings mutateInstance(HuggingFaceChatCompletionServiceSettings instance)
        throws IOException {
        var modelId = instance.modelId();
        var uri = instance.uri();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomFrom(randomAlphaOfLength(8), null));
            case 1 -> uri = randomValueOtherThan(uri, () -> createUri(randomAlphaOfLength(15)));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new HuggingFaceChatCompletionServiceSettings(modelId, uri, rateLimitSettings);
    }

    @Override
    protected HuggingFaceChatCompletionServiceSettings mutateInstanceForVersion(
        HuggingFaceChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static HuggingFaceChatCompletionServiceSettings createRandomWithNonNullUrl() {
        return createRandom(randomAlphaOfLength(15));
    }

    private static HuggingFaceChatCompletionServiceSettings createRandom(String url) {
        var modelId = randomAlphaOfLength(8);

        return new HuggingFaceChatCompletionServiceSettings(modelId, createUri(url), RateLimitSettingsTests.createRandom());
    }

    public static Map<String, Object> getServiceSettingsMap(String url, String model) {
        var map = new HashMap<String, Object>();

        map.put(ServiceFields.URL, url);
        map.put(ServiceFields.MODEL_ID, model);

        return map;
    }
}
