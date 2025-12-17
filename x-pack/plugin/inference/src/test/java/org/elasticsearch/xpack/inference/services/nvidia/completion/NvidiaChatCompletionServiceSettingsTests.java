/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.completion;

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
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class NvidiaChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<NvidiaChatCompletionServiceSettings> {

    private static final String MODEL_VALUE = "some_model";
    private static final String URL_VALUE = "http://www.abc.com";
    private static final String URL_DEFAULT_VALUE = "https://integrate.api.nvidia.com/v1/chat/completions";
    private static final String URL_INVALID_VALUE = "^^^";
    private static final int RATE_LIMIT_VALUE = 2;
    private static final int RATE_LIMIT_DEFAULT_VALUE = 3000;

    public void testFromMap_AllFields_Success() {
        var serviceSettings = NvidiaChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                URL_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new NvidiaChatCompletionServiceSettings(MODEL_VALUE, URL_VALUE, new RateLimitSettings(RATE_LIMIT_VALUE)))
        );
    }

    public void testFromMap_NoModelId_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaChatCompletionServiceSettings.fromMap(
                buildServiceSettingsMap(
                    null,
                    URL_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];")
        );

    }

    public void testFromMap_NoUrl_Success() {
        var serviceSettings = NvidiaChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                null,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
            ),
            ConfigurationParseContext.PERSISTENT
        );
        assertThat(
            serviceSettings,
            is(new NvidiaChatCompletionServiceSettings(MODEL_VALUE, URL_DEFAULT_VALUE, new RateLimitSettings(RATE_LIMIT_VALUE)))
        );
    }

    public void testFromMap_InvalidUrl_ThrowsException() {
        testFromMap_InvalidUrl(
            buildServiceSettingsMap(
                MODEL_VALUE,
                URL_INVALID_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
            ),
            Strings.format("""
                Validation Failed: 1: [service_settings] Invalid url [%s] received for field [url]. \
                Error: unable to parse url [%s]. Reason: Illegal character in path;""", URL_INVALID_VALUE, URL_INVALID_VALUE)
        );
    }

    public void testFromMap_EmptyUrl_ThrowsException() {
        testFromMap_InvalidUrl(
            buildServiceSettingsMap(MODEL_VALUE, "", new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))),
            "Validation Failed: 1: [service_settings] Invalid value empty string. [url] must be a non-empty string;"
        );
    }

    private static void testFromMap_InvalidUrl(Map<String, Object> serviceSettingsMap, String expectedErrorMessage) {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaChatCompletionServiceSettings.fromMap(new HashMap<>(serviceSettingsMap), ConfigurationParseContext.PERSISTENT)
        );

        assertThat(thrownException.getMessage(), containsString(expectedErrorMessage));
    }

    public void testFromMap_NoRateLimit_Success() {
        var serviceSettings = NvidiaChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(MODEL_VALUE, URL_VALUE, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new NvidiaChatCompletionServiceSettings(MODEL_VALUE, URL_VALUE, new RateLimitSettings(RATE_LIMIT_DEFAULT_VALUE)))
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new NvidiaChatCompletionServiceSettings(MODEL_VALUE, URL_VALUE, new RateLimitSettings(RATE_LIMIT_VALUE));

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
            """, MODEL_VALUE, URL_VALUE, RATE_LIMIT_VALUE));

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_DefaultUrl_DefaultRateLimit() throws IOException {
        var serviceSettings = new NvidiaChatCompletionServiceSettings(MODEL_VALUE, createOptionalUri(null), null);

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
            """, MODEL_VALUE, URL_DEFAULT_VALUE, RATE_LIMIT_DEFAULT_VALUE));
        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<NvidiaChatCompletionServiceSettings> instanceReader() {
        return NvidiaChatCompletionServiceSettings::new;
    }

    @Override
    protected NvidiaChatCompletionServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected NvidiaChatCompletionServiceSettings mutateInstance(NvidiaChatCompletionServiceSettings instance) throws IOException {
        String modelId = instance.modelId();
        URI uri = instance.uri();
        RateLimitSettings rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> createUri(randomAlphaOfLength(15)));
            case 2 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new NvidiaChatCompletionServiceSettings(modelId, uri, rateLimitSettings);
    }

    @Override
    protected NvidiaChatCompletionServiceSettings mutateInstanceForVersion(
        NvidiaChatCompletionServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }

    private static NvidiaChatCompletionServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);
        var url = randomAlphaOfLengthOrNull(15);
        return new NvidiaChatCompletionServiceSettings(modelId, createOptionalUri(url), RateLimitSettingsTests.createRandom());
    }

    /**
     * Helper method to build a service settings map.
     * @param modelId the model ID
     * @param url the service URL
     * @param rateLimitSettings the rate limit settings
     * @return a map representing the service settings
     */
    public static Map<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable HashMap<String, Integer> rateLimitSettings
    ) {
        var result = new HashMap<String, Object>();

        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        if (url != null) {
            result.put(ServiceFields.URL, url);
        }
        if (rateLimitSettings != null) {
            result.put(RateLimitSettings.FIELD_NAME, rateLimitSettings);
        }

        return result;
    }

}
