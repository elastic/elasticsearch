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
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class NvidiaChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<NvidiaChatCompletionServiceSettings> {

    private static final String MODEL_VALUE = "some_model";
    private static final String URL_VALUE = "http://www.abc.com";
    private static final String INVALID_URL_VALUE = "^^^";
    private static final int RATE_LIMIT = 2;

    public void testFromMap_AllFields_Success() {
        var serviceSettings = NvidiaChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    MODEL_VALUE,
                    ServiceFields.URL,
                    URL_VALUE,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new NvidiaChatCompletionServiceSettings(MODEL_VALUE, URL_VALUE, new RateLimitSettings(RATE_LIMIT))));
    }

    public void testFromMap_MissingModelId_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaChatCompletionServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.URL,
                        URL_VALUE,
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

    public void testFromMap_MissingUrl_Success() {
        var serviceSettings = NvidiaChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    MODEL_VALUE,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                )
            ),
            ConfigurationParseContext.PERSISTENT
        );
        assertThat(
            serviceSettings,
            is(
                new NvidiaChatCompletionServiceSettings(
                    MODEL_VALUE,
                    ServiceUtils.createOptionalUri(null),
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_InvalidUrl_ThrowsException() {
        testFromMap_InvalidUrl(
            Map.of(
                ServiceFields.URL,
                INVALID_URL_VALUE,
                ServiceFields.MODEL_ID,
                MODEL_VALUE,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            """
                Validation Failed: 1: [service_settings] Invalid url [^^^] received for field [url]. \
                Error: unable to parse url [^^^]. Reason: Illegal character in path;"""
        );
    }

    public void testFromMap_EmptyUrl_ThrowsException() {
        testFromMap_InvalidUrl(
            Map.of(
                ServiceFields.URL,
                "",
                ServiceFields.MODEL_ID,
                MODEL_VALUE,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
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

    public void testFromMap_MissingRateLimit_Success() {
        var serviceSettings = NvidiaChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_VALUE, ServiceFields.URL, URL_VALUE)),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new NvidiaChatCompletionServiceSettings(MODEL_VALUE, URL_VALUE, null)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = NvidiaChatCompletionServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    MODEL_VALUE,
                    ServiceFields.URL,
                    URL_VALUE,
                    RateLimitSettings.FIELD_NAME,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                )
            ),
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
                    "requests_per_minute": 2
                }
            }
            """, MODEL_VALUE, URL_VALUE));

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit() throws IOException {
        var serviceSettings = NvidiaChatCompletionServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, MODEL_VALUE)),
            ConfigurationParseContext.PERSISTENT
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "rate_limit": {
                    "requests_per_minute": 3000
                }
            }
            """, MODEL_VALUE));
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
            case 1 -> uri = randomValueOtherThan(uri, () -> ServiceUtils.createOptionalUri(randomAlphaOfLengthOrNull(15)));
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
        return new NvidiaChatCompletionServiceSettings(modelId, ServiceUtils.createOptionalUri(url), RateLimitSettingsTests.createRandom());
    }

    public static Map<String, Object> getServiceSettingsMap(
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
