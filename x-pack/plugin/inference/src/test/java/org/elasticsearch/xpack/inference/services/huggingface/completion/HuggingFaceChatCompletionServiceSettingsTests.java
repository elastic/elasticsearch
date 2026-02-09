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
import org.elasticsearch.inference.TaskType;
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

public class HuggingFaceChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    HuggingFaceChatCompletionServiceSettings> {

    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final URI TEST_URI = ServiceUtils.createUri("https://test-uri.com");
    private static final URI INITIAL_TEST_URI = ServiceUtils.createUri("https://initial-test-uri.com");
    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = getServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_RATE_LIMIT);

        var serviceSettings = createInitialServiceSettings().updateServiceSettings(settingsMap, TaskType.COMPLETION);

        assertThat(
            serviceSettings,
            is(new HuggingFaceChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = createInitialServiceSettings().updateServiceSettings(new HashMap<>(), TaskType.COMPLETION);

        assertThat(serviceSettings, is(createInitialServiceSettings()));
    }

    private static HuggingFaceChatCompletionServiceSettings createInitialServiceSettings() {
        return new HuggingFaceChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
    }

    public void testFromMap_AllFields_Success() {
        var serviceSettings = HuggingFaceChatCompletionServiceSettings.fromMap(
            getServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_RATE_LIMIT),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new HuggingFaceChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingModelId_Success() {
        var serviceSettings = HuggingFaceChatCompletionServiceSettings.fromMap(
            getServiceSettingsMap(null, TEST_URI.toString(), TEST_RATE_LIMIT),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(new HuggingFaceChatCompletionServiceSettings(null, TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingRateLimit_Success() {
        var serviceSettings = HuggingFaceChatCompletionServiceSettings.fromMap(
            getServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(serviceSettings, is(new HuggingFaceChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URI, null)));
    }

    public void testFromMap_MissingUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceChatCompletionServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ServiceFields.MODEL_ID,
                        TEST_MODEL_ID,
                        RateLimitSettings.FIELD_NAME,
                        new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
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
        String emptyUrl = "";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceChatCompletionServiceSettings.fromMap(
                getServiceSettingsMap(TEST_MODEL_ID, emptyUrl, TEST_RATE_LIMIT),
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
                getServiceSettingsMap(TEST_MODEL_ID, invalidUrl, TEST_RATE_LIMIT),
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
        var serviceSettings = new HuggingFaceChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT));

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

    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit() throws IOException {
        var serviceSettings = new HuggingFaceChatCompletionServiceSettings(null, TEST_URI, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);
        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "url": "%s",
                "rate_limit": {
                    "requests_per_minute": 3000
                }
            }
            """, TEST_URI));
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
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLengthOrNull(8));
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

    public static HashMap<String, Object> getServiceSettingsMap(String modelId, String url, Integer rateLimit) {
        var map = new HashMap<String, Object>();

        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        if (modelId != null) {
            map.put(ServiceFields.MODEL_ID, modelId);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }

        return map;
    }
}
