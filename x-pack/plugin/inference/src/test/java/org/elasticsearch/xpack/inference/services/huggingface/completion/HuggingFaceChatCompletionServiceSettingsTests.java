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
import static org.hamcrest.Matchers.is;

public class HuggingFaceChatCompletionServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    HuggingFaceChatCompletionServiceSettings> {

    private static final URI TEST_URI = URI.create("https://www.elastic.co");
    private static final URI INITIAL_TEST_URI = URI.create("https://www.initial.co");

    private static final String TEST_MODEL_ID = "some model";
    private static final String INITIAL_TEST_MODEL_ID = "initial model";

    private static final int TEST_RATE_LIMIT = 2;
    private static final int INITIAL_TEST_RATE_LIMIT = 100;

    private static final int DEFAULT_RATE_LIMIT_REQUESTS_PER_MINUTE = 3000;

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(TEST_URI.toString(), TEST_MODEL_ID, TEST_RATE_LIMIT);
        var originalServiceSettings = new HuggingFaceChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);
        assertThat(
            updatedServiceSettings,
            is(
                new HuggingFaceChatCompletionServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_URI,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new HuggingFaceChatCompletionServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testFromMap_AllFields_Success() {
        var serviceSettings = HuggingFaceChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_URI.toString(), TEST_MODEL_ID, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(new HuggingFaceChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingModelId_Success() {
        var serviceSettings = HuggingFaceChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_URI.toString(), null, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(new HuggingFaceChatCompletionServiceSettings(null, TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testFromMap_MissingRateLimit_Success() {
        var serviceSettings = HuggingFaceChatCompletionServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_URI.toString(), TEST_MODEL_ID, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(new HuggingFaceChatCompletionServiceSettings(TEST_MODEL_ID, TEST_URI, null)));
    }

    public void testFromMap_MissingUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceChatCompletionServiceSettings.fromMap(
                buildServiceSettingsMap(null, TEST_MODEL_ID, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.URL))
        );
    }

    public void testFromMap_EmptyUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceChatCompletionServiceSettings.fromMap(
                buildServiceSettingsMap("", TEST_MODEL_ID, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value empty string. [%s] must be a non-empty string", ServiceFields.URL))
        );
    }

    public void testFromMap_InvalidUrl_ThrowsException() {
        var invalidUrl = "https://www.elastic^^co";
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceChatCompletionServiceSettings.fromMap(
                buildServiceSettingsMap(invalidUrl, TEST_MODEL_ID, TEST_RATE_LIMIT),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(thrownException.validationErrors().getFirst(), is(Strings.format("""
            [service_settings] Invalid url [%s] received for field [%s]. \
            Error: unable to parse url [%s]. Reason: Illegal character in authority""", invalidUrl, ServiceFields.URL, invalidUrl)));
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
            """, TEST_MODEL_ID, TEST_URI.toString(), TEST_RATE_LIMIT));

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
                    "requests_per_minute": %d
                }
            }
            """, TEST_URI.toString(), DEFAULT_RATE_LIMIT_REQUESTS_PER_MINUTE));
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

    public static Map<String, Object> buildServiceSettingsMap(@Nullable String url, @Nullable String modelId, @Nullable Integer rateLimit) {
        var map = new HashMap<String, Object>();
        if (modelId != null) {
            map.put(ServiceFields.MODEL_ID, modelId);
        }
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }
}
