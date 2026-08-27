/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class HuggingFaceElserServiceSettingsTests extends AbstractWireSerializingTestCase<HuggingFaceElserServiceSettings> {

    private static final URI TEST_URI = URI.create("https://www.test.com");
    private static final URI INITIAL_TEST_URI = URI.create("https://www.initial.com");

    private static final int TEST_RATE_LIMIT = 50;
    private static final int INITIAL_TEST_RATE_LIMIT = 100;
    private static final int DEFAULT_RATE_LIMIT = 3000;

    /** Matches {@link HuggingFaceElserServiceSettings} serialized {@code max_input_tokens}. */
    private static final int DEFAULT_ELSER_TOKEN_LIMIT = 512;

    public static HuggingFaceElserServiceSettings createRandom() {
        return new HuggingFaceElserServiceSettings(randomAlphaOfLength(15));
    }

    public void testUpdateServiceSettings_OnlyRateLimit_IsUpdated() {
        var originalServiceSettings = new HuggingFaceElserServiceSettings(INITIAL_TEST_URI, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));
        var updateMap = buildServiceSettingsMap(null, TEST_RATE_LIMIT);
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(updateMap);

        assertThat(
            updatedServiceSettings,
            is(new HuggingFaceElserServiceSettings(INITIAL_TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
        // The caller relies on updateServiceSettings consuming the parsed entries to verify that no unknown settings remain.
        assertThat(updateMap, is(anEmptyMap()));
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new HuggingFaceElserServiceSettings(INITIAL_TEST_URI, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));
        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_RateLimitNull_ResetsToDefault() {
        var originalServiceSettings = new HuggingFaceElserServiceSettings(INITIAL_TEST_URI, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));

        var updateMap = new HashMap<String, Object>();
        updateMap.put(RateLimitSettings.FIELD_NAME, null);
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(updateMap);

        assertThat(
            updatedServiceSettings,
            is(new HuggingFaceElserServiceSettings(INITIAL_TEST_URI, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_GivenImmutableFields_ThrowsException() {
        var originalServiceSettings = new HuggingFaceElserServiceSettings(INITIAL_TEST_URI, new RateLimitSettings(INITIAL_TEST_RATE_LIMIT));

        for (String immutableField : List.of(ServiceFields.URL, ServiceFields.MAX_INPUT_TOKENS)) {
            var e = expectThrows(
                XContentParseException.class,
                () -> originalServiceSettings.updateServiceSettings(new HashMap<>(Map.of(immutableField, "value")))
            );
            assertThat(
                e.getMessage(),
                endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, immutableField))
            );
        }
    }

    public void testFromMap_AllFields_Success() {
        var settingsMap = buildServiceSettingsMap(TEST_URI.toString(), TEST_RATE_LIMIT);
        var serviceSettings = HuggingFaceElserServiceSettings.fromMap(settingsMap, randomFrom(ConfigurationParseContext.values()));

        assertThat(serviceSettings, is(new HuggingFaceElserServiceSettings(TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT))));
        // The caller relies on fromMap consuming the parsed entries to verify that no unknown settings remain.
        assertThat(settingsMap, is(anEmptyMap()));
    }

    public void testFromMap_OnlyMandatoryFields_Success() {
        var serviceSettings = HuggingFaceElserServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_URI.toString(), null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(new HuggingFaceElserServiceSettings(TEST_URI.toString())));
    }

    public void testFromMap_ExplicitNullRateLimit_UsesDefault() {
        var settings = buildServiceSettingsMap(TEST_URI.toString(), null);
        settings.put(RateLimitSettings.FIELD_NAME, null);

        var serviceSettings = HuggingFaceElserServiceSettings.fromMap(settings, randomFrom(ConfigurationParseContext.values()));

        assertThat(serviceSettings, is(new HuggingFaceElserServiceSettings(TEST_URI.toString())));
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserServiceSettings.fromMap(buildServiceSettingsMap("", null), randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[service_settings] Invalid value empty string. [%s] must be a non-empty string", ServiceFields.URL))
        );
    }

    public void testFromMap_MissingUrl_ThrowsError() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserServiceSettings.fromMap(new HashMap<>(), randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.URL))
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        String invalidTestUrl = "https://www.abc^.com";
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserServiceSettings.fromMap(
                buildServiceSettingsMap(invalidTestUrl, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "[service_settings] Invalid url [%s] received for field [url]. "
                        + "Error: unable to parse url [%s]. Reason: Illegal character in authority",
                    invalidTestUrl,
                    invalidTestUrl
                )
            )
        );
    }

    public void testFromMap_UnknownField_RequestContext_ThrowsError() {
        var settings = buildServiceSettingsMap(TEST_URI.toString(), null);
        settings.put("unknown_field", "value");

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> HuggingFaceElserServiceSettings.fromMap(settings, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString("unknown field [unknown_field]"));
    }

    public void testFromMap_UnknownField_PersistentContext_IsIgnored() {
        var settings = buildServiceSettingsMap(TEST_URI.toString(), null);
        // max_input_tokens is written to the persisted XContent by toXContentFragmentOfExposedFields and must round-trip on read.
        settings.put(ServiceFields.MAX_INPUT_TOKENS, DEFAULT_ELSER_TOKEN_LIMIT);

        var serviceSettings = HuggingFaceElserServiceSettings.fromMap(settings, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings, is(new HuggingFaceElserServiceSettings(TEST_URI.toString())));
    }

    public void testFromMap_ApiKey_IsIgnored() {
        // In REST requests api_key appears in the same JSON block as service_settings; DefaultSecretSettings extracts it separately, so the
        // strict parser must accept it as a no-op.
        var settings = buildServiceSettingsMap(TEST_URI.toString(), null);
        settings.put("api_key", "some-secret");

        var serviceSettings = HuggingFaceElserServiceSettings.fromMap(settings, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new HuggingFaceElserServiceSettings(TEST_URI.toString())));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new HuggingFaceElserServiceSettings(TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "url": "%s",
                "max_input_tokens": %d,
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_URI.toString(), DEFAULT_ELSER_TOKEN_LIMIT, TEST_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<HuggingFaceElserServiceSettings> instanceReader() {
        return HuggingFaceElserServiceSettings::new;
    }

    @Override
    protected HuggingFaceElserServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected HuggingFaceElserServiceSettings mutateInstance(HuggingFaceElserServiceSettings instance) throws IOException {
        var uri = instance.uri();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomIntBetween(0, 1)) {
            case 0 -> uri = randomValueOtherThan(uri, () -> createUri(randomAlphaOfLength(15)));
            case 1 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new HuggingFaceElserServiceSettings(uri, rateLimitSettings);
    }

    public static Map<String, Object> buildServiceSettingsMap(@Nullable String url, @Nullable Integer rateLimit) {
        var map = new HashMap<String, Object>();
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }
}
