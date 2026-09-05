/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.rerank;

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
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class HuggingFaceRerankServiceSettingsTests extends AbstractWireSerializingTestCase<HuggingFaceRerankServiceSettings> {

    private static final URI TEST_URI = URI.create("https://www.test.com");
    private static final URI INITIAL_TEST_URI = URI.create("https://www.initial.com");

    private static final int TEST_RATE_LIMIT = 50;
    private static final int INITIAL_TEST_RATE_LIMIT = 100;
    private static final int DEFAULT_RATE_LIMIT = 3000;

    public static HuggingFaceRerankServiceSettings createRandom() {
        return new HuggingFaceRerankServiceSettings(randomAlphaOfLength(15));
    }

    public void testUpdateServiceSettings_OnlyRateLimit_IsUpdated() {
        var originalServiceSettings = new HuggingFaceRerankServiceSettings(
            INITIAL_TEST_URI,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updateMap = buildServiceSettingsMap(null, TEST_RATE_LIMIT);
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(updateMap);

        assertThat(
            updatedServiceSettings,
            is(new HuggingFaceRerankServiceSettings(INITIAL_TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new HuggingFaceRerankServiceSettings(
            INITIAL_TEST_URI,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_RateLimitNull_ResetsToDefault() {
        var originalServiceSettings = new HuggingFaceRerankServiceSettings(
            INITIAL_TEST_URI,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updateMap = new HashMap<String, Object>();
        updateMap.put(RateLimitSettings.FIELD_NAME, null);
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(updateMap);

        assertThat(
            updatedServiceSettings,
            is(new HuggingFaceRerankServiceSettings(INITIAL_TEST_URI, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testUpdateServiceSettings_GivenImmutableUrl_ThrowsException() {
        var originalServiceSettings = new HuggingFaceRerankServiceSettings(
            INITIAL_TEST_URI,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var e = expectThrows(
            XContentParseException.class,
            () -> originalServiceSettings.updateServiceSettings(new HashMap<>(Map.of(ServiceFields.URL, "value")))
        );
        assertThat(
            e.getMessage(),
            endsWith(Strings.format("[%s] unknown field [%s]", ModelConfigurations.SERVICE_SETTINGS, ServiceFields.URL))
        );
    }

    public void testFromMap_AllFields_Success() {
        var settingsMap = buildServiceSettingsMap(TEST_URI.toString(), TEST_RATE_LIMIT);
        var serviceSettings = HuggingFaceRerankServiceSettings.fromMap(settingsMap, randomFrom(ConfigurationParseContext.values()));

        assertThat(serviceSettings, is(new HuggingFaceRerankServiceSettings(TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT))));
    }

    public void testFromMap_OnlyMandatoryFields_Success() {
        var serviceSettings = HuggingFaceRerankServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_URI.toString(), null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(new HuggingFaceRerankServiceSettings(TEST_URI.toString())));
    }

    /**
     * An explicit {@code "rate_limit": null} was accepted by the old map-based parsing (it fell back to the default), but the
     * {@code ObjectParser} rejects it, the same behavior as the other parser-based services (for example Groq).
     */
    public void testFromMap_ExplicitNullRateLimit_ThrowsException() {
        var settings = buildServiceSettingsMap(TEST_URI.toString(), null);
        settings.put(RateLimitSettings.FIELD_NAME, null);

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> HuggingFaceRerankServiceSettings.fromMap(settings, randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(thrownException.getMessage(), containsString(RateLimitSettings.FIELD_NAME));
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceRerankServiceSettings.fromMap(
                buildServiceSettingsMap("", null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[service_settings] Invalid value empty string. [%s] must be a non-empty string", ServiceFields.URL))
        );
    }

    public void testFromMap_MissingUrl_ThrowsError() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceRerankServiceSettings.fromMap(new HashMap<>(), randomFrom(ConfigurationParseContext.values()))
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
            () -> HuggingFaceRerankServiceSettings.fromMap(
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
            () -> HuggingFaceRerankServiceSettings.fromMap(settings, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString("unknown field [unknown_field]"));
    }

    public void testFromMap_UnknownField_PersistentContext_IsIgnored() {
        var settings = buildServiceSettingsMap(TEST_URI.toString(), null);
        settings.put("unknown_field", "value");

        var serviceSettings = HuggingFaceRerankServiceSettings.fromMap(settings, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings, is(new HuggingFaceRerankServiceSettings(TEST_URI.toString())));
    }

    public void testFromMap_ApiKey_IsIgnored() {
        // In REST requests api_key appears in the same JSON block as service_settings; DefaultSecretSettings extracts it separately, so the
        // strict parser must accept it as a no-op.
        var settings = buildServiceSettingsMap(TEST_URI.toString(), null);
        settings.put("api_key", "some-secret");

        var serviceSettings = HuggingFaceRerankServiceSettings.fromMap(settings, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new HuggingFaceRerankServiceSettings(TEST_URI.toString())));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new HuggingFaceRerankServiceSettings(TEST_URI, new RateLimitSettings(TEST_RATE_LIMIT));

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
            """, TEST_URI.toString(), TEST_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<HuggingFaceRerankServiceSettings> instanceReader() {
        return HuggingFaceRerankServiceSettings::new;
    }

    @Override
    protected HuggingFaceRerankServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected HuggingFaceRerankServiceSettings mutateInstance(HuggingFaceRerankServiceSettings instance) throws IOException {
        var uri = instance.uri();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomIntBetween(0, 1)) {
            case 0 -> uri = randomValueOtherThan(uri, () -> createUri(randomAlphaOfLength(15)));
            case 1 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new HuggingFaceRerankServiceSettings(uri, rateLimitSettings);
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
