/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class HuggingFaceServiceSettingsTests extends AbstractWireSerializingTestCase<HuggingFaceServiceSettings> {

    private static final URI TEST_URI = URI.create("https://www.test.com");
    private static final URI INITIAL_TEST_URI = URI.create("https://www.initial.com");

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;

    private static final int TEST_DIMENSIONS = 384;
    private static final int INITIAL_TEST_DIMENSIONS = 128;

    private static final int TEST_MAX_INPUT_TOKENS = 256;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 64;

    private static final int TEST_RATE_LIMIT = 500;
    private static final int INITIAL_TEST_RATE_LIMIT = 100;

    private static final int DEFAULT_RATE_LIMIT_REQUESTS_PER_MINUTE = 3000;

    private static final String INVALID_TEST_URL = "https://www.abc^.com";

    private static final String INVALID_SIMILARITY_STRING = "by_size";

    public static HuggingFaceServiceSettings createRandom() {
        return createRandom(randomAlphaOfLength(15));
    }

    private static HuggingFaceServiceSettings createRandom(String url) {
        SimilarityMeasure similarityMeasure = null;
        Integer dimensions = null;
        var isTextEmbeddingModel = randomBoolean();
        if (isTextEmbeddingModel) {
            similarityMeasure = randomSimilarityMeasure();
            dimensions = randomIntBetween(32, 256);
        }
        Integer maxInputTokens = randomBoolean() ? null : randomIntBetween(128, 256);
        return new HuggingFaceServiceSettings(
            ServiceUtils.createUri(url),
            similarityMeasure,
            dimensions,
            maxInputTokens,
            RateLimitSettingsTests.createRandom()
        );
    }

    public void testUpdateServiceSettings_OnlyMutableFields_AreUpdated() {
        var originalServiceSettings = new HuggingFaceServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updateMap = buildServiceSettingsMap(null, null, null, TEST_MAX_INPUT_TOKENS, TEST_RATE_LIMIT);

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(updateMap);

        assertThat(
            updatedServiceSettings,
            is(
                new HuggingFaceServiceSettings(
                    INITIAL_TEST_URI,
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new HuggingFaceServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_RateLimitNull_ResetsToDefault() {
        var originalServiceSettings = new HuggingFaceServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updateMap = new HashMap<String, Object>();
        updateMap.put(RateLimitSettings.FIELD_NAME, null);
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(updateMap);

        assertThat(
            updatedServiceSettings,
            is(
                new HuggingFaceServiceSettings(
                    INITIAL_TEST_URI,
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    INITIAL_TEST_DIMENSIONS,
                    INITIAL_TEST_MAX_INPUT_TOKENS,
                    new RateLimitSettings(DEFAULT_RATE_LIMIT_REQUESTS_PER_MINUTE)
                )
            )
        );
    }

    public void testUpdateServiceSettings_GivenImmutableFields_ThrowsException() {
        var originalServiceSettings = new HuggingFaceServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        for (String immutableField : List.of(ServiceFields.URL, ServiceFields.SIMILARITY, ServiceFields.DIMENSIONS)) {
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
        var serviceSettings = HuggingFaceServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_URI.toString(),
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_DIMENSIONS,
                TEST_MAX_INPUT_TOKENS,
                TEST_RATE_LIMIT
            ),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new HuggingFaceServiceSettings(
                    TEST_URI,
                    TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_OnlyMandatoryFields_Success() {
        var serviceSettings = HuggingFaceServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_URI.toString(), null, null, null, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(serviceSettings, is(new HuggingFaceServiceSettings(TEST_URI)));
    }

    /**
     * An explicit {@code "rate_limit": null} was accepted by the old map-based parsing (it fell back to the default), but the
     * {@code ObjectParser} rejects it, the same behavior as the other parser-based services (for example Groq).
     */
    public void testFromMap_ExplicitNullRateLimit_ThrowsException() {
        var settings = buildServiceSettingsMap(TEST_URI.toString(), null, null, null, null);
        settings.put(RateLimitSettings.FIELD_NAME, null);

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> HuggingFaceServiceSettings.fromMap(settings, randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(thrownException.getMessage(), containsString(RateLimitSettings.FIELD_NAME));
    }

    public void testFromMap_MissingUrl_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceServiceSettings.fromMap(new HashMap<>(), randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.URL))
        );
    }

    public void testFromMap_EmptyUrl_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceServiceSettings.fromMap(
                buildServiceSettingsMap("", null, null, null, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[service_settings] Invalid value empty string. [%s] must be a non-empty string", ServiceFields.URL))
        );
    }

    public void testFromMap_InvalidUrl_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceServiceSettings.fromMap(
                buildServiceSettingsMap(INVALID_TEST_URL, null, null, null, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "[service_settings] Invalid url [%s] received for field [%s]. "
                        + "Error: unable to parse url [%s]. Reason: Illegal character in authority",
                    INVALID_TEST_URL,
                    ServiceFields.URL,
                    INVALID_TEST_URL
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsException() {
        var thrownException = expectThrows(
            XContentParseException.class,
            () -> HuggingFaceServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_URI.toString(), INVALID_SIMILARITY_STRING, null, null, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getCause().getMessage(),
            containsString(Strings.format("Invalid value [%s]", INVALID_SIMILARITY_STRING))
        );
    }

    public void testFromMap_NonPositiveDimensions_ThrowsException() {
        var thrownException = expectThrows(
            XContentParseException.class,
            () -> HuggingFaceServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_URI.toString(), null, 0, null, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getCause().getMessage(),
            is(Strings.format("[service_settings] Invalid value [0]. [%s] must be a positive integer", ServiceFields.DIMENSIONS))
        );
    }

    public void testFromMap_NonPositiveMaxInputTokens_ThrowsException() {
        var thrownException = expectThrows(
            XContentParseException.class,
            () -> HuggingFaceServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_URI.toString(), null, null, 0, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(
            thrownException.getCause().getMessage(),
            is(Strings.format("[service_settings] Invalid value [0]. [%s] must be a positive integer", ServiceFields.MAX_INPUT_TOKENS))
        );
    }

    public void testFromMap_UnknownField_RequestContext_ThrowsError() {
        var settings = buildServiceSettingsMap(TEST_URI.toString(), null, null, null, null);
        settings.put("unknown_field", "value");

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> HuggingFaceServiceSettings.fromMap(settings, ConfigurationParseContext.REQUEST)
        );

        assertThat(thrownException.getMessage(), containsString("unknown field [unknown_field]"));
    }

    public void testFromMap_UnknownField_PersistentContext_IsIgnored() {
        var settings = buildServiceSettingsMap(TEST_URI.toString(), null, null, null, null);
        settings.put("unknown_field", "value");

        var serviceSettings = HuggingFaceServiceSettings.fromMap(settings, ConfigurationParseContext.PERSISTENT);

        assertThat(serviceSettings, is(new HuggingFaceServiceSettings(TEST_URI)));
    }

    public void testFromMap_ApiKey_IsIgnored() {
        // In REST requests api_key appears in the same JSON block as service_settings; DefaultSecretSettings extracts it separately, so the
        // strict parser must accept it as a no-op.
        var settings = buildServiceSettingsMap(TEST_URI.toString(), null, null, null, null);
        settings.put("api_key", "some-secret");

        var serviceSettings = HuggingFaceServiceSettings.fromMap(settings, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new HuggingFaceServiceSettings(TEST_URI)));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new HuggingFaceServiceSettings(
            TEST_URI,
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "url": "%s",
                "similarity": "%s",
                "dimensions": %d,
                "max_input_tokens": %d,
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_URI, TEST_SIMILARITY_MEASURE, TEST_DIMENSIONS, TEST_MAX_INPUT_TOKENS, TEST_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit() throws IOException {
        var serviceSettings = new HuggingFaceServiceSettings(TEST_URI);

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
            """, TEST_URI, DEFAULT_RATE_LIMIT_REQUESTS_PER_MINUTE));

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<HuggingFaceServiceSettings> instanceReader() {
        return HuggingFaceServiceSettings::new;
    }

    @Override
    protected HuggingFaceServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected HuggingFaceServiceSettings mutateInstance(HuggingFaceServiceSettings instance) throws IOException {
        var uri = instance.uri();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(4)) {
            case 0 -> uri = randomValueOtherThan(uri, () -> ServiceUtils.createUri(randomAlphaOfLength(15)));
            case 1 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 2 -> dimensions = randomValueOtherThan(dimensions, () -> randomFrom(randomIntBetween(32, 256), null));
            case 3 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 4 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new HuggingFaceServiceSettings(uri, similarity, dimensions, maxInputTokens, rateLimitSettings);
    }

    public static Map<String, Object> buildServiceSettingsMap(String url) {
        var map = new HashMap<String, Object>();

        map.put(ServiceFields.URL, url);

        return map;
    }

    private static Map<String, Object> buildServiceSettingsMap(
        @Nullable String url,
        @Nullable String similarityString,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable Integer rateLimit
    ) {
        var map = new HashMap<String, Object>();
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        if (similarityString != null) {
            map.put(ServiceFields.SIMILARITY, similarityString);
        }
        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            map.put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return map;
    }
}
