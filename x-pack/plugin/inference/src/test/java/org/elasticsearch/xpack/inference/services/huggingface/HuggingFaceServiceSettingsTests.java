/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
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

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(
            TEST_URI.toString(),
            TEST_SIMILARITY_MEASURE.toString(),
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            TEST_RATE_LIMIT
        );
        var originalServiceSettings = new HuggingFaceServiceSettings(
            INITIAL_TEST_URI,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

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

    public void testFromMap() {
        {
            var serviceSettings = HuggingFaceServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_URI.toString(), null, null, null, null),
                randomFrom(ConfigurationParseContext.values())
            );
            assertThat(serviceSettings, is(new HuggingFaceServiceSettings(TEST_URI)));
        }
        {
            var serviceSettings = HuggingFaceServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_URI.toString(),
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    null
                ),
                randomFrom(ConfigurationParseContext.values())
            );
            assertThat(
                serviceSettings,
                is(new HuggingFaceServiceSettings(TEST_URI, TEST_SIMILARITY_MEASURE, TEST_DIMENSIONS, TEST_MAX_INPUT_TOKENS, null))
            );
        }
        {
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
    }

    public void testFromMap_MissingUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceServiceSettings.fromMap(new HashMap<>(), randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.URL))
        );
    }

    public void testFromMap_EmptyUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceServiceSettings.fromMap(
                buildServiceSettingsMap("", null, null, null, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value empty string. [%s] must be a non-empty string", ServiceFields.URL))
        );
    }

    public void testFromMap_InvalidUrl_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceServiceSettings.fromMap(
                buildServiceSettingsMap(INVALID_TEST_URL, null, null, null, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(
                Strings.format(
                    """
                        [service_settings] Invalid url [%s] received for field [%s]. \
                        Error: unable to parse url [%s]. Reason: Illegal character in authority""",
                    INVALID_TEST_URL,
                    ServiceFields.URL,
                    INVALID_TEST_URL
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> HuggingFaceServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_URI.toString(), INVALID_SIMILARITY_STRING, null, null, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[service_settings] Invalid value [%s] received. [similarity] must be one of [cosine, dot_product, l2_norm]",
                    INVALID_SIMILARITY_STRING
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new HuggingFaceServiceSettings(TEST_URI, null, null, null, new RateLimitSettings(TEST_RATE_LIMIT));

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
