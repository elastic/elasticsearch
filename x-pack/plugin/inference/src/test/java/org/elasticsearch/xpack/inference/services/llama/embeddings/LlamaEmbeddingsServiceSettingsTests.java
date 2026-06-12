/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
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
import static org.hamcrest.Matchers.is;

public class LlamaEmbeddingsServiceSettingsTests extends AbstractBWCSerializationTestCase<LlamaEmbeddingsServiceSettings> {

    private static final URI TEST_URI = URI.create("https://www.test.com");
    private static final URI INITIAL_TEST_URI = URI.create("https://www.initial.com");

    private static final String TEST_MODEL_ID = "test-model";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model";

    private static final int TEST_DIMENSIONS = 384;
    private static final int INITIAL_TEST_DIMENSIONS = 256;

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;

    private static final int TEST_MAX_INPUT_TOKENS = 128;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 64;

    private static final int TEST_RATE_LIMIT = 2;
    private static final int INITIAL_TEST_RATE_LIMIT = 5;
    private static final int DEFAULT_RATE_LIMIT = 3000;

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = LlamaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                TEST_DIMENSIONS,
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_MAX_INPUT_TOKENS,
                TEST_RATE_LIMIT
            ),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new LlamaEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE,
                    TEST_MAX_INPUT_TOKENS,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_OnlyMandatoryFields_UsesDefaultValues_Success() {
        var serviceSettings = LlamaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), null, null, null, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(new LlamaEmbeddingsServiceSettings(TEST_MODEL_ID, TEST_URI, null, null, null, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_NoModelId_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    null,
                    TEST_URI.toString(),
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.MODEL_ID))
        );
    }

    public void testFromMap_NoUrl_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    null,
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.URL))
        );
    }

    public void testFromMap_EmptyUrl_ThrowsException() {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    "",
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.URL))
        );
    }

    public void testFromMap_InvalidUrl_ThrowsException() {
        var invalidUrl = "^^^";
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    invalidUrl,
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            is(Strings.format("unable to parse url [%s]. Reason: Illegal character in path", invalidUrl))
        );
    }

    public void testFromMap_NoSimilarity_Success() {
        var serviceSettings = LlamaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_URI.toString(), TEST_DIMENSIONS, null, TEST_MAX_INPUT_TOKENS, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new LlamaEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    TEST_DIMENSIONS,
                    null,
                    TEST_MAX_INPUT_TOKENS,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsException() {
        var invalidSimilarity = "by_size";
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    TEST_DIMENSIONS,
                    invalidSimilarity,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(thrownException.getMessage(), is("No enum constant org.elasticsearch.inference.SimilarityMeasure.BY_SIZE"));
    }

    public void testFromMap_NoDimensions_Success() {
        var serviceSettings = LlamaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                null,
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_MAX_INPUT_TOKENS,
                TEST_RATE_LIMIT
            ),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new LlamaEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    null,
                    TEST_SIMILARITY_MEASURE,
                    TEST_MAX_INPUT_TOKENS,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_ZeroDimensions_ThrowsException() {
        int zeroDimensions = 0;
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    zeroDimensions,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ServiceFields.DIMENSIONS,
                    zeroDimensions,
                    ServiceFields.DIMENSIONS
                )
            )
        );
    }

    public void testFromMap_NegativeDimensions_ThrowsException() {
        int negativeDimensions = -10;
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    negativeDimensions,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ServiceFields.DIMENSIONS,
                    negativeDimensions,
                    ServiceFields.DIMENSIONS
                )
            )
        );
    }

    public void testFromMap_NoInputTokens_Success() {
        var serviceSettings = LlamaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                TEST_DIMENSIONS,
                TEST_SIMILARITY_MEASURE.toString(),
                null,
                TEST_RATE_LIMIT
            ),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new LlamaEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE,
                    null,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_ZeroInputTokens_ThrowsException() {
        int zeroMaxInputTokens = 0;
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE.toString(),
                    zeroMaxInputTokens,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ServiceFields.MAX_INPUT_TOKENS,
                    zeroMaxInputTokens,
                    ServiceFields.MAX_INPUT_TOKENS
                )
            )
        );
    }

    public void testFromMap_NegativeInputTokens_ThrowsException() {
        int negativeMaxInputTokens = -10;
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE.toString(),
                    negativeMaxInputTokens,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ServiceFields.MAX_INPUT_TOKENS,
                    negativeMaxInputTokens,
                    ServiceFields.MAX_INPUT_TOKENS
                )
            )
        );
    }

    public void testUpdateServiceSettings_MutableFields_AreUpdated() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put(ServiceFields.MAX_INPUT_TOKENS, TEST_MAX_INPUT_TOKENS);
        settingsMap.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)));
        var originalServiceSettings = new LlamaEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new LlamaEmbeddingsServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_URI,
                    INITIAL_TEST_DIMENSIONS,
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    TEST_MAX_INPUT_TOKENS,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new LlamaEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testUpdateServiceSettings_GivenImmutableFields_ShouldThrow() {
        var serviceSettings = new LlamaEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        for (String immutableField : List.of(
            ServiceFields.MODEL_ID,
            ServiceFields.URL,
            ServiceFields.DIMENSIONS,
            ServiceFields.SIMILARITY
        )) {
            var e = expectThrows(
                XContentParseException.class,
                () -> serviceSettings.updateServiceSettings(new HashMap<>(Map.of(immutableField, "value")))
            );
            assertThat(e.getMessage(), is(Strings.format("[1:2] [service_settings] unknown field [%s]", immutableField)));
        }
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new LlamaEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_URI,
            TEST_DIMENSIONS,
            TEST_SIMILARITY_MEASURE,
            TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        var xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace(
            Strings.format(
                """
                    {
                        "model_id": "%s",
                        "url": "%s",
                        "rate_limit": {
                            "requests_per_minute": %d
                        },
                        "dimensions": %d,
                        "similarity": "%s",
                        "max_input_tokens": %d
                    }
                    """,
                TEST_MODEL_ID,
                TEST_URI.toString(),
                TEST_RATE_LIMIT,
                TEST_DIMENSIONS,
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_MAX_INPUT_TOKENS
            )
        );

        assertThat(xContentResult, is(expected));
    }

    @Override
    protected Writeable.Reader<LlamaEmbeddingsServiceSettings> instanceReader() {
        return LlamaEmbeddingsServiceSettings::new;
    }

    @Override
    protected LlamaEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected LlamaEmbeddingsServiceSettings mutateInstance(LlamaEmbeddingsServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var uri = instance.uri();
        var dimensions = instance.dimensions();
        var similarity = instance.similarity();
        var maxInputTokens = instance.maxInputTokens();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(5)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> createUri("https://" + randomAlphaOfLength(10) + ".example"));
            case 2 -> dimensions = randomValueOtherThan(dimensions, () -> randomIntBetween(32, 256));
            case 3 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(SimilarityMeasure.values()));
            case 4 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomIntBetween(128, 256));
            case 5 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new LlamaEmbeddingsServiceSettings(modelId, uri, dimensions, similarity, maxInputTokens, rateLimitSettings);
    }

    @Override
    protected LlamaEmbeddingsServiceSettings mutateInstanceForVersion(LlamaEmbeddingsServiceSettings instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected LlamaEmbeddingsServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return LlamaEmbeddingsServiceSettings.createParser(true).apply(parser, ConfigurationParseContext.PERSISTENT).build();
    }

    private static LlamaEmbeddingsServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);
        var uri = createUri("https://" + randomAlphaOfLength(10) + ".example");
        var similarityMeasure = randomFrom(SimilarityMeasure.values());
        var dimensions = randomIntBetween(32, 256);
        var maxInputTokens = randomIntBetween(128, 256);
        return new LlamaEmbeddingsServiceSettings(
            modelId,
            uri,
            dimensions,
            similarityMeasure,
            maxInputTokens,
            RateLimitSettingsTests.createRandom()
        );
    }

    public static Map<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable Integer dimensions,
        @Nullable String similarity,
        @Nullable Integer maxInputTokens,
        @Nullable Integer rateLimit
    ) {
        var map = new HashMap<String, Object>();
        if (modelId != null) {
            map.put(ServiceFields.MODEL_ID, modelId);
        }
        if (url != null) {
            map.put(ServiceFields.URL, url);
        }
        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }
        if (similarity != null) {
            map.put(ServiceFields.SIMILARITY, similarity);
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
