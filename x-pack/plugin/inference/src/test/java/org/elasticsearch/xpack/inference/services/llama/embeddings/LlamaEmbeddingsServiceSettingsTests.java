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
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.llama.AbstractLlamaServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

public class LlamaEmbeddingsServiceSettingsTests extends AbstractLlamaServiceSettingsTests<LlamaEmbeddingsServiceSettings> {

    private static final int TEST_DIMENSIONS = 384;
    private static final int INITIAL_TEST_DIMENSIONS = 256;

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;

    private static final int TEST_MAX_INPUT_TOKENS = 128;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 64;

    @Override
    protected LlamaEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        return LlamaEmbeddingsServiceSettings.fromMap(map, context);
    }

    @Override
    protected Map<String, Object> buildCommonServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable Integer rateLimit
    ) {
        return buildServiceSettingsMap(modelId, url, null, null, null, rateLimit);
    }

    @Override
    protected LlamaEmbeddingsServiceSettings createServiceSettings(String modelId, URI uri, RateLimitSettings rateLimitSettings) {
        return new LlamaEmbeddingsServiceSettings(modelId, uri, null, null, null, rateLimitSettings);
    }

    @Override
    protected List<String> additionalImmutableFields() {
        return List.of(ServiceFields.DIMENSIONS, ServiceFields.SIMILARITY);
    }

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
            is(
                Strings.format(
                    "[%s] Invalid value empty string. [%s] must be a non-empty string",
                    ModelConfigurations.SERVICE_SETTINGS,
                    ServiceFields.URL
                )
            )
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
        assertThat(
            thrownException.getMessage(),
            endsWith(Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, ServiceFields.SIMILARITY))
        );
        assertThat(
            thrownException.getCause().getMessage(),
            is(Strings.format("Invalid value [%s]; expected one of [cosine, dot_product, l2_norm]", invalidSimilarity))
        );
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
        assertFromMap_NonPositiveDimensions_ThrowsException(0);
    }

    public void testFromMap_NegativeDimensions_ThrowsException() {
        assertFromMap_NonPositiveDimensions_ThrowsException(randomNegativeInt());
    }

    private static void assertFromMap_NonPositiveDimensions_ThrowsException(int dimensions) {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    dimensions,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            endsWith(Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, ServiceFields.DIMENSIONS))
        );
        assertThat(
            thrownException.getCause().getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ModelConfigurations.SERVICE_SETTINGS,
                    dimensions,
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
        assertFromMap_InvalidInputTokens_ThrowsException(0);
    }

    public void testFromMap_NegativeInputTokens_ThrowsException() {
        assertFromMap_InvalidInputTokens_ThrowsException(randomNegativeInt());
    }

    private static void assertFromMap_InvalidInputTokens_ThrowsException(int nonPositiveMaxInputTokens) {
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE.toString(),
                    nonPositiveMaxInputTokens,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );
        assertThat(
            thrownException.getMessage(),
            endsWith(
                Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, ServiceFields.MAX_INPUT_TOKENS)
            )
        );
        assertThat(
            thrownException.getCause().getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ModelConfigurations.SERVICE_SETTINGS,
                    nonPositiveMaxInputTokens,
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

    public void testUpdateServiceSettings_EmptyMap_PreservesTaskSpecificFields() {
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

    public void testUpdateServiceSettings_ZeroInputTokens_ThrowsException() {
        assertUpdateServiceSettings_InvalidMaxInputTokens_ThrowsException(0);
    }

    public void testUpdateServiceSettings_NegativeInputTokens_ThrowsException() {
        assertUpdateServiceSettings_InvalidMaxInputTokens_ThrowsException(randomNegativeInt());
    }

    private static void assertUpdateServiceSettings_InvalidMaxInputTokens_ThrowsException(int nonPositiveMaxInputTokens) {
        var serviceSettings = new LlamaEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var e = expectThrows(
            XContentParseException.class,
            () -> serviceSettings.updateServiceSettings(new HashMap<>(Map.of(ServiceFields.MAX_INPUT_TOKENS, nonPositiveMaxInputTokens)))
        );
        assertThat(
            e.getMessage(),
            endsWith(
                Strings.format("[%s] failed to parse field [%s]", ModelConfigurations.SERVICE_SETTINGS, ServiceFields.MAX_INPUT_TOKENS)
            )
        );
        assertThat(
            e.getCause().getMessage(),
            is(
                Strings.format(
                    "[%s] Invalid value [%d]. [%s] must be a positive integer",
                    ModelConfigurations.SERVICE_SETTINGS,
                    nonPositiveMaxInputTokens,
                    ServiceFields.MAX_INPUT_TOKENS
                )
            )
        );
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
            case 2 -> dimensions = randomValueOtherThan(dimensions, () -> randomFrom(randomIntBetween(32, 256), null));
            case 3 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomFrom(SimilarityMeasure.values()), null));
            case 4 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
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
        var similarityMeasure = randomFrom(randomFrom(SimilarityMeasure.values()), null);
        var dimensions = randomFrom(randomIntBetween(32, 256), null);
        var maxInputTokens = randomFrom(randomIntBetween(128, 256), null);
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
