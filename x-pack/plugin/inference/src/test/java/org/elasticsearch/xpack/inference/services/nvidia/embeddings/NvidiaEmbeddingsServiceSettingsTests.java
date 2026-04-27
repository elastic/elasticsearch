/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
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

public class NvidiaEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<NvidiaEmbeddingsServiceSettings> {

    private static final URI TEST_URI = URI.create("http://www.abc.com");
    private static final URI INITIAL_TEST_URI = URI.create("http://www.initial.com");
    private static final URI DEFAULT_URI = URI.create("https://integrate.api.nvidia.com/v1/embeddings");

    private static final String TEST_MODEL_ID = "test-model";
    private static final String INITIAL_TEST_MODEL_ID = "initial-model";

    private static final int TEST_DIMENSIONS = 384;
    private static final int INITIAL_TEST_DIMENSIONS = 128;

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;

    private static final int TEST_MAX_INPUT_TOKENS = 256;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 64;

    private static final int TEST_RATE_LIMIT = 500;
    private static final int INITIAL_TEST_RATE_LIMIT = 100;
    private static final int DEFAULT_RATE_LIMIT = 3000;

    private static final String INVALID_TEST_URL = "^^^";
    private static final String INVALID_SIMILARITY_STRING = "by_size";

    public void testFromMap_Request_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                null,
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_MAX_INPUT_TOKENS,
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
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

    public void testFromMap_Request_OnlyMandatoryFields_UsesDefaultValues_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null, null),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(new NvidiaEmbeddingsServiceSettings(TEST_MODEL_ID, DEFAULT_URI, null, null, null, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_Persistent_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                TEST_DIMENSIONS,
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_MAX_INPUT_TOKENS,
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
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

    public void testFromMap_Persistent_OnlyMandatoryFields_UsesDefaultValues_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, TEST_DIMENSIONS, null, null, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    DEFAULT_URI,
                    TEST_DIMENSIONS,
                    null,
                    null,
                    new RateLimitSettings(DEFAULT_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_NoModelId_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
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

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.MODEL_ID))
        );
    }

    public void testFromMap_EmptyUrl_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
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

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value empty string. [%s] must be a non-empty string", ServiceFields.URL))
        );
    }

    public void testFromMap_InvalidUrl_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    INVALID_TEST_URL,
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(thrownException.validationErrors().getFirst(), is(Strings.format("""
            [service_settings] Invalid url [%s] received for field [%s]. \
            Error: unable to parse url [%s]. Reason: Illegal character in path""", INVALID_TEST_URL, ServiceFields.URL, INVALID_TEST_URL)));
    }

    public void testFromMap_InvalidSimilarity_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    TEST_DIMENSIONS,
                    INVALID_SIMILARITY_STRING,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
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

    public void testFromMap_Persistent_NoDimensions_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    null,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", ServiceFields.DIMENSIONS))
        );
    }

    public void testFromMap_Request_NoDimensions_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_URI.toString(),
                null,
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_MAX_INPUT_TOKENS,
                TEST_RATE_LIMIT
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
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

    public void testFromMap_Request_WithDimensions_Success_DimensionsIgnored() {
        HashMap<String, Object> serviceSettingsMap = buildServiceSettingsMap(
            TEST_MODEL_ID,
            TEST_URI.toString(),
            TEST_DIMENSIONS,
            TEST_SIMILARITY_MEASURE.toString(),
            TEST_MAX_INPUT_TOKENS,
            TEST_RATE_LIMIT
        );
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(serviceSettingsMap, ConfigurationParseContext.REQUEST);

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_URI,
                    null,
                    TEST_SIMILARITY_MEASURE,
                    TEST_MAX_INPUT_TOKENS,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
        assertThat(serviceSettingsMap, is(Map.of(ServiceFields.DIMENSIONS, TEST_DIMENSIONS)));
    }

    public void testFromMap_Persistent_ZeroDimensions_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    0,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value [0]. [%s] must be a positive integer", ServiceFields.DIMENSIONS))
        );
    }

    public void testFromMap_Persistent_NegativeDimensions_ThrowsValidationError() {
        var negativeDimensions = -10;
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    negativeDimensions,
                    TEST_SIMILARITY_MEASURE.toString(),
                    TEST_MAX_INPUT_TOKENS,
                    TEST_RATE_LIMIT
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[service_settings] Invalid value [%s]. [%s] must be a positive integer",
                    negativeDimensions,
                    ServiceFields.DIMENSIONS
                )
            )
        );
    }

    public void testFromMap_ZeroMaxInputTokens_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    TEST_MODEL_ID,
                    TEST_URI.toString(),
                    TEST_DIMENSIONS,
                    TEST_SIMILARITY_MEASURE.toString(),
                    0,
                    TEST_RATE_LIMIT
                ),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] Invalid value [0]. [%s] must be a positive integer", ServiceFields.MAX_INPUT_TOKENS))
        );
    }

    public void testFromMap_NegativeMaxInputTokens_ThrowsValidationError() {
        var negativeMaxInputTokens = -10;
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
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

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[service_settings] Invalid value [%s]. [%s] must be a positive integer",
                    negativeMaxInputTokens,
                    ServiceFields.MAX_INPUT_TOKENS
                )
            )
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(
            TEST_MODEL_ID,
            TEST_URI.toString(),
            TEST_DIMENSIONS,
            TEST_SIMILARITY_MEASURE.toString(),
            TEST_MAX_INPUT_TOKENS,
            TEST_RATE_LIMIT
        );
        var originalServiceSettings = new NvidiaEmbeddingsServiceSettings(
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
                new NvidiaEmbeddingsServiceSettings(
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
        var originalServiceSettings = new NvidiaEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_URI,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new NvidiaEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_URI,
            TEST_DIMENSIONS,
            TEST_SIMILARITY_MEASURE,
            TEST_MAX_INPUT_TOKENS,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                XContentHelper.stripWhitespace(
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
                )
            )
        );
    }

    public void testToXContent_DoesNotWriteOptionalValues_WritesDefaultValues() throws IOException {
        var entity = new NvidiaEmbeddingsServiceSettings(TEST_MODEL_ID, (URI) null, null, null, null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {
                "model_id": "%s",
                "url": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, DEFAULT_URI.toString(), DEFAULT_RATE_LIMIT))));
    }

    @Override
    protected Writeable.Reader<NvidiaEmbeddingsServiceSettings> instanceReader() {
        return NvidiaEmbeddingsServiceSettings::new;
    }

    @Override
    protected NvidiaEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected NvidiaEmbeddingsServiceSettings mutateInstance(NvidiaEmbeddingsServiceSettings instance) throws IOException {
        String modelId = instance.modelId();
        URI uri = instance.uri();
        Integer dimensions = instance.dimensions();
        SimilarityMeasure similarity = instance.similarity();
        Integer maxInputTokens = instance.maxInputTokens();
        RateLimitSettings rateLimitSettings = instance.rateLimitSettings();

        switch (between(0, 5)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> uri = randomValueOtherThan(uri, () -> createUri(randomAlphaOfLength(15)));
            case 2 -> dimensions = randomValueOtherThan(dimensions, () -> randomBoolean() ? randomIntBetween(32, 256) : null);
            case 3 -> similarity = randomValueOtherThan(similarity, () -> randomBoolean() ? randomFrom(SimilarityMeasure.values()) : null);
            case 4 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomBoolean() ? randomIntBetween(128, 256) : null);
            case 5 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new NvidiaEmbeddingsServiceSettings(modelId, uri, dimensions, similarity, maxInputTokens, rateLimitSettings);
    }

    private static NvidiaEmbeddingsServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);
        var url = randomAlphaOfLengthOrNull(15);
        var similarityMeasure = randomBoolean() ? randomFrom(SimilarityMeasure.values()) : null;
        var dimensions = randomBoolean() ? randomIntBetween(32, 256) : null;
        var maxInputTokens = randomBoolean() ? randomIntBetween(128, 256) : null;
        return new NvidiaEmbeddingsServiceSettings(
            modelId,
            url,
            dimensions,
            similarityMeasure,
            maxInputTokens,
            RateLimitSettingsTests.createRandom()
        );
    }

    /**
     * Helper method to build a service settings map.
     * @param modelId the model id to set
     * @param url the url to set
     * @param dimensions the dimensions to set
     * @param similarity the similarity measure to set
     * @param maxInputTokens the max input tokens to set
     * @param rateLimit the rate limit value (requests per minute)
     * @return a map representing the service settings
     */
    public static HashMap<String, Object> buildServiceSettingsMap(
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

    @Override
    protected NvidiaEmbeddingsServiceSettings mutateInstanceForVersion(NvidiaEmbeddingsServiceSettings instance, TransportVersion version) {
        return instance;
    }
}
