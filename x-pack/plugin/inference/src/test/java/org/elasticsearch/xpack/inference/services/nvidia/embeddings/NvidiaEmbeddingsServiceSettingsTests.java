/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.embeddings;

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
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class NvidiaEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<NvidiaEmbeddingsServiceSettings> {
    private static final String MODEL_VALUE = "some_model";
    private static final String URL_VALUE = "http://www.abc.com";
    private static final String URL_DEFAULT_VALUE = "https://integrate.api.nvidia.com/v1/embeddings";
    private static final String URL_INVALID_VALUE = "^^^";
    private static final int DIMENSIONS_VALUE = 384;
    private static final SimilarityMeasure SIMILARITY_MEASURE_VALUE = SimilarityMeasure.DOT_PRODUCT;
    private static final int MAX_INPUT_TOKENS_VALUE = 128;
    private static final int RATE_LIMIT_VALUE = 2;
    private static final int RATE_LIMIT_DEFAULT_VALUE = 3000;
    private static final int MAX_INPUT_TOKENS_NEGATIVE_VALUE = -10;
    private static final int DIMENSIONS_NEGATIVE_VALUE = -10;
    private static final String SIMILARITY_INVALID_VALUE = "by_size";

    public void testFromMap_AllFields_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    URL_VALUE,
                    DIMENSIONS_VALUE,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE)
                )
            )
        );
    }

    public void testFromMap_NoModelId_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    null,
                    URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [model_id];")
        );
    }

    public void testFromMap_NoUrl_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                null,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
            ),
            ConfigurationParseContext.PERSISTENT

        );
        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    URL_DEFAULT_VALUE,
                    DIMENSIONS_VALUE,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE)
                )
            )
        );
    }

    public void testFromMap_EmptyUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    "",
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value empty string. [url] must be a non-empty string;")
        );
    }

    public void testFromMap_InvalidUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    URL_INVALID_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(thrownException.getMessage(), containsString(Strings.format("""
            Validation Failed: 1: [service_settings] Invalid url [%s] received for field [url]. \
            Error: unable to parse url [%s]. Reason: Illegal character in path;""", URL_INVALID_VALUE, URL_INVALID_VALUE)));
    }

    public void testFromMap_NoSimilarity_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                URL_VALUE,
                null,
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    URL_VALUE,
                    DIMENSIONS_VALUE,
                    null,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE)
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    URL_VALUE,
                    SIMILARITY_INVALID_VALUE,
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(thrownException.getMessage(), containsString(Strings.format("""
            Validation Failed: 1: [service_settings] Invalid value [%s] received. \
            [similarity] must be one of [cosine, dot_product, l2_norm];""", SIMILARITY_INVALID_VALUE)));
    }

    public void testFromMap_NoDimensions_Persistent_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    null,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(thrownException.getMessage(), containsString("[service_settings] does not contain the required setting [dimensions];"));
    }

    public void testFromMap_NoDimensions_Request_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                null,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    URL_VALUE,
                    null,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE)
                )
            )
        );
    }

    public void testFromMap_WithDimensions_Request_Success_DimensionsIgnored() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    URL_VALUE,
                    null,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_VALUE)
                )
            )
        );
    }

    public void testFromMap_ZeroDimensions_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    0,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [0]. [dimensions] must be a positive integer;")
        );
    }

    public void testFromMap_NegativeDimensions_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_NEGATIVE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%s]. [dimensions] must be a positive integer;",
                    DIMENSIONS_NEGATIVE_VALUE
                )
            )
        );
    }

    public void testFromMap_NoInputTokens_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                null,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    URL_VALUE,
                    DIMENSIONS_VALUE,
                    SIMILARITY_MEASURE_VALUE,
                    null,
                    new RateLimitSettings(RATE_LIMIT_VALUE)
                )
            )
        );
    }

    public void testFromMap_ZeroInputTokens_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    0,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [0]. [max_input_tokens] must be a positive integer;")
        );
    }

    public void testFromMap_NegativeInputTokens_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_VALUE,
                    URL_VALUE,
                    SIMILARITY_MEASURE_VALUE.toString(),
                    DIMENSIONS_VALUE,
                    MAX_INPUT_TOKENS_NEGATIVE_VALUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT_VALUE))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                Strings.format(
                    "Validation Failed: 1: [service_settings] Invalid value [%s]. [max_input_tokens] must be a positive integer;",
                    MAX_INPUT_TOKENS_NEGATIVE_VALUE
                )
            )
        );
    }

    public void testFromMap_NoRateLimit_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_VALUE,
                URL_VALUE,
                SIMILARITY_MEASURE_VALUE.toString(),
                DIMENSIONS_VALUE,
                MAX_INPUT_TOKENS_VALUE,
                null
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_VALUE,
                    URL_VALUE,
                    DIMENSIONS_VALUE,
                    SIMILARITY_MEASURE_VALUE,
                    MAX_INPUT_TOKENS_VALUE,
                    new RateLimitSettings(RATE_LIMIT_DEFAULT_VALUE)
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new NvidiaEmbeddingsServiceSettings(
            MODEL_VALUE,
            URL_VALUE,
            DIMENSIONS_VALUE,
            SIMILARITY_MEASURE_VALUE,
            MAX_INPUT_TOKENS_VALUE,
            new RateLimitSettings(RATE_LIMIT_VALUE)
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
                        MODEL_VALUE,
                        URL_VALUE,
                        RATE_LIMIT_VALUE,
                        DIMENSIONS_VALUE,
                        SIMILARITY_MEASURE_VALUE.toString(),
                        MAX_INPUT_TOKENS_VALUE
                    )
                )
            )
        );
    }

    public void testToXContent_WritesDefaultValues() throws IOException {
        var entity = new NvidiaEmbeddingsServiceSettings(MODEL_VALUE, createOptionalUri(null), null, null, null, null);

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
            """, MODEL_VALUE, URL_DEFAULT_VALUE, RATE_LIMIT_DEFAULT_VALUE))));
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
     * @param similarity the similarity measure to set
     * @param dimensions the dimensions to set
     * @param maxInputTokens the max input tokens to set
     * @param rateLimitSettings the rate limit settings to set
     * @return a map representing the service settings
     */
    public static HashMap<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable String similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable HashMap<String, Integer> rateLimitSettings
    ) {
        HashMap<String, Object> result = new HashMap<>();
        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        if (url != null) {
            result.put(ServiceFields.URL, url);
        }
        if (similarity != null) {
            result.put(ServiceFields.SIMILARITY, similarity);
        }
        if (dimensions != null) {
            result.put(ServiceFields.DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            result.put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (rateLimitSettings != null) {
            result.put(RateLimitSettings.FIELD_NAME, rateLimitSettings);
        }
        return result;
    }
}
