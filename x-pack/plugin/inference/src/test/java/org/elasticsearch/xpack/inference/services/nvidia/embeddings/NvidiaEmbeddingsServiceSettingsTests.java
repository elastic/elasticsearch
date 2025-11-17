/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class NvidiaEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<NvidiaEmbeddingsServiceSettings> {
    private static final String MODEL_ID = "some model";
    private static final String CORRECT_URL = "https://www.elastic.co";
    private static final int DIMENSIONS = 384;
    private static final SimilarityMeasure SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final int MAX_INPUT_TOKENS = 128;
    private static final int RATE_LIMIT = 2;

    public void testFromMap_AllFields_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_ID,
                CORRECT_URL,
                SIMILARITY_MEASURE.toString(),
                DIMENSIONS,
                MAX_INPUT_TOKENS,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    DIMENSIONS,
                    SIMILARITY_MEASURE,
                    MAX_INPUT_TOKENS,
                    new RateLimitSettings(RATE_LIMIT)
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
                    CORRECT_URL,
                    SIMILARITY_MEASURE.toString(),
                    DIMENSIONS,
                    MAX_INPUT_TOKENS,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
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
                MODEL_ID,
                null,
                SIMILARITY_MEASURE.toString(),
                DIMENSIONS,
                MAX_INPUT_TOKENS,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            ConfigurationParseContext.PERSISTENT

        );
        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_ID,
                    ServiceUtils.createOptionalUri(null),
                    DIMENSIONS,
                    SIMILARITY_MEASURE,
                    MAX_INPUT_TOKENS,
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_EmptyUrl_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_ID,
                    "",
                    SIMILARITY_MEASURE.toString(),
                    DIMENSIONS,
                    MAX_INPUT_TOKENS,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
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
                    MODEL_ID,
                    "^^^",
                    SIMILARITY_MEASURE.toString(),
                    DIMENSIONS,
                    MAX_INPUT_TOKENS,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                "Validation Failed: 1: [service_settings] Invalid url [^^^] received for field [url]. "
                    + "Error: unable to parse url [^^^]. Reason: Illegal character in path;"
            )
        );
    }

    public void testFromMap_NoSimilarity_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_ID,
                CORRECT_URL,
                null,
                DIMENSIONS,
                MAX_INPUT_TOKENS,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    DIMENSIONS,
                    null,
                    MAX_INPUT_TOKENS,
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_ID,
                    CORRECT_URL,
                    "by_size",
                    DIMENSIONS,
                    MAX_INPUT_TOKENS,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString(
                "Validation Failed: 1: [service_settings] Invalid value [by_size] received. "
                    + "[similarity] must be one of [cosine, dot_product, l2_norm];"
            )
        );
    }

    public void testFromMap_NoDimensions_Persistent_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_ID,
                CORRECT_URL,
                SIMILARITY_MEASURE.toString(),
                null,
                MAX_INPUT_TOKENS,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    null,
                    SIMILARITY_MEASURE,
                    MAX_INPUT_TOKENS,
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_NoDimensions_Request_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_ID,
                CORRECT_URL,
                SIMILARITY_MEASURE.toString(),
                null,
                MAX_INPUT_TOKENS,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    null,
                    SIMILARITY_MEASURE,
                    MAX_INPUT_TOKENS,
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_WithDimensions_Request_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_ID,
                CORRECT_URL,
                SIMILARITY_MEASURE.toString(),
                DIMENSIONS,
                MAX_INPUT_TOKENS,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    DIMENSIONS,
                    SIMILARITY_MEASURE,
                    MAX_INPUT_TOKENS,
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_ZeroDimensions_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_ID,
                    CORRECT_URL,
                    SIMILARITY_MEASURE.toString(),
                    0,
                    MAX_INPUT_TOKENS,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
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
                    MODEL_ID,
                    CORRECT_URL,
                    SIMILARITY_MEASURE.toString(),
                    -10,
                    MAX_INPUT_TOKENS,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [-10]. [dimensions] must be a positive integer;")
        );
    }

    public void testFromMap_NoInputTokens_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_ID,
                CORRECT_URL,
                SIMILARITY_MEASURE.toString(),
                DIMENSIONS,
                null,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    DIMENSIONS,
                    SIMILARITY_MEASURE,
                    null,
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_ZeroInputTokens_ThrowsException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> NvidiaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_ID,
                    CORRECT_URL,
                    SIMILARITY_MEASURE.toString(),
                    DIMENSIONS,
                    0,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
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
                    MODEL_ID,
                    CORRECT_URL,
                    SIMILARITY_MEASURE.toString(),
                    DIMENSIONS,
                    -10,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] Invalid value [-10]. [max_input_tokens] must be a positive integer;")
        );
    }

    public void testFromMap_NoRateLimit_Success() {
        var serviceSettings = NvidiaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(MODEL_ID, CORRECT_URL, SIMILARITY_MEASURE.toString(), DIMENSIONS, MAX_INPUT_TOKENS, null),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new NvidiaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    DIMENSIONS,
                    SIMILARITY_MEASURE,
                    MAX_INPUT_TOKENS,
                    new RateLimitSettings(3000)
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new NvidiaEmbeddingsServiceSettings(
            MODEL_ID,
            CORRECT_URL,
            DIMENSIONS,
            SIMILARITY_MEASURE,
            MAX_INPUT_TOKENS,
            new RateLimitSettings(3)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
                "model_id": "some model",
                "url": "https://www.elastic.co",
                "rate_limit": {
                    "requests_per_minute": 3
                },
                "dimensions": 384,
                "similarity": "dot_product",
                "max_input_tokens": 128
            }
            """)));
    }

    public void testStreamInputAndOutput_WritesValuesCorrectly() throws IOException {
        var outputBuffer = new BytesStreamOutput();
        var settings = new NvidiaEmbeddingsServiceSettings(
            MODEL_ID,
            CORRECT_URL,
            DIMENSIONS,
            SIMILARITY_MEASURE,
            MAX_INPUT_TOKENS,
            new RateLimitSettings(3)
        );
        settings.writeTo(outputBuffer);

        var outputBufferRef = outputBuffer.bytes();
        var inputBuffer = new ByteArrayStreamInput(outputBufferRef.array());

        var settingsFromBuffer = new NvidiaEmbeddingsServiceSettings(inputBuffer);

        assertThat(settingsFromBuffer, is(settings));
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
            case 1 -> uri = randomValueOtherThan(uri, () -> ServiceUtils.createOptionalUri(randomAlphaOfLengthOrNull(15)));
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
