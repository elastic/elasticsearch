/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.embeddings;

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
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class LlamaEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<LlamaEmbeddingsServiceSettings> {
    private static final String MODEL_ID = "some model";
    private static final String CORRECT_URL = "https://www.elastic.co";
    private static final int DIMENSIONS = 384;
    private static final SimilarityMeasure SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;
    private static final int MAX_INPUT_TOKENS = 128;
    private static final int RATE_LIMIT = 2;
    private static final Boolean DIMENSIONS_SET_BY_USER_TRUE = Boolean.TRUE;
    private static final String DIMENSIONS_SET_BY_USER_FIELD = "dimensions_set_by_user";

    public void testFromMap_AllFields_Persistent_Success() {
        var serviceSettings = LlamaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_ID,
                CORRECT_URL,
                SIMILARITY_MEASURE.toString(),
                DIMENSIONS,
                MAX_INPUT_TOKENS,
                DIMENSIONS_SET_BY_USER_TRUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new LlamaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    DIMENSIONS,
                    SIMILARITY_MEASURE,
                    MAX_INPUT_TOKENS,
                    DIMENSIONS_SET_BY_USER_TRUE,
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_NoModelId_Failure() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    null,
                    CORRECT_URL,
                    SIMILARITY_MEASURE.toString(),
                    DIMENSIONS,
                    MAX_INPUT_TOKENS,
                    DIMENSIONS_SET_BY_USER_TRUE,
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

    public void testFromMap_NoUrl_Failure() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_ID,
                    null,
                    SIMILARITY_MEASURE.toString(),
                    DIMENSIONS,
                    MAX_INPUT_TOKENS,
                    DIMENSIONS_SET_BY_USER_TRUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(
            thrownException.getMessage(),
            containsString("Validation Failed: 1: [service_settings] does not contain the required setting [url];")
        );
    }

    public void testFromMap_EmptyUrl_Failure() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_ID,
                    "",
                    SIMILARITY_MEASURE.toString(),
                    DIMENSIONS,
                    MAX_INPUT_TOKENS,
                    DIMENSIONS_SET_BY_USER_TRUE,
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

    public void testFromMap_InvalidUrl_Failure() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_ID,
                    "^^^",
                    SIMILARITY_MEASURE.toString(),
                    DIMENSIONS,
                    MAX_INPUT_TOKENS,
                    DIMENSIONS_SET_BY_USER_TRUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(thrownException.getMessage(), containsString("""
            Validation Failed: 1: [service_settings] Invalid url [^^^] received for field [url]. \
            Error: unable to parse url [^^^]. Reason: Illegal character in path;"""));
    }

    public void testFromMap_NoSimilarity_Success() {
        var serviceSettings = LlamaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_ID,
                CORRECT_URL,
                null,
                DIMENSIONS,
                MAX_INPUT_TOKENS,
                DIMENSIONS_SET_BY_USER_TRUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new LlamaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    DIMENSIONS,
                    null,
                    MAX_INPUT_TOKENS,
                    DIMENSIONS_SET_BY_USER_TRUE,
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_InvalidSimilarity_Failure() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_ID,
                    CORRECT_URL,
                    "by_size",
                    DIMENSIONS,
                    MAX_INPUT_TOKENS,
                    DIMENSIONS_SET_BY_USER_TRUE,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                ),
                ConfigurationParseContext.PERSISTENT
            )
        );
        assertThat(thrownException.getMessage(), containsString("""
            Validation Failed: 1: [service_settings] Invalid value [by_size] received. \
            [similarity] must be one of [cosine, dot_product, l2_norm];"""));
    }

    // Test cases for dimensions_set_by_user and dimensions fields

    public void testFromMap_RequestContext_DimensionsSetByUserTrue_DimensionsPositive_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            DIMENSIONS,
            DIMENSIONS_SET_BY_USER_TRUE,
            ConfigurationParseContext.REQUEST,
            "Validation Failed: 1: [service_settings] does not allow the setting [dimensions_set_by_user];"
        );
    }

    public void testFromMap_RequestContext_DimensionsSetByUserTrue_DimensionsZero_Failed() {
        testFromMap_Dimensions_AssertValidationException(0, DIMENSIONS_SET_BY_USER_TRUE, ConfigurationParseContext.REQUEST, """
            Validation Failed: 1: [service_settings] Invalid value [0]. [dimensions] must be a positive integer;\
            2: [service_settings] does not allow the setting [dimensions_set_by_user];""");
    }

    public void testFromMap_RequestContext_DimensionsSetByUserTrue_DimensionsNegative_Failed() {
        testFromMap_Dimensions_AssertValidationException(-1, DIMENSIONS_SET_BY_USER_TRUE, ConfigurationParseContext.REQUEST, """
            Validation Failed: 1: [service_settings] Invalid value [-1]. [dimensions] must be a positive integer;\
            2: [service_settings] does not allow the setting [dimensions_set_by_user];""");
    }

    public void testFromMap_RequestContext_DimensionsSetByUserTrue_DimensionsNull_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            null,
            DIMENSIONS_SET_BY_USER_TRUE,
            ConfigurationParseContext.REQUEST,
            "Validation Failed: 1: [service_settings] does not allow the setting [dimensions_set_by_user];"
        );
    }

    public void testFromMap_RequestContext_DimensionsSetByUserFalse_DimensionsPositive_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            DIMENSIONS,
            false,
            ConfigurationParseContext.REQUEST,
            "Validation Failed: 1: [service_settings] does not allow the setting [dimensions_set_by_user];"
        );
    }

    public void testFromMap_RequestContext_DimensionsSetByUserFalse_DimensionsZero_Failed() {
        testFromMap_Dimensions_AssertValidationException(0, false, ConfigurationParseContext.REQUEST, """
            Validation Failed: 1: [service_settings] Invalid value [0]. [dimensions] must be a positive integer;\
            2: [service_settings] does not allow the setting [dimensions_set_by_user];""");
    }

    public void testFromMap_RequestContext_DimensionsSetByUserFalse_DimensionsNegative_Failed() {
        testFromMap_Dimensions_AssertValidationException(-1, false, ConfigurationParseContext.REQUEST, """
            Validation Failed: 1: [service_settings] Invalid value [-1]. [dimensions] must be a positive integer;\
            2: [service_settings] does not allow the setting [dimensions_set_by_user];""");
    }

    public void testFromMap_RequestContext_DimensionsSetByUserFalse_DimensionsNull_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            null,
            false,
            ConfigurationParseContext.REQUEST,
            "Validation Failed: 1: [service_settings] does not allow the setting [dimensions_set_by_user];"
        );
    }

    public void testFromMap_RequestContext_DimensionsSetByUserNull_DimensionsPositive_Success() {
        testFromMap_Dimensions_AssertValidationSuccess(DIMENSIONS, null, DIMENSIONS_SET_BY_USER_TRUE, ConfigurationParseContext.REQUEST);
    }

    public void testFromMap_RequestContext_DimensionsSetByUserNull_DimensionsZero_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            0,
            null,
            ConfigurationParseContext.REQUEST,
            "Validation Failed: 1: [service_settings] Invalid value [0]. [dimensions] must be a positive integer;"
        );
    }

    public void testFromMap_RequestContext_DimensionsSetByUserNull_DimensionsNegative_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            -1,
            null,
            ConfigurationParseContext.REQUEST,
            "Validation Failed: 1: [service_settings] Invalid value [-1]. [dimensions] must be a positive integer;"
        );
    }

    public void testFromMap_RequestContext_DimensionsSetByUserNull_DimensionsNull_Success() {
        testFromMap_Dimensions_AssertValidationSuccess(null, null, false, ConfigurationParseContext.REQUEST);
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserTrue_DimensionsZero_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            0,
            DIMENSIONS_SET_BY_USER_TRUE,
            ConfigurationParseContext.PERSISTENT,
            "Validation Failed: 1: [service_settings] Invalid value [0]. [dimensions] must be a positive integer;"
        );
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserTrue_DimensionsNegative_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            -1,
            DIMENSIONS_SET_BY_USER_TRUE,
            ConfigurationParseContext.PERSISTENT,
            "Validation Failed: 1: [service_settings] Invalid value [-1]. [dimensions] must be a positive integer;"
        );
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserTrue_DimensionsNull_Success() {
        testFromMap_Dimensions_AssertValidationSuccess(
            null,
            DIMENSIONS_SET_BY_USER_TRUE,
            DIMENSIONS_SET_BY_USER_TRUE,
            ConfigurationParseContext.PERSISTENT
        );
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserFalse_DimensionsPositive_Success() {
        testFromMap_Dimensions_AssertValidationSuccess(DIMENSIONS, false, false, ConfigurationParseContext.PERSISTENT);
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserFalse_DimensionsZero_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            0,
            false,
            ConfigurationParseContext.PERSISTENT,
            "Validation Failed: 1: [service_settings] Invalid value [0]. [dimensions] must be a positive integer;"
        );
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserFalse_DimensionsNegative_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            -1,
            false,
            ConfigurationParseContext.PERSISTENT,
            "Validation Failed: 1: [service_settings] Invalid value [-1]. [dimensions] must be a positive integer;"
        );
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserFalse_DimensionsNull_Success() {
        testFromMap_Dimensions_AssertValidationSuccess(null, false, false, ConfigurationParseContext.PERSISTENT);
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserNull_DimensionsPositive_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            DIMENSIONS,
            null,
            ConfigurationParseContext.PERSISTENT,
            "Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];"
        );
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserNull_DimensionsZero_Failed() {
        testFromMap_Dimensions_AssertValidationException(0, null, ConfigurationParseContext.PERSISTENT, """
            Validation Failed: 1: [service_settings] Invalid value [0]. [dimensions] must be a positive integer;\
            2: [service_settings] does not contain the required setting [dimensions_set_by_user];""");
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserNull_DimensionsNegative_Failed() {
        testFromMap_Dimensions_AssertValidationException(-1, null, ConfigurationParseContext.PERSISTENT, """
            Validation Failed: 1: [service_settings] Invalid value [-1]. [dimensions] must be a positive integer;\
            2: [service_settings] does not contain the required setting [dimensions_set_by_user];""");
    }

    public void testFromMap_PersistentContext_DimensionsSetByUserNull_DimensionsNull_Failed() {
        testFromMap_Dimensions_AssertValidationException(
            null,
            null,
            ConfigurationParseContext.PERSISTENT,
            "Validation Failed: 1: [service_settings] does not contain the required setting [dimensions_set_by_user];"
        );
    }

    private static void testFromMap_Dimensions_AssertValidationSuccess(
        Integer dimensions,
        Boolean dimensionsSetByUser,
        Boolean expectedDimensionsSetByUser,
        ConfigurationParseContext configurationParseContext
    ) {
        var serviceSettings = LlamaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_ID,
                CORRECT_URL,
                SIMILARITY_MEASURE.toString(),
                dimensions,
                MAX_INPUT_TOKENS,
                dimensionsSetByUser,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            configurationParseContext
        );

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new LlamaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    dimensions,
                    SIMILARITY_MEASURE,
                    MAX_INPUT_TOKENS,
                    expectedDimensionsSetByUser,
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    private static void testFromMap_Dimensions_AssertValidationException(
        Integer dimensions,
        Boolean dimensionsSetByUser,
        ConfigurationParseContext configurationParseContext,
        String errorMessage
    ) {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_ID,
                    CORRECT_URL,
                    SIMILARITY_MEASURE.toString(),
                    dimensions,
                    MAX_INPUT_TOKENS,
                    dimensionsSetByUser,
                    new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
                ),
                configurationParseContext
            )
        );

        MatcherAssert.assertThat(thrownException.getMessage(), containsString(errorMessage));
    }

    public void testFromMap_NoInputTokens_Success() {
        var serviceSettings = LlamaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_ID,
                CORRECT_URL,
                SIMILARITY_MEASURE.toString(),
                DIMENSIONS,
                null,
                DIMENSIONS_SET_BY_USER_TRUE,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, RATE_LIMIT))
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new LlamaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    DIMENSIONS,
                    SIMILARITY_MEASURE,
                    null,
                    DIMENSIONS_SET_BY_USER_TRUE,
                    new RateLimitSettings(RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_ZeroInputTokens_Failure() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_ID,
                    CORRECT_URL,
                    SIMILARITY_MEASURE.toString(),
                    DIMENSIONS,
                    0,
                    DIMENSIONS_SET_BY_USER_TRUE,
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

    public void testFromMap_NegativeInputTokens_Failure() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> LlamaEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(
                    MODEL_ID,
                    CORRECT_URL,
                    SIMILARITY_MEASURE.toString(),
                    DIMENSIONS,
                    -10,
                    DIMENSIONS_SET_BY_USER_TRUE,
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
        var serviceSettings = LlamaEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(
                MODEL_ID,
                CORRECT_URL,
                SIMILARITY_MEASURE.toString(),
                DIMENSIONS,
                MAX_INPUT_TOKENS,
                DIMENSIONS_SET_BY_USER_TRUE,
                null
            ),
            ConfigurationParseContext.PERSISTENT
        );

        assertThat(
            serviceSettings,
            is(
                new LlamaEmbeddingsServiceSettings(
                    MODEL_ID,
                    CORRECT_URL,
                    DIMENSIONS,
                    SIMILARITY_MEASURE,
                    MAX_INPUT_TOKENS,
                    DIMENSIONS_SET_BY_USER_TRUE,
                    new RateLimitSettings(3000)
                )
            )
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new LlamaEmbeddingsServiceSettings(
            MODEL_ID,
            CORRECT_URL,
            DIMENSIONS,
            SIMILARITY_MEASURE,
            MAX_INPUT_TOKENS,
            DIMENSIONS_SET_BY_USER_TRUE,
            new RateLimitSettings(3)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is(XContentHelper.stripWhitespace("""
            {
                "model_id": "some model",
                "url": "https://www.elastic.co",
                "dimensions": 384,
                "similarity": "dot_product",
                "max_input_tokens": 128,
                "rate_limit": {
                    "requests_per_minute": 3
                },
                "dimensions_set_by_user": true
            }
            """)));
    }

    public void testStreamInputAndOutput_WritesValuesCorrectly() throws IOException {
        var outputBuffer = new BytesStreamOutput();
        var settings = new LlamaEmbeddingsServiceSettings(
            MODEL_ID,
            CORRECT_URL,
            DIMENSIONS,
            SIMILARITY_MEASURE,
            MAX_INPUT_TOKENS,
            DIMENSIONS_SET_BY_USER_TRUE,
            new RateLimitSettings(3)
        );
        settings.writeTo(outputBuffer);

        var outputBufferRef = outputBuffer.bytes();
        var inputBuffer = new ByteArrayStreamInput(outputBufferRef.array());

        var settingsFromBuffer = new LlamaEmbeddingsServiceSettings(inputBuffer);

        assertEquals(settings, settingsFromBuffer);
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
        return randomValueOtherThan(instance, LlamaEmbeddingsServiceSettingsTests::createRandom);
    }

    private static LlamaEmbeddingsServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(8);
        var url = randomAlphaOfLength(15);
        var similarityMeasure = randomFrom(SimilarityMeasure.values());
        var dimensions = randomIntBetween(32, 256);
        var maxInputTokens = randomIntBetween(128, 256);
        var dimensionsSetByUser = randomBoolean();
        return new LlamaEmbeddingsServiceSettings(
            modelId,
            url,
            dimensions,
            similarityMeasure,
            maxInputTokens,
            dimensionsSetByUser,
            RateLimitSettingsTests.createRandom()
        );
    }

    public static HashMap<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable String similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable Boolean dimensionsSetByUser,
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
        if (dimensionsSetByUser != null) {
            result.put(DIMENSIONS_SET_BY_USER_FIELD, dimensionsSetByUser);
        }
        if (rateLimitSettings != null) {
            result.put(RateLimitSettings.FIELD_NAME, rateLimitSettings);
        }
        return result;
    }
}
