/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.mistral.MistralConstants;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class MistralEmbeddingsServiceSettingsTests extends ESTestCase {

    private static final String TEST_MODEL_ID = "mistral-embed";
    private static final String INITIAL_TEST_MODEL_ID = "initial-mistral-embed";

    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 512;

    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 128;

    private static final SimilarityMeasure TEST_SIMILARITY = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY = SimilarityMeasure.DOT_PRODUCT;

    private static final int TEST_RATE_LIMIT = 3;
    private static final int INITIAL_TEST_RATE_LIMIT = 100;

    private static final int DEFAULT_RATE_LIMIT = 240;

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var serviceSettings = MistralEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, TEST_DIMENSIONS, TEST_MAX_INPUT_TOKENS, TEST_SIMILARITY, TEST_RATE_LIMIT),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new MistralEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    TEST_SIMILARITY,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testFromMap_OnlyMandatoryFields_UsesDefaultRateLimit_Success() {
        var serviceSettings = MistralEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(TEST_MODEL_ID, null, null, null, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(new MistralEmbeddingsServiceSettings(TEST_MODEL_ID, null, null, null, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_NoModel_ThrowsValidationError() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> MistralEmbeddingsServiceSettings.fromMap(new HashMap<>(), randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(Strings.format("[service_settings] does not contain the required setting [%s]", MistralConstants.MODEL_FIELD))
        );
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreZero() {
        var zeroDimensions = 0;

        var thrownException = expectThrows(
            ValidationException.class,
            () -> MistralEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, zeroDimensions, null, null, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[service_settings] Invalid value [%d]. [%s] must be a positive integer",
                    zeroDimensions,
                    ServiceFields.DIMENSIONS
                )
            )
        );
    }

    public void testFromMap_ThrowsException_WhenDimensionsAreNegative() {
        var negativeDimensions = randomNegativeInt();

        var thrownException = expectThrows(
            ValidationException.class,
            () -> MistralEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, negativeDimensions, null, TEST_SIMILARITY, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[service_settings] Invalid value [%d]. [%s] must be a positive integer",
                    negativeDimensions,
                    ServiceFields.DIMENSIONS
                )
            )
        );
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreZero() {
        var zeroMaxInputTokens = 0;

        var thrownException = expectThrows(
            ValidationException.class,
            () -> MistralEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, null, zeroMaxInputTokens, TEST_SIMILARITY, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[service_settings] Invalid value [%d]. [%s] must be a positive integer",
                    zeroMaxInputTokens,
                    ServiceFields.MAX_INPUT_TOKENS
                )
            )
        );
    }

    public void testFromMap_ThrowsException_WhenMaxInputTokensAreNegative() {
        var negativeMaxInputTokens = randomNegativeInt();

        var thrownException = expectThrows(
            ValidationException.class,
            () -> MistralEmbeddingsServiceSettings.fromMap(
                buildServiceSettingsMap(TEST_MODEL_ID, null, negativeMaxInputTokens, TEST_SIMILARITY, null),
                randomFrom(ConfigurationParseContext.values())
            )
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is(
                Strings.format(
                    "[service_settings] Invalid value [%d]. [%s] must be a positive integer",
                    negativeMaxInputTokens,
                    ServiceFields.MAX_INPUT_TOKENS
                )
            )
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(TEST_MODEL_ID, TEST_DIMENSIONS, TEST_MAX_INPUT_TOKENS, TEST_SIMILARITY, TEST_RATE_LIMIT);
        var originalServiceSettings = new MistralEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_SIMILARITY,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new MistralEmbeddingsServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    INITIAL_TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS,
                    INITIAL_TEST_SIMILARITY,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new MistralEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_SIMILARITY,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        assertThat(originalServiceSettings.updateServiceSettings(new HashMap<>()), is(originalServiceSettings));
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new MistralEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "model": "%s",
                "dimensions": %d,
                "similarity": "%s",
                "max_input_tokens": %d,
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_DIMENSIONS, TEST_SIMILARITY, TEST_MAX_INPUT_TOKENS, TEST_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit() throws IOException {
        var entity = new MistralEmbeddingsServiceSettings(TEST_MODEL_ID, null, null, null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace(Strings.format("""
            {
                "model": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, DEFAULT_RATE_LIMIT));

        assertThat(xContentResult, is(expected));
    }

    public void testStreamInputAndOutput_WritesValuesCorrectly() throws IOException {
        var outputBuffer = new BytesStreamOutput();
        var settings = new MistralEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS,
            TEST_SIMILARITY,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );
        settings.writeTo(outputBuffer);

        var outputBufferRef = outputBuffer.bytes();
        var inputBuffer = new ByteArrayStreamInput(outputBufferRef.array());

        var settingsFromBuffer = new MistralEmbeddingsServiceSettings(inputBuffer);

        assertEquals(settings, settingsFromBuffer);
    }

    public static Map<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer rateLimit
    ) {
        var map = new HashMap<String, Object>();

        if (modelId != null) {
            map.put(MistralConstants.MODEL_FIELD, modelId);
        }
        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            map.put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (similarity != null) {
            map.put(ServiceFields.SIMILARITY, similarity.toString());
        }
        if (rateLimit != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }

        return map;
    }

}
