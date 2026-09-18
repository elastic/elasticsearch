/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;
import static org.hamcrest.Matchers.is;

public class GoogleAiStudioEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<GoogleAiStudioEmbeddingsServiceSettings> {
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";

    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 3072;

    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;

    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;

    private static final int TEST_RATE_LIMIT = 20;
    private static final int INITIAL_TEST_RATE_LIMIT = 30;
    private static final int DEFAULT_RATE_LIMIT = 360;

    private static GoogleAiStudioEmbeddingsServiceSettings createRandom() {
        return new GoogleAiStudioEmbeddingsServiceSettings(
            randomAlphaOfLength(8),
            randomNonNegativeIntOrNull(),
            randomNonNegativeIntOrNull(),
            randomFrom(randomSimilarityMeasure(), null),
            randomFrom(RateLimitSettingsTests.createRandom(), null)
        );
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var originalServiceSettings = new GoogleAiStudioEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_SIMILARITY_MEASURE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );

        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(
            buildServiceSettingsMap(
                TEST_MODEL_ID,
                TEST_MAX_INPUT_TOKENS,
                TEST_DIMENSIONS,
                TEST_SIMILARITY_MEASURE.toString(),
                TEST_RATE_LIMIT
            )
        );

        assertThat(
            updatedServiceSettings,
            is(
                new GoogleAiStudioEmbeddingsServiceSettings(
                    INITIAL_TEST_MODEL_ID,
                    TEST_MAX_INPUT_TOKENS,
                    INITIAL_TEST_DIMENSIONS,
                    INITIAL_TEST_SIMILARITY_MEASURE,
                    new RateLimitSettings(TEST_RATE_LIMIT)
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new GoogleAiStudioEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_MAX_INPUT_TOKENS,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_SIMILARITY_MEASURE,
            new RateLimitSettings(INITIAL_TEST_RATE_LIMIT)
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    public void testFromMap_AllFields_CreatesSettingsCorrectly() {
        var modelId = randomAlphaOfLength(8);
        var maxInputTokens = randomIntBetween(1, 1024);
        var dimensions = randomIntBetween(1, 10000);
        var similarity = randomSimilarityMeasure();
        var rateLimit = randomIntBetween(1, 10000);

        var serviceSettings = GoogleAiStudioEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(modelId, maxInputTokens, dimensions, similarity.toString(), rateLimit),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(
                new GoogleAiStudioEmbeddingsServiceSettings(
                    modelId,
                    maxInputTokens,
                    dimensions,
                    similarity,
                    new RateLimitSettings(rateLimit)
                )
            )
        );
    }

    public void testFromMap_OnlyMandatoryFields_CreatesSettingsCorrectly() {
        var modelId = randomAlphaOfLength(8);

        var serviceSettings = GoogleAiStudioEmbeddingsServiceSettings.fromMap(
            buildServiceSettingsMap(modelId, null, null, null, null),
            randomFrom(ConfigurationParseContext.values())
        );

        assertThat(
            serviceSettings,
            is(new GoogleAiStudioEmbeddingsServiceSettings(modelId, null, null, null, new RateLimitSettings(DEFAULT_RATE_LIMIT)))
        );
    }

    public void testFromMap_NoModelId_ThrowsValidationException() {
        var thrownException = expectThrows(
            ValidationException.class,
            () -> GoogleAiStudioEmbeddingsServiceSettings.fromMap(new HashMap<>(), randomFrom(ConfigurationParseContext.values()))
        );

        assertThat(thrownException.validationErrors().size(), is(1));
        assertThat(
            thrownException.validationErrors().getFirst(),
            is("[service_settings] does not contain the required setting [model_id]")
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var entity = new GoogleAiStudioEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_MAX_INPUT_TOKENS,
            TEST_DIMENSIONS,
            TEST_SIMILARITY_MEASURE,
            new RateLimitSettings(TEST_RATE_LIMIT)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model_id":"%s",
                "max_input_tokens": %d,
                "dimensions": %d,
                "similarity": "%s",
                "rate_limit": {
                    "requests_per_minute": %d
                }
            }
            """, TEST_MODEL_ID, TEST_MAX_INPUT_TOKENS, TEST_DIMENSIONS, TEST_SIMILARITY_MEASURE.toString(), TEST_RATE_LIMIT)));
    }

    @Override
    protected Writeable.Reader<GoogleAiStudioEmbeddingsServiceSettings> instanceReader() {
        return GoogleAiStudioEmbeddingsServiceSettings::new;
    }

    @Override
    protected GoogleAiStudioEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected GoogleAiStudioEmbeddingsServiceSettings mutateInstance(GoogleAiStudioEmbeddingsServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var maxInputTokens = instance.maxInputTokens();
        var dimensions = instance.dimensions();
        var similarity = instance.similarity();
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(4)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(8));
            case 1 -> maxInputTokens = randomValueOtherThan(maxInputTokens, ESTestCase::randomNonNegativeIntOrNull);
            case 2 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 3 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(Utils.randomSimilarityMeasure(), null));
            case 4 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new GoogleAiStudioEmbeddingsServiceSettings(modelId, maxInputTokens, dimensions, similarity, rateLimitSettings);
    }

    private static HashMap<String, Object> buildServiceSettingsMap(
        @Nullable String modelId,
        @Nullable Integer maxInputTokens,
        @Nullable Integer dimensions,
        @Nullable String similarity,
        @Nullable Integer rateLimit
    ) {
        var result = new HashMap<String, Object>();
        if (modelId != null) {
            result.put(ServiceFields.MODEL_ID, modelId);
        }
        if (maxInputTokens != null) {
            result.put(ServiceFields.MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (dimensions != null) {
            result.put(ServiceFields.DIMENSIONS, dimensions);
        }
        if (similarity != null) {
            result.put(ServiceFields.SIMILARITY, similarity);
        }
        if (rateLimit != null) {
            result.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, rateLimit)));
        }
        return result;
    }
}
