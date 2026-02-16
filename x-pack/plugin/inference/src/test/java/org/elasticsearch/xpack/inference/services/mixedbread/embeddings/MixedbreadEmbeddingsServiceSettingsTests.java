/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.mixedbread.TestUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.Utils.randomSimilarityMeasure;

public class MixedbreadEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<MixedbreadEmbeddingsServiceSettings> {
    private static final RateLimitSettings RATE_LIMIT = new RateLimitSettings(2);
    private static final int MAX_INPUT_TOKENS = 3;

    public static MixedbreadEmbeddingsServiceSettings createRandom() {
        return new MixedbreadEmbeddingsServiceSettings(
            randomAlphaOfLength(10),
            randomNonNegativeIntOrNull(),
            randomAlphaOfLengthOrNull(10),
            randomFrom(randomSimilarityMeasure(), null),
            randomFrom(randomIntBetween(128, 256), null),
            randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() }),
            randomBoolean()
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new MixedbreadEmbeddingsServiceSettings(
            TestUtils.MODEL_ID,
            TestUtils.DIMENSIONS,
            TestUtils.ENCODING_VALUE,
            SimilarityMeasure.COSINE,
            MAX_INPUT_TOKENS,
            RATE_LIMIT,
            TestUtils.DIMENSIONS_SET_BY_USER_TRUE
        );
        assertThat(getXContentResult(serviceSettings), equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model_id_value",
                "rate_limit": {
                    "requests_per_minute": 2
                },
                "dimensions": 3,
                "encoding_format": "float",
                "similarity": "cosine",
                "max_input_tokens": 3,
                "dimensions_set_by_user": true
            }
            """));
    }

    public void testToXContent_DoesNotWriteOptionalValues_DefaultRateLimit() throws IOException {
        var serviceSettings = new MixedbreadEmbeddingsServiceSettings(
            TestUtils.MODEL_ID,
            null,
            null,
            null,
            null,
            null,
            TestUtils.DIMENSIONS_SET_BY_USER_TRUE
        );
        assertThat(getXContentResult(serviceSettings), equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model_id_value",
                "rate_limit": {
                    "requests_per_minute": 100
                },
                "dimensions_set_by_user": true
            }
            """));
    }

    private String getXContentResult(MixedbreadEmbeddingsServiceSettings serviceSettings) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        return Strings.toString(builder);
    }

    @Override
    protected Writeable.Reader<MixedbreadEmbeddingsServiceSettings> instanceReader() {
        return MixedbreadEmbeddingsServiceSettings::new;
    }

    @Override
    protected MixedbreadEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected MixedbreadEmbeddingsServiceSettings mutateInstance(MixedbreadEmbeddingsServiceSettings instance) throws IOException {
        var modelId = instance.modelId();
        var dimensions = instance.dimensions();
        var encodingFormat = instance.encodingFormat();
        var maxInputTokens = instance.maxInputTokens();
        var similarity = instance.similarity();
        var rateLimitSettings = instance.rateLimitSettings();
        var dimensionsSetByUser = instance.dimensionsSetByUser();
        switch (randomInt(5)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(10));
            case 1 -> dimensions = randomValueOtherThan(dimensions, ESTestCase::randomNonNegativeIntOrNull);
            case 2 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            case 3 -> similarity = randomValueOtherThan(similarity, () -> randomFrom(randomSimilarityMeasure(), null));
            case 4 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            case 5 -> dimensionsSetByUser = dimensionsSetByUser == false;
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new MixedbreadEmbeddingsServiceSettings(
            modelId,
            TestUtils.DIMENSIONS,
            TestUtils.ENCODING_VALUE,
            SimilarityMeasure.COSINE,
            MAX_INPUT_TOKENS,
            rateLimitSettings,
            TestUtils.DIMENSIONS_SET_BY_USER_TRUE
        );
    }

    public static HashMap<String, Object> getServiceSettingsMap(
        @Nullable String modelId,
        @Nullable String url,
        @Nullable String similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable HashMap<String, Integer> rateLimitSettings,
        @Nullable Boolean dimensionsSetByUser
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
        if (dimensionsSetByUser != null) {
            result.put(ServiceFields.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        return result;
    }

    @Override
    protected MixedbreadEmbeddingsServiceSettings mutateInstanceForVersion(
        MixedbreadEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
