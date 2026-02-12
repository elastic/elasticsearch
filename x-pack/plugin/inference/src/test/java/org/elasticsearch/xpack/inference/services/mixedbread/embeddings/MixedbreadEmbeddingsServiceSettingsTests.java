/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.mixedbread.TestUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.settings.RateLimitSettings.REQUESTS_PER_MINUTE_FIELD;

public class MixedbreadEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<MixedbreadEmbeddingsServiceSettings> {
    private static final RateLimitSettings RATE_LIMIT = new RateLimitSettings(2);
    private static final int MAX_INPUT_TOKENS = 3;

    public static MixedbreadEmbeddingsServiceSettings createRandom() {
        return createRandom(randomFrom(new RateLimitSettings[] { null, RateLimitSettingsTests.createRandom() }));
    }

    public static MixedbreadEmbeddingsServiceSettings createRandom(@Nullable RateLimitSettings rateLimitSettings) {
        return new MixedbreadEmbeddingsServiceSettings(
            randomAlphaOfLength(10),
            randomAlphaOfLengthOrNull(10),
            randomInt(10),
            randomAlphaOfLengthOrNull(10),
            randomFrom(SimilarityMeasure.values()),
            randomInt(10),
            rateLimitSettings
        );
    }

    public void testToXContent_WritesAllValues() throws IOException {
        var serviceSettings = new MixedbreadEmbeddingsServiceSettings(
            TestUtils.MODEL_ID,
            TestUtils.CUSTOM_URL,
            TestUtils.DIMENSIONS,
            TestUtils.ENCODING_VALUE,
            SimilarityMeasure.COSINE,
            MAX_INPUT_TOKENS,
            RATE_LIMIT
        );
        assertThat(getXContentResult(serviceSettings), equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model_id_value",
                "url": "https://custom.url.com/v1/task",
                "rate_limit": {
                    "requests_per_minute": 2
                },
                "dimensions": 3,
                "encoding_format": "float",
                "similarity": "cosine",
                "max_input_tokens": 3
            }
            """));
    }

    public void testToXContent_DoesNotWriteOptionalValues_DefaultUri_DefaultRateLimit() throws IOException {
        var serviceSettings = new MixedbreadEmbeddingsServiceSettings(TestUtils.MODEL_ID, (URI) null, null, null, null, null, null);
        assertThat(getXContentResult(serviceSettings), equalToIgnoringWhitespaceInJsonString("""
            {
                "model_id":"model_id_value",
                "url": "https://api.mixedbread.com/v1/embeddings",
                "rate_limit": {
                    "requests_per_minute": 100
                }
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
        var rateLimitSettings = instance.rateLimitSettings();
        switch (randomInt(1)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(10));
            case 1 -> rateLimitSettings = randomValueOtherThan(rateLimitSettings, RateLimitSettingsTests::createRandom);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new MixedbreadEmbeddingsServiceSettings(
            modelId,
            TestUtils.CUSTOM_URL,
            TestUtils.DIMENSIONS,
            TestUtils.ENCODING_VALUE,
            SimilarityMeasure.COSINE,
            MAX_INPUT_TOKENS,
            rateLimitSettings
        );
    }

    public static Map<String, Object> getServiceSettingsMap(String modelId) {
        return getServiceSettingsMap(modelId, null);
    }

    public static Map<String, Object> getServiceSettingsMap(String modelId, @Nullable Integer requestsPerMinute) {
        var map = new HashMap<String, Object>();

        map.put(ServiceFields.MODEL_ID, modelId);

        if (requestsPerMinute != null) {
            map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(REQUESTS_PER_MINUTE_FIELD, requestsPerMinute)));
        }

        return map;
    }

    public static HashMap<String, Object> getServiceSettingsMap(
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
