/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.denseembeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceDenseEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    ElasticInferenceServiceDenseEmbeddingsServiceSettings> {
    private static final String TEST_MODEL_ID = "test-model-id";
    private static final String INITIAL_TEST_MODEL_ID = "initial-test-model-id";
    private static final int TEST_RATE_LIMIT = 20;
    private static final int TEST_DIMENSIONS = 1536;
    private static final int INITIAL_TEST_DIMENSIONS = 3072;
    private static final int TEST_MAX_INPUT_TOKENS = 512;
    private static final int INITIAL_TEST_MAX_INPUT_TOKENS = 1024;
    private static final SimilarityMeasure TEST_SIMILARITY_MEASURE = SimilarityMeasure.COSINE;
    private static final SimilarityMeasure INITIAL_TEST_SIMILARITY_MEASURE = SimilarityMeasure.DOT_PRODUCT;

    public void testUpdateServiceSettings_AllFields_Success() {
        HashMap<String, Object> settingsMap = createSettingsMap();

        var serviceSettings = createInitialServiceSettings().updateServiceSettings(settingsMap, TaskType.TEXT_EMBEDDING);

        MatcherAssert.assertThat(
            serviceSettings,
            is(
                new ElasticInferenceServiceDenseEmbeddingsServiceSettings(
                    TEST_MODEL_ID,
                    TEST_SIMILARITY_MEASURE,
                    TEST_DIMENSIONS,
                    TEST_MAX_INPUT_TOKENS
                )
            )
        );
    }

    private static HashMap<String, Object> createSettingsMap() {
        return new HashMap<>(
            Map.of(
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                ServiceFields.SIMILARITY,
                TEST_SIMILARITY_MEASURE.toString(),
                ServiceFields.DIMENSIONS,
                TEST_DIMENSIONS,
                ServiceFields.MAX_INPUT_TOKENS,
                TEST_MAX_INPUT_TOKENS
            )
        );
    }

    public void testUpdateServiceSettings_WithRateLimit_ThrowsException() {
        HashMap<String, Object> settingsMap = new HashMap<>(
            Map.of(
                ServiceFields.MODEL_ID,
                TEST_MODEL_ID,
                ServiceFields.SIMILARITY,
                TEST_SIMILARITY_MEASURE.toString(),
                ServiceFields.DIMENSIONS,
                TEST_DIMENSIONS,
                ServiceFields.MAX_INPUT_TOKENS,
                TEST_MAX_INPUT_TOKENS,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))
            )
        );

        var exception = expectThrows(
            ValidationException.class,
            () -> createInitialServiceSettings().updateServiceSettings(settingsMap, TaskType.TEXT_EMBEDDING)
        );

        assertThat(
            exception.getMessage(),
            containsString("[service_settings] rate limit settings are not permitted for service [elastic] and task type [text_embedding]")
        );
    }

    public void testUpdateServiceSettings_EmptyMap_Success() {
        var serviceSettings = createInitialServiceSettings().updateServiceSettings(new HashMap<>(), TaskType.TEXT_EMBEDDING);

        MatcherAssert.assertThat(serviceSettings, is(createInitialServiceSettings()));
    }

    private static ElasticInferenceServiceDenseEmbeddingsServiceSettings createInitialServiceSettings() {
        return new ElasticInferenceServiceDenseEmbeddingsServiceSettings(
            INITIAL_TEST_MODEL_ID,
            INITIAL_TEST_SIMILARITY_MEASURE,
            INITIAL_TEST_DIMENSIONS,
            INITIAL_TEST_MAX_INPUT_TOKENS
        );
    }

    @Override
    protected Writeable.Reader<ElasticInferenceServiceDenseEmbeddingsServiceSettings> instanceReader() {
        return ElasticInferenceServiceDenseEmbeddingsServiceSettings::new;
    }

    @Override
    protected ElasticInferenceServiceDenseEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElasticInferenceServiceDenseEmbeddingsServiceSettings mutateInstance(
        ElasticInferenceServiceDenseEmbeddingsServiceSettings instance
    ) throws IOException {
        var modelId = instance.modelId();
        var similarity = instance.similarity();
        var dimensions = instance.dimensions();
        var maxInputTokens = instance.maxInputTokens();
        switch (randomInt(3)) {
            case 0 -> modelId = randomValueOtherThan(modelId, () -> randomAlphaOfLength(10));
            case 1 -> similarity = randomValueOtherThan(similarity, Utils::randomSimilarityMeasure);
            case 2 -> dimensions = randomValueOtherThan(dimensions, () -> randomFrom(randomIntBetween(1, 1024), null));
            case 3 -> maxInputTokens = randomValueOtherThan(maxInputTokens, () -> randomFrom(randomIntBetween(128, 256), null));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new ElasticInferenceServiceDenseEmbeddingsServiceSettings(modelId, similarity, dimensions, maxInputTokens);
    }

    public void testFromMap_Request_WithAllSettings() {
        var serviceSettings = ElasticInferenceServiceDenseEmbeddingsServiceSettings.fromMap(
            createSettingsMap(),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings.modelId(), is(TEST_MODEL_ID));
        assertThat(serviceSettings.similarity(), is(TEST_SIMILARITY_MEASURE));
        assertThat(serviceSettings.dimensions(), is(TEST_DIMENSIONS));
        assertThat(serviceSettings.maxInputTokens(), is(TEST_MAX_INPUT_TOKENS));
    }

    public void testFromMap_WithAllSettings_DoesNotRemoveRateLimitField_DoesNotThrowValidationException_PersistentContext() {
        var map = createSettingsMap();
        map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)));
        var serviceSettings = ElasticInferenceServiceDenseEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))));
        assertThat(serviceSettings.modelId(), is(TEST_MODEL_ID));
        assertThat(serviceSettings.similarity(), is(TEST_SIMILARITY_MEASURE));
        assertThat(serviceSettings.dimensions(), is(TEST_DIMENSIONS));
        assertThat(serviceSettings.maxInputTokens(), is(TEST_MAX_INPUT_TOKENS));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_WithAllSettings_DoesNotRemoveRateLimitField_ThrowsValidationException_RequestContext() {
        var map = createSettingsMap();
        map.put(RateLimitSettings.FIELD_NAME, new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT)));
        var exception = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceDenseEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, TEST_RATE_LIMIT))));
        assertThat(
            exception.getMessage(),
            containsString("[service_settings] rate limit settings are not permitted for service [elastic] and task type [text_embedding]")
        );
    }

    public void testFromMap_WithAllSettings_DoesNotThrowValidationException_WhenRateLimitFieldDoesNotExist_RequestContext() {
        var map = createSettingsMap();
        var serviceSettings = ElasticInferenceServiceDenseEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(map, anEmptyMap());
        assertThat(serviceSettings.modelId(), is(TEST_MODEL_ID));
        assertThat(serviceSettings.similarity(), is(TEST_SIMILARITY_MEASURE));
        assertThat(serviceSettings.dimensions(), is(TEST_DIMENSIONS));
        assertThat(serviceSettings.maxInputTokens(), is(TEST_MAX_INPUT_TOKENS));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var serviceSettings = new ElasticInferenceServiceDenseEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        String expectedResult = Strings.format(
            """
                {"model_id":"%s","similarity":"%s","dimensions":%d,"max_input_tokens":%d}""",
            TEST_MODEL_ID,
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS
        );

        assertThat(xContentResult, is(expectedResult));
    }

    public void testToXContent_WritesOnlyNonNullFields() throws IOException {
        var serviceSettings = new ElasticInferenceServiceDenseEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            null, // similarity
            null, // dimensions
            null // maxInputTokens
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s"}""", TEST_MODEL_ID)));
    }

    public void testToXContentFragmentOfExposedFields() throws IOException {
        var serviceSettings = new ElasticInferenceServiceDenseEmbeddingsServiceSettings(
            TEST_MODEL_ID,
            TEST_SIMILARITY_MEASURE,
            TEST_DIMENSIONS,
            TEST_MAX_INPUT_TOKENS
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        serviceSettings.toXContentFragmentOfExposedFields(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                XContentHelper.stripWhitespace(
                    Strings.format(
                        """
                            {"model_id":"%s","similarity":"%s","dimensions":%d,"max_input_tokens":%d}""",
                        TEST_MODEL_ID,
                        TEST_SIMILARITY_MEASURE,
                        TEST_DIMENSIONS,
                        TEST_MAX_INPUT_TOKENS
                    )
                )
            )
        );
    }

    public static ElasticInferenceServiceDenseEmbeddingsServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(10);
        var similarity = SimilarityMeasure.COSINE;
        var dimensions = randomBoolean() ? randomIntBetween(1, 1024) : null;
        var maxInputTokens = randomBoolean() ? randomIntBetween(128, 256) : null;

        return new ElasticInferenceServiceDenseEmbeddingsServiceSettings(modelId, similarity, dimensions, maxInputTokens);
    }

    @Override
    protected ElasticInferenceServiceDenseEmbeddingsServiceSettings mutateInstanceForVersion(
        ElasticInferenceServiceDenseEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
