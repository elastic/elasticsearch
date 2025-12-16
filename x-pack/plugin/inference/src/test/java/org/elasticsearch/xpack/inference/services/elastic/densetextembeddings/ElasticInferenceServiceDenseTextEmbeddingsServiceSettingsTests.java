/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.densetextembeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceDenseTextEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    ElasticInferenceServiceDenseTextEmbeddingsServiceSettings> {

    @Override
    protected Writeable.Reader<ElasticInferenceServiceDenseTextEmbeddingsServiceSettings> instanceReader() {
        return ElasticInferenceServiceDenseTextEmbeddingsServiceSettings::new;
    }

    @Override
    protected ElasticInferenceServiceDenseTextEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElasticInferenceServiceDenseTextEmbeddingsServiceSettings mutateInstance(
        ElasticInferenceServiceDenseTextEmbeddingsServiceSettings instance
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

        return new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(modelId, similarity, dimensions, maxInputTokens);
    }

    public void testFromMap_Request_WithAllSettings() {
        var modelId = "my-dense-model-id";
        var similarity = SimilarityMeasure.COSINE;
        var dimensions = 384;
        var maxInputTokens = 512;

        var serviceSettings = ElasticInferenceServiceDenseTextEmbeddingsServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ServiceFields.MODEL_ID,
                    modelId,
                    ServiceFields.SIMILARITY,
                    similarity.toString(),
                    ServiceFields.DIMENSIONS,
                    dimensions,
                    ServiceFields.MAX_INPUT_TOKENS,
                    maxInputTokens
                )
            ),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings.modelId(), is(modelId));
        assertThat(serviceSettings.similarity(), is(similarity));
        assertThat(serviceSettings.dimensions(), is(dimensions));
        assertThat(serviceSettings.maxInputTokens(), is(maxInputTokens));
    }

    public void testFromMap_WithAllSettings_DoesNotRemoveRateLimitField_DoesNotThrowValidationException_PersistentContext() {
        var modelId = "my-dense-model-id";
        var similarity = SimilarityMeasure.COSINE;
        var dimensions = 384;
        var maxInputTokens = 512;

        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                modelId,
                ServiceFields.SIMILARITY,
                similarity.toString(),
                ServiceFields.DIMENSIONS,
                dimensions,
                ServiceFields.MAX_INPUT_TOKENS,
                maxInputTokens,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
            )
        );
        var serviceSettings = ElasticInferenceServiceDenseTextEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))));
        assertThat(serviceSettings.modelId(), is(modelId));
        assertThat(serviceSettings.similarity(), is(similarity));
        assertThat(serviceSettings.dimensions(), is(dimensions));
        assertThat(serviceSettings.maxInputTokens(), is(maxInputTokens));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_WithAllSettings_DoesNotRemoveRateLimitField_ThrowsValidationException_RequestContext() {
        var modelId = "my-dense-model-id";
        var similarity = SimilarityMeasure.COSINE;
        var dimensions = 384;
        var maxInputTokens = 512;

        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                modelId,
                ServiceFields.SIMILARITY,
                similarity.toString(),
                ServiceFields.DIMENSIONS,
                dimensions,
                ServiceFields.MAX_INPUT_TOKENS,
                maxInputTokens,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
            )
        );
        var exception = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceDenseTextEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))));
        assertThat(
            exception.getMessage(),
            containsString("[service_settings] rate limit settings are not permitted for service [elastic] and task type [text_embedding]")
        );
    }

    public void testFromMap_WithAllSettings_DoesNotThrowValidationException_WhenRateLimitFieldDoesNotExist_RequestContext() {
        var modelId = "my-dense-model-id";
        var similarity = SimilarityMeasure.COSINE;
        var dimensions = 384;
        var maxInputTokens = 512;

        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                modelId,
                ServiceFields.SIMILARITY,
                similarity.toString(),
                ServiceFields.DIMENSIONS,
                dimensions,
                ServiceFields.MAX_INPUT_TOKENS,
                maxInputTokens
            )
        );
        var serviceSettings = ElasticInferenceServiceDenseTextEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(map, anEmptyMap());
        assertThat(serviceSettings.modelId(), is(modelId));
        assertThat(serviceSettings.similarity(), is(similarity));
        assertThat(serviceSettings.dimensions(), is(dimensions));
        assertThat(serviceSettings.maxInputTokens(), is(maxInputTokens));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var modelId = "my-dense-model";
        var similarity = SimilarityMeasure.DOT_PRODUCT;
        var dimensions = 1024;
        var maxInputTokens = 256;

        var serviceSettings = new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
            modelId,
            similarity,
            dimensions,
            maxInputTokens
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        String expectedResult = Strings.format("""
            {"similarity":"%s","dimensions":%d,"max_input_tokens":%d,"model_id":"%s"}""", similarity, dimensions, maxInputTokens, modelId);

        assertThat(xContentResult, is(expectedResult));
    }

    public void testToXContent_WritesOnlyNonNullFields() throws IOException {
        var modelId = "my-dense-model";

        var serviceSettings = new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
            modelId,
            null, // similarity
            null, // dimensions
            null // maxInputTokens
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s"}""", modelId)));
    }

    public void testToXContentFragmentOfExposedFields() throws IOException {
        var modelId = "my-dense-model";

        var serviceSettings = new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(modelId, SimilarityMeasure.COSINE, 512, 128);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        serviceSettings.toXContentFragmentOfExposedFields(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        // Only model_id and rate_limit should be in exposed fields
        assertThat(xContentResult, is(XContentHelper.stripWhitespace(Strings.format("""
            {"model_id":"%s"}""", modelId))));
    }

    public static ElasticInferenceServiceDenseTextEmbeddingsServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(10);
        var similarity = SimilarityMeasure.COSINE;
        var dimensions = randomBoolean() ? randomIntBetween(1, 1024) : null;
        var maxInputTokens = randomBoolean() ? randomIntBetween(128, 256) : null;

        return new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(modelId, similarity, dimensions, maxInputTokens);
    }

    @Override
    protected ElasticInferenceServiceDenseTextEmbeddingsServiceSettings mutateInstanceForVersion(
        ElasticInferenceServiceDenseTextEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
