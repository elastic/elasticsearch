/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.densetextembeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceDenseTextEmbeddingsServiceSettingsTests extends AbstractWireSerializingTestCase<
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
        return randomValueOtherThan(instance, ElasticInferenceServiceDenseTextEmbeddingsServiceSettingsTests::createRandom);
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

    public void testToXContent_WritesAllFields() throws IOException {
        var modelId = "my-dense-model";
        var similarity = SimilarityMeasure.DOT_PRODUCT;
        var dimensions = 1024;
        var maxInputTokens = 256;
        var rateLimitSettings = new RateLimitSettings(5000);

        var serviceSettings = new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
            modelId,
            similarity,
            dimensions,
            maxInputTokens,
            rateLimitSettings
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        String expectedResult = Strings.format(
            """
            {"similarity":"%s","dimensions":%d,"max_input_tokens":%d,"model_id":"%s","rate_limit":{"requests_per_minute":%d}}""",
            similarity,
            dimensions,
            maxInputTokens,
            modelId,
            rateLimitSettings.requestsPerTimeUnit()
        );

        assertThat(
            xContentResult,
            is(
                expectedResult
            )
        );
    }

    public void testToXContent_WritesOnlyNonNullFields() throws IOException {
        var modelId = "my-dense-model";
        var rateLimitSettings = new RateLimitSettings(2000);

        var serviceSettings = new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
            modelId,
            null, // similarity
            null, // dimensions
            null, // maxInputTokens
            rateLimitSettings
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":%d}}""", modelId, rateLimitSettings.requestsPerTimeUnit())));
    }

    public void testToXContentFragmentOfExposedFields() throws IOException {
        var modelId = "my-dense-model";
        var rateLimitSettings = new RateLimitSettings(1500);

        var serviceSettings = new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
            modelId,
            SimilarityMeasure.COSINE,
            512,
            128,
            rateLimitSettings
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        serviceSettings.toXContentFragmentOfExposedFields(builder, null);
        builder.endObject();
        String xContentResult = Strings.toString(builder);

        // Only model_id and rate_limit should be in exposed fields
        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","rate_limit":{"requests_per_minute":%d}}""", modelId, rateLimitSettings.requestsPerTimeUnit())));
    }

    public static ElasticInferenceServiceDenseTextEmbeddingsServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(10);
        var similarity = SimilarityMeasure.COSINE;
        var dimensions = randomBoolean() ? randomIntBetween(1, 1024) : null;
        var maxInputTokens = randomBoolean() ? randomIntBetween(128, 256) : null;
        var dimensionsSetByUser = randomBoolean();
        var rateLimitSettings = randomBoolean() ? new RateLimitSettings(randomIntBetween(1, 10000)) : null;

        return new ElasticInferenceServiceDenseTextEmbeddingsServiceSettings(
            modelId,
            similarity,
            dimensions,
            maxInputTokens,
            rateLimitSettings
        );
    }
}
