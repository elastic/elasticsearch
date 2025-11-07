/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserModels;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserModelsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elasticsearch.ElserModelsTests.randomElserModel;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceSparseEmbeddingsServiceSettingsTests extends AbstractBWCWireSerializationTestCase<
    ElasticInferenceServiceSparseEmbeddingsServiceSettings> {

    @Override
    protected Writeable.Reader<ElasticInferenceServiceSparseEmbeddingsServiceSettings> instanceReader() {
        return ElasticInferenceServiceSparseEmbeddingsServiceSettings::new;
    }

    @Override
    protected ElasticInferenceServiceSparseEmbeddingsServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElasticInferenceServiceSparseEmbeddingsServiceSettings mutateInstance(
        ElasticInferenceServiceSparseEmbeddingsServiceSettings instance
    ) throws IOException {
        if (randomBoolean()) {
            var modelId = randomValueOtherThan(instance.modelId(), ElserModelsTests::randomElserModel);
            return new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, instance.maxInputTokens());
        } else {
            var maxInputTokens = randomValueOtherThan(instance.maxInputTokens(), ESTestCase::randomNonNegativeIntOrNull);
            return new ElasticInferenceServiceSparseEmbeddingsServiceSettings(instance.modelId(), maxInputTokens);
        }
    }

    public void testFromMap() {
        var modelId = "my-model-id";

        var serviceSettings = ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, null)));
    }

    public void testFromMap_DoesNotRemoveRateLimitField_DoesNotThrowValidationException_PersistentContext() {
        var modelId = "my-model-id";
        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                modelId,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
            )
        );
        var serviceSettings = ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))));
        assertThat(serviceSettings, is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, null)));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_DoesNotRemoveRateLimitField_DoesNotThrowValidationException_WhenRateLimitFieldDoesNotExist() {
        var modelId = "my-model-id";
        var map = new HashMap<String, Object>(Map.of(ServiceFields.MODEL_ID, modelId));
        var serviceSettings = ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(map, anEmptyMap());
        assertThat(serviceSettings, is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, null)));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_DoesThrowValidationException_WhenRateLimitFieldDoesExist_RequestContext() {
        var modelId = "my-model-id";
        var map = new HashMap<String, Object>(
            Map.of(
                ServiceFields.MODEL_ID,
                modelId,
                RateLimitSettings.FIELD_NAME,
                new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))
            )
        );
        var exception = expectThrows(
            ValidationException.class,
            () -> ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            exception.getMessage(),
            containsString(
                "[service_settings] rate limit settings are not permitted for service [elastic] and task type [sparse_embedding]"
            )
        );
        assertThat(map, is(Map.of(RateLimitSettings.FIELD_NAME, Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, 100))));
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var modelId = ElserModels.ELSER_V1_MODEL;
        var maxInputTokens = 10;
        var serviceSettings = new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, maxInputTokens);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","max_input_tokens":%d}""", modelId, maxInputTokens)));
    }

    public static ElasticInferenceServiceSparseEmbeddingsServiceSettings createRandom() {
        return new ElasticInferenceServiceSparseEmbeddingsServiceSettings(randomElserModel(), randomNonNegativeInt());
    }

    @Override
    protected ElasticInferenceServiceSparseEmbeddingsServiceSettings mutateInstanceForVersion(
        ElasticInferenceServiceSparseEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        return instance;
    }
}
