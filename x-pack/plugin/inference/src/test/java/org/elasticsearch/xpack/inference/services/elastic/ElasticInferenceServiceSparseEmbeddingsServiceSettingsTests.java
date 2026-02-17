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
import org.elasticsearch.inference.ServiceSettings;
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

import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsUtils.INFERENCE_API_EIS_MAX_BATCH_SIZE;
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
        var modelId = instance.modelId();
        var maxInputTokens = instance.maxInputTokens();
        var maxBatchSize = instance.maxBatchSize();

        switch (randomIntBetween(0, 2)) {
            case 0 -> modelId = randomValueOtherThan(instance.modelId(), ElserModelsTests::randomElserModel);
            case 1 -> maxInputTokens = randomValueOtherThan(instance.maxInputTokens(), ESTestCase::randomNonNegativeIntOrNull);
            case 2 -> maxBatchSize = randomValueOtherThan(
                instance.maxBatchSize(),
                () -> randomIntBetween(1, ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, maxInputTokens, maxBatchSize);
    }

    public void testFromMap() {
        var modelId = "my-model-id";

        var serviceSettings = ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, null, null)));
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
        assertThat(serviceSettings, is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, null, null)));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_DoesNotRemoveRateLimitField_DoesNotThrowValidationException_WhenRateLimitFieldDoesNotExist() {
        var modelId = "my-model-id";
        var map = new HashMap<String, Object>(Map.of(ServiceFields.MODEL_ID, modelId));
        var serviceSettings = ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

        assertThat(map, anEmptyMap());
        assertThat(serviceSettings, is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, null, null)));
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
        var maxInputTokens = randomNonNegativeInt();
        var maxBatchSize = randomIntBetween(1, ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND);
        var serviceSettings = new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, maxInputTokens, maxBatchSize);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(Strings.format("""
            {"model_id":"%s","max_input_tokens":%d,"max_batch_size":%d}""", modelId, maxInputTokens, maxBatchSize)));
    }

    public void testUpdateServiceSettings_GivenValidMaxBatchSize() {
        ElasticInferenceServiceSparseEmbeddingsServiceSettings original = createRandom();
        int newBatchSize = randomValueOtherThan(
            original.maxBatchSize(),
            () -> randomIntBetween(1, ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND)
        );

        ServiceSettings updated = original.updateServiceSettings(Map.of("max_batch_size", newBatchSize));

        assertThat(
            updated,
            is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(original.modelId(), original.maxInputTokens(), newBatchSize))
        );
    }

    public void testUpdateServiceSettings_GivenInvalidMaxBatchSize() {
        ElasticInferenceServiceSparseEmbeddingsServiceSettings original = createRandom();

        {
            ValidationException e = expectThrows(
                ValidationException.class,
                () -> original.updateServiceSettings(Map.of("max_batch_size", 0))
            );
            assertThat(e.getMessage(), containsString("Invalid value [0]. [max_batch_size] must be a positive integer;"));
        }

        {
            final int newBatchSize = randomIntBetween(Integer.MIN_VALUE, 0);
            ValidationException e = expectThrows(
                ValidationException.class,
                () -> original.updateServiceSettings(Map.of("max_batch_size", newBatchSize))
            );
            assertThat(
                e.getMessage(),
                containsString("Invalid value [" + newBatchSize + "]. [max_batch_size] must be a positive integer;")
            );
        }

        {
            final int newBatchSize = randomIntBetween(
                ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND + 1,
                Integer.MAX_VALUE
            );
            ValidationException e = expectThrows(
                ValidationException.class,
                () -> original.updateServiceSettings(Map.of("max_batch_size", newBatchSize))
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "Invalid value ["
                        + Strings.format("%s", (double) newBatchSize)
                        + "]. [max_batch_size] must be less than or equal to ["
                        + (double) ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND
                        + "];"
                )
            );
        }
    }

    public static ElasticInferenceServiceSparseEmbeddingsServiceSettings createRandom() {
        return new ElasticInferenceServiceSparseEmbeddingsServiceSettings(
            randomElserModel(),
            randomNonNegativeIntOrNull(),
            randomIntBetween(1, ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND)
        );
    }

    @Override
    protected ElasticInferenceServiceSparseEmbeddingsServiceSettings mutateInstanceForVersion(
        ElasticInferenceServiceSparseEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        if (version.supports(INFERENCE_API_EIS_MAX_BATCH_SIZE) == false) {
            return new ElasticInferenceServiceSparseEmbeddingsServiceSettings(instance.modelId(), instance.maxInputTokens(), null);
        }
        return instance;
    }
}
