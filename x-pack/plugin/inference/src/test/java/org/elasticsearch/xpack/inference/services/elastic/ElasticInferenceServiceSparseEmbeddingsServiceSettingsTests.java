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
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserModels;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserModelsTests;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elasticsearch.ElserModelsTests.randomElserModel;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

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
        var rateLimitSettings = instance.rateLimitSettings();
        var maxBatchSize = instance.maxBatchSize();

        switch (randomIntBetween(0, 3)) {
            case 0 -> modelId = randomValueOtherThan(instance.modelId(), ElserModelsTests::randomElserModel);
            case 1 -> maxInputTokens = randomValueOtherThan(instance.maxInputTokens(), ESTestCase::randomNonNegativeIntOrNull);
            case 2 -> rateLimitSettings = randomValueOtherThan(instance.rateLimitSettings(), RateLimitSettingsTests::createRandom);
            case 3 -> maxBatchSize = randomValueOtherThan(
                instance.maxBatchSize(),
                () -> randomIntBetween(1, ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, maxInputTokens, rateLimitSettings, maxBatchSize);
    }

    public void testFromMap() {
        var modelId = "my-model-id";

        var serviceSettings = ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(
            new HashMap<>(Map.of(ServiceFields.MODEL_ID, modelId)),
            ConfigurationParseContext.REQUEST
        );

        assertThat(serviceSettings, is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, null, null, null)));
    }

    public void testToXContent_WritesAllFields() throws IOException {
        var modelId = ElserModels.ELSER_V1_MODEL;
        var maxInputTokens = 10;
        var maxBatchSize = randomIntBetween(1, ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND);
        var serviceSettings = new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, maxInputTokens, null, maxBatchSize);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        serviceSettings.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(
            xContentResult,
            is(
                Strings.format(
                    """
                        {"model_id":"%s","max_input_tokens":%d,"max_batch_size":%d,"rate_limit":{"requests_per_minute":1000}}""",
                    modelId,
                    maxInputTokens,
                    maxBatchSize
                )
            )
        );
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
            is(
                new ElasticInferenceServiceSparseEmbeddingsServiceSettings(
                    original.modelId(),
                    original.maxInputTokens(),
                    original.rateLimitSettings(),
                    newBatchSize
                )
            )
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
            randomNonNegativeInt(),
            RateLimitSettingsTests.createRandom(),
            randomIntBetween(1, ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND)
        );
    }

    @Override
    protected ElasticInferenceServiceSparseEmbeddingsServiceSettings mutateInstanceForVersion(
        ElasticInferenceServiceSparseEmbeddingsServiceSettings instance,
        TransportVersion version
    ) {
        if (version.supports(ElasticInferenceServiceSettingsUtils.INFERENCE_API_EIS_MAX_BATCH_SIZE) == false) {
            return new ElasticInferenceServiceSparseEmbeddingsServiceSettings(
                instance.modelId(),
                instance.maxInputTokens(),
                instance.rateLimitSettings(),
                null
            );
        }
        return instance;
    }
}
