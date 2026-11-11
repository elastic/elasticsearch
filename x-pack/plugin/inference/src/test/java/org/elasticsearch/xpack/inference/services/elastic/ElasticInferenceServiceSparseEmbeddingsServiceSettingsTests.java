/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceSparseEmbeddingsServiceSettingsTests extends AbstractBWCSerializationTestCase<
    ElasticInferenceServiceSparseEmbeddingsServiceSettings> {

    @Override
    protected Writeable.Reader<ElasticInferenceServiceSparseEmbeddingsServiceSettings> instanceReader() {
        return ElasticInferenceServiceSparseEmbeddingsServiceSettings::new;
    }

    private final boolean ignoreUnknownFields = randomBoolean();

    @Override
    protected boolean supportsUnknownFields() {
        return ignoreUnknownFields;
    }

    @Override
    protected ElasticInferenceServiceSparseEmbeddingsServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return ElasticInferenceServiceSparseEmbeddingsServiceSettings.createParser(true, ConfigurationParseContext.PERSISTENT)
            .apply(parser, ConfigurationParseContext.PERSISTENT)
            .build();
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
        var map = new HashMap<String, Object>(Map.of(ServiceFields.MODEL_ID, modelId));

        var serviceSettings = ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST);

        assertThat(serviceSettings, is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, null, null)));
        assertThat(map, is(anEmptyMap()));
    }

    public void testFromMap_ThrowsIllegalArgumentException_WhenModelIdIsMissing() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(new HashMap<>(), ConfigurationParseContext.REQUEST)
        );

        assertThat(e.getMessage(), containsString("[service_settings] does not contain the required setting [model_id]"));
    }

    public void testFromMap_ThrowsOnInvalidMaxBatchSize() {
        final int invalidBatchSize = randomIntBetween(Integer.MIN_VALUE, 0);
        var map = new HashMap<String, Object>(
            Map.of(ServiceFields.MODEL_ID, "my-model-id", ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE, invalidBatchSize)
        );

        var e = expectThrows(
            XContentParseException.class,
            () -> ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(
            e.getCause().getMessage(),
            containsString("Invalid value [" + invalidBatchSize + "]. [max_batch_size] must be a positive integer")
        );
    }

    public void testFromMap_IgnoresRateLimitField_PersistentContext() {
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

        assertThat(serviceSettings, is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, null, null)));
        assertThat(serviceSettings.rateLimitSettings(), sameInstance(RateLimitSettings.DISABLED_INSTANCE));
    }

    public void testFromMap_DoesNotRemoveRateLimitField_DoesNotThrowValidationException_WhenRateLimitFieldDoesNotExist() {
        var modelId = "my-model-id";
        var map = new HashMap<String, Object>(Map.of(ServiceFields.MODEL_ID, modelId));
        var serviceSettings = ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.PERSISTENT);

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
            XContentParseException.class,
            () -> ElasticInferenceServiceSparseEmbeddingsServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST)
        );

        assertThat(exception.getCause(), instanceOf(ElasticsearchParseException.class));
        assertThat(
            exception.getCause().getMessage(),
            containsString(
                "[service_settings] rate limit settings are not permitted for service [elastic] and task type [sparse_embedding]"
            )
        );
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

        var map = new HashMap<String, Object>(Map.of("max_batch_size", newBatchSize));
        ServiceSettings updated = original.updateServiceSettings(map);

        assertThat(
            updated,
            is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(original.modelId(), original.maxInputTokens(), newBatchSize))
        );
        assertThat(map, is(anEmptyMap()));
    }

    public void testUpdateServiceSettings_GivenInvalidMaxBatchSize() {
        ElasticInferenceServiceSparseEmbeddingsServiceSettings original = createRandom();

        {
            var e = expectThrows(
                XContentParseException.class,
                () -> original.updateServiceSettings(new HashMap<>(Map.of("max_batch_size", 0)))
            );
            assertThat(e.getCause().getMessage(), containsString("Invalid value [0]. [max_batch_size] must be a positive integer"));
        }

        {
            final int newBatchSize = randomIntBetween(Integer.MIN_VALUE, 0);
            var e = expectThrows(
                XContentParseException.class,
                () -> original.updateServiceSettings(new HashMap<>(Map.of("max_batch_size", newBatchSize)))
            );
            assertThat(
                e.getCause().getMessage(),
                containsString("Invalid value [" + newBatchSize + "]. [max_batch_size] must be a positive integer")
            );
        }

        {
            final int newBatchSize = randomIntBetween(
                ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND + 1,
                Integer.MAX_VALUE
            );
            var e = expectThrows(
                XContentParseException.class,
                () -> original.updateServiceSettings(new HashMap<>(Map.of("max_batch_size", newBatchSize)))
            );
            assertThat(
                e.getCause().getMessage(),
                containsString(
                    "Invalid value ["
                        + Strings.format("%s", (double) newBatchSize)
                        + "]. [max_batch_size] must be less than or equal to ["
                        + (double) ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND
                        + "]"
                )
            );
        }
    }

    public void testUpdateServiceSettings_RejectsImmutableAndUnknownFields() {
        ElasticInferenceServiceSparseEmbeddingsServiceSettings original = createRandom();
        var immutableOrUnknownField = randomFrom(ServiceFields.MODEL_ID, ServiceFields.MAX_INPUT_TOKENS, "unknown_field");

        var e = expectThrows(
            XContentParseException.class,
            () -> original.updateServiceSettings(new HashMap<>(Map.of(immutableOrUnknownField, "some-value")))
        );

        assertThat(e.getMessage(), containsString("unknown field [" + immutableOrUnknownField + "]"));
    }

    public void testUpdateServiceSettings_KeepsExistingMaxBatchSize_WhenAbsent() {
        ElasticInferenceServiceSparseEmbeddingsServiceSettings original = createRandom();

        ServiceSettings updated = original.updateServiceSettings(new HashMap<>());

        assertThat(updated, is(original));
    }

    public void testUpdateServiceSettings_ClearsMaxBatchSize_WhenSetToNull() {
        ElasticInferenceServiceSparseEmbeddingsServiceSettings original = createRandom();
        var map = new HashMap<String, Object>();
        map.put("max_batch_size", null);

        ServiceSettings updated = original.updateServiceSettings(map);

        assertThat(
            updated,
            is(new ElasticInferenceServiceSparseEmbeddingsServiceSettings(original.modelId(), original.maxInputTokens(), null))
        );
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
