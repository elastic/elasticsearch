/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.model;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemas;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemasTests;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredServiceSchema;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SageMakerServiceSettingsTests extends InferenceSettingsTestCase<SageMakerServiceSettings> {

    private static final String TEST_ENDPOINT_NAME = "test-endpoint";
    private static final String INITIAL_TEST_ENDPOINT_NAME = "initial-endpoint";

    private static final String TEST_REGION = "us-east-1";
    private static final String INITIAL_TEST_REGION = "eu-west-1";

    private static final String TEST_API = "test-api";
    private static final String INITIAL_TEST_API = "initial-api";

    private static final String TEST_TARGET_MODEL = "test-target-model";
    private static final String INITIAL_TEST_TARGET_MODEL = "initial-target-model";

    private static final String TEST_TARGET_CONTAINER_HOSTNAME = "test-container-hostname";
    private static final String INITIAL_TEST_TARGET_CONTAINER_HOSTNAME = "initial-container-hostname";

    private static final String TEST_INFERENCE_COMPONENT_NAME = "test-inference-component";
    private static final String INITIAL_TEST_INFERENCE_COMPONENT_NAME = "initial-inference-component";

    private static final int TEST_BATCH_SIZE = 64;
    private static final int INITIAL_TEST_BATCH_SIZE = 16;

    private static final String TEST_ELASTIC_API = "elastic";
    private static final String TEST_OPENAI_API = "openai";
    private static final String ELEMENT_TYPE_FIELD = "element_type";

    private static final int TEST_DIMENSIONS = 384;
    private static final SimilarityMeasure TEST_SIMILARITY = SimilarityMeasure.COSINE;
    private static final DenseVectorFieldMapper.ElementType TEST_ELEMENT_TYPE = DenseVectorFieldMapper.ElementType.FLOAT;

    private static final SageMakerSchemas SCHEMAS = new SageMakerSchemas();

    @Override
    protected Writeable.Reader<SageMakerServiceSettings> instanceReader() {
        return SageMakerServiceSettings::new;
    }

    @Override
    protected SageMakerServiceSettings createTestInstance() {
        return new SageMakerServiceSettings(
            randomString(),
            randomString(),
            randomString(),
            randomOptionalString(),
            randomOptionalString(),
            randomOptionalString(),
            randomBoolean() ? randomIntBetween(1, 1000) : null,
            SageMakerStoredServiceSchema.NO_OP
        );
    }

    @Override
    protected SageMakerServiceSettings fromMutableMap(Map<String, Object> mutableMap) {
        return SageMakerServiceSettings.fromMap(SageMakerSchemasTests.mockSchemas(), randomFrom(TaskType.values()), mutableMap);
    }

    public void testFromMap_ElasticTextEmbedding_AllFields_CreatesSettingsCorrectly() {
        var settingsMap = buildElasticTextEmbeddingServiceSettingsMap(
            TEST_ENDPOINT_NAME,
            TEST_REGION,
            TEST_TARGET_MODEL,
            TEST_TARGET_CONTAINER_HOSTNAME,
            TEST_INFERENCE_COMPONENT_NAME,
            TEST_BATCH_SIZE,
            TEST_DIMENSIONS,
            true,
            TEST_SIMILARITY,
            TEST_ELEMENT_TYPE
        );

        var serviceSettings = SageMakerServiceSettings.fromMap(SCHEMAS, TaskType.TEXT_EMBEDDING, settingsMap);

        assertThat(serviceSettings.endpointName(), is(TEST_ENDPOINT_NAME));
        assertThat(serviceSettings.region(), is(TEST_REGION));
        assertThat(serviceSettings.api(), is(TEST_ELASTIC_API));
        assertThat(serviceSettings.targetModel(), is(TEST_TARGET_MODEL));
        assertThat(serviceSettings.targetContainerHostname(), is(TEST_TARGET_CONTAINER_HOSTNAME));
        assertThat(serviceSettings.inferenceComponentName(), is(TEST_INFERENCE_COMPONENT_NAME));
        assertThat(serviceSettings.batchSize(), is(TEST_BATCH_SIZE));

        var apiServiceSettings = serviceSettings.apiServiceSettings();
        assertThat(apiServiceSettings.dimensions(), is(TEST_DIMENSIONS));
        assertThat(apiServiceSettings.dimensionsSetByUser(), is(true));
        assertThat(apiServiceSettings.similarity(), is(TEST_SIMILARITY));
        assertThat(apiServiceSettings.elementType(), is(TEST_ELEMENT_TYPE));
    }

    public void testFromMap_ElasticTextEmbedding_OnlyMandatoryFields_DefaultsAreUsed() {
        var settingsMap = buildElasticTextEmbeddingServiceSettingsMap(
            TEST_ENDPOINT_NAME,
            TEST_REGION,
            null,
            null,
            null,
            null,
            null,
            null,
            TEST_SIMILARITY,
            TEST_ELEMENT_TYPE
        );

        var serviceSettings = SageMakerServiceSettings.fromMap(SCHEMAS, TaskType.TEXT_EMBEDDING, settingsMap);

        assertThat(serviceSettings.endpointName(), is(TEST_ENDPOINT_NAME));
        assertThat(serviceSettings.region(), is(TEST_REGION));
        assertThat(serviceSettings.api(), is(TEST_ELASTIC_API));
        assertThat(serviceSettings.targetModel(), is(nullValue()));
        assertThat(serviceSettings.targetContainerHostname(), is(nullValue()));
        assertThat(serviceSettings.inferenceComponentName(), is(nullValue()));
        assertThat(serviceSettings.batchSize(), is(nullValue()));

        var apiServiceSettings = serviceSettings.apiServiceSettings();
        assertThat(apiServiceSettings.dimensions(), is(nullValue()));
        assertThat(apiServiceSettings.dimensionsSetByUser(), is(false));
        assertThat(apiServiceSettings.similarity(), is(TEST_SIMILARITY));
        assertThat(apiServiceSettings.elementType(), is(TEST_ELEMENT_TYPE));
    }

    public void testFromMap_OpenAiTextEmbedding_AllFields_CreatesSettingsCorrectly() {
        var settingsMap = buildOpenAiTextEmbeddingServiceSettingsMap(
            TEST_ENDPOINT_NAME,
            TEST_REGION,
            TEST_TARGET_MODEL,
            TEST_TARGET_CONTAINER_HOSTNAME,
            TEST_INFERENCE_COMPONENT_NAME,
            TEST_BATCH_SIZE,
            TEST_DIMENSIONS
        );

        var serviceSettings = SageMakerServiceSettings.fromMap(SCHEMAS, TaskType.TEXT_EMBEDDING, settingsMap);

        assertThat(serviceSettings.endpointName(), is(TEST_ENDPOINT_NAME));
        assertThat(serviceSettings.region(), is(TEST_REGION));
        assertThat(serviceSettings.api(), is(TEST_OPENAI_API));
        assertThat(serviceSettings.targetModel(), is(TEST_TARGET_MODEL));
        assertThat(serviceSettings.targetContainerHostname(), is(TEST_TARGET_CONTAINER_HOSTNAME));
        assertThat(serviceSettings.inferenceComponentName(), is(TEST_INFERENCE_COMPONENT_NAME));
        assertThat(serviceSettings.batchSize(), is(TEST_BATCH_SIZE));

        var apiServiceSettings = serviceSettings.apiServiceSettings();
        assertThat(apiServiceSettings.dimensions(), is(TEST_DIMENSIONS));
        assertThat(apiServiceSettings.dimensionsSetByUser(), is(true));
        assertThat(apiServiceSettings.similarity(), is(SimilarityMeasure.DOT_PRODUCT));
        assertThat(apiServiceSettings.elementType(), is(DenseVectorFieldMapper.ElementType.FLOAT));
    }

    public void testFromMap_OpenAiTextEmbedding_OnlyMandatoryFields_DefaultsAreUsed() {
        var settingsMap = buildOpenAiTextEmbeddingServiceSettingsMap(TEST_ENDPOINT_NAME, TEST_REGION, null, null, null, null, null);

        var serviceSettings = SageMakerServiceSettings.fromMap(SCHEMAS, TaskType.TEXT_EMBEDDING, settingsMap);

        assertThat(serviceSettings.endpointName(), is(TEST_ENDPOINT_NAME));
        assertThat(serviceSettings.region(), is(TEST_REGION));
        assertThat(serviceSettings.api(), is(TEST_OPENAI_API));
        assertThat(serviceSettings.targetModel(), is(nullValue()));
        assertThat(serviceSettings.targetContainerHostname(), is(nullValue()));
        assertThat(serviceSettings.inferenceComponentName(), is(nullValue()));
        assertThat(serviceSettings.batchSize(), is(nullValue()));

        var apiServiceSettings = serviceSettings.apiServiceSettings();
        assertThat(apiServiceSettings.dimensions(), is(nullValue()));
        assertThat(apiServiceSettings.dimensionsSetByUser(), is(false));
        assertThat(apiServiceSettings.similarity(), is(SimilarityMeasure.DOT_PRODUCT));
        assertThat(apiServiceSettings.elementType(), is(DenseVectorFieldMapper.ElementType.FLOAT));
    }

    public void testUpdateServiceSettings_AllFields_OnlyMutableFieldsAreUpdated() {
        var settingsMap = buildServiceSettingsMap(
            TEST_ENDPOINT_NAME,
            TEST_REGION,
            TEST_API,
            TEST_TARGET_MODEL,
            TEST_TARGET_CONTAINER_HOSTNAME,
            TEST_INFERENCE_COMPONENT_NAME,
            TEST_BATCH_SIZE
        );
        var originalServiceSettings = new SageMakerServiceSettings(
            INITIAL_TEST_ENDPOINT_NAME,
            INITIAL_TEST_REGION,
            INITIAL_TEST_API,
            INITIAL_TEST_TARGET_MODEL,
            INITIAL_TEST_TARGET_CONTAINER_HOSTNAME,
            INITIAL_TEST_INFERENCE_COMPONENT_NAME,
            INITIAL_TEST_BATCH_SIZE,
            SageMakerStoredServiceSchema.NO_OP
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(settingsMap);

        assertThat(
            updatedServiceSettings,
            is(
                new SageMakerServiceSettings(
                    INITIAL_TEST_ENDPOINT_NAME,
                    INITIAL_TEST_REGION,
                    INITIAL_TEST_API,
                    INITIAL_TEST_TARGET_MODEL,
                    INITIAL_TEST_TARGET_CONTAINER_HOSTNAME,
                    INITIAL_TEST_INFERENCE_COMPONENT_NAME,
                    TEST_BATCH_SIZE,
                    SageMakerStoredServiceSchema.NO_OP
                )
            )
        );
    }

    public void testUpdateServiceSettings_EmptyMap_DoesNotChangeSettings() {
        var originalServiceSettings = new SageMakerServiceSettings(
            INITIAL_TEST_ENDPOINT_NAME,
            INITIAL_TEST_REGION,
            INITIAL_TEST_API,
            INITIAL_TEST_TARGET_MODEL,
            INITIAL_TEST_TARGET_CONTAINER_HOSTNAME,
            INITIAL_TEST_INFERENCE_COMPONENT_NAME,
            INITIAL_TEST_BATCH_SIZE,
            SageMakerStoredServiceSchema.NO_OP
        );
        var updatedServiceSettings = originalServiceSettings.updateServiceSettings(new HashMap<>());

        assertThat(updatedServiceSettings, is(originalServiceSettings));
    }

    private static Map<String, Object> buildServiceSettingsMap(
        @Nullable String endpointName,
        @Nullable String region,
        @Nullable String api,
        @Nullable String targetModel,
        @Nullable String targetContainerHostname,
        @Nullable String inferenceComponentName,
        @Nullable Integer batchSize
    ) {
        var map = new HashMap<String, Object>();
        if (endpointName != null) {
            map.put(SageMakerServiceSettings.ENDPOINT_NAME, endpointName);
        }
        if (region != null) {
            map.put(SageMakerServiceSettings.REGION, region);
        }
        if (api != null) {
            map.put(SageMakerServiceSettings.API, api);
        }
        if (targetModel != null) {
            map.put(SageMakerServiceSettings.TARGET_MODEL, targetModel);
        }
        if (targetContainerHostname != null) {
            map.put(SageMakerServiceSettings.TARGET_CONTAINER_HOSTNAME, targetContainerHostname);
        }
        if (inferenceComponentName != null) {
            map.put(SageMakerServiceSettings.INFERENCE_COMPONENT_NAME, inferenceComponentName);
        }
        if (batchSize != null) {
            map.put(SageMakerServiceSettings.BATCH_SIZE, batchSize);
        }
        return map;
    }

    private static Map<String, Object> buildElasticTextEmbeddingServiceSettingsMap(
        @Nullable String endpointName,
        @Nullable String region,
        @Nullable String targetModel,
        @Nullable String targetContainerHostname,
        @Nullable String inferenceComponentName,
        @Nullable Integer batchSize,
        @Nullable Integer dimensions,
        @Nullable Boolean dimensionsSetByUser,
        @Nullable SimilarityMeasure similarity,
        @Nullable DenseVectorFieldMapper.ElementType elementType
    ) {
        var map = buildServiceSettingsMap(
            endpointName,
            region,
            TEST_ELASTIC_API,
            targetModel,
            targetContainerHostname,
            inferenceComponentName,
            batchSize
        );
        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }
        if (dimensionsSetByUser != null) {
            map.put(ServiceFields.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        }
        if (similarity != null) {
            map.put(ServiceFields.SIMILARITY, similarity.toString());
        }
        if (elementType != null) {
            map.put(ELEMENT_TYPE_FIELD, elementType.toString());
        }
        return map;
    }

    private static Map<String, Object> buildOpenAiTextEmbeddingServiceSettingsMap(
        @Nullable String endpointName,
        @Nullable String region,
        @Nullable String targetModel,
        @Nullable String targetContainerHostname,
        @Nullable String inferenceComponentName,
        @Nullable Integer batchSize,
        @Nullable Integer dimensions
    ) {
        var map = buildServiceSettingsMap(
            endpointName,
            region,
            TEST_OPENAI_API,
            targetModel,
            targetContainerHostname,
            inferenceComponentName,
            batchSize
        );
        if (dimensions != null) {
            map.put(ServiceFields.DIMENSIONS, dimensions);
        }
        return map;
    }
}
