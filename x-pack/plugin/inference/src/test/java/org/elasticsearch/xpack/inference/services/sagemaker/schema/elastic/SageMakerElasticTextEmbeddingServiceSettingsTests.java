/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic.ElasticTextEmbeddingPayload.ApiServiceSettings.ELEMENT_TYPE_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class SageMakerElasticTextEmbeddingServiceSettingsTests extends InferenceSettingsTestCase<
    ElasticTextEmbeddingPayload.ApiServiceSettings> {

    public void testFromMap_SetsValidationErrorWhenSimilarityIsNotPresent() {
        var validationException = new ValidationException();
        var settings = ElasticTextEmbeddingPayload.ApiServiceSettings.fromMap(
            new HashMap<>(Map.of(ELEMENT_TYPE_FIELD, DenseVectorFieldMapper.ElementType.FLOAT.toString())),
            ConfigurationParseContext.REQUEST,
            validationException
        );
        var exception = expectThrows(ValidationException.class, validationException::throwIfValidationErrorsExist);
        assertThat(exception.getMessage(), containsString("[service_settings] does not contain the required setting [similarity]"));
        assertThat(settings.similarity(), is(nullValue()));
    }

    public void testFromRequest_DimensionsSetByUserIsDerivedFromDimensions() {
        var validationException = new ValidationException();
        var withDimensions = ElasticTextEmbeddingPayload.ApiServiceSettings.fromMap(
            new HashMap<String, Object>(
                Map.of(
                    "dimensions",
                    123,
                    "similarity",
                    SimilarityMeasure.COSINE.toString(),
                    ELEMENT_TYPE_FIELD,
                    DenseVectorFieldMapper.ElementType.FLOAT.toString()
                )
            ),
            ConfigurationParseContext.REQUEST,
            validationException
        );
        validationException.throwIfValidationErrorsExist();
        assertThat(withDimensions.dimensions(), is(123));
        assertThat(withDimensions.dimensionsSetByUser(), is(true));

        var withoutDimensions = ElasticTextEmbeddingPayload.ApiServiceSettings.fromMap(
            new HashMap<String, Object>(
                Map.of(
                    "similarity",
                    SimilarityMeasure.COSINE.toString(),
                    ELEMENT_TYPE_FIELD,
                    DenseVectorFieldMapper.ElementType.FLOAT.toString()
                )
            ),
            ConfigurationParseContext.REQUEST,
            validationException
        );
        validationException.throwIfValidationErrorsExist();
        assertThat(withoutDimensions.dimensions(), is(nullValue()));
        assertThat(withoutDimensions.dimensionsSetByUser(), is(false));
    }

    public void testFromRequest_DoesNotConsumeDimensionsSetByUser() {
        // In a request, dimensions_set_by_user is not parsed, so it remains in the map for the service to reject as unknown.
        var validationException = new ValidationException();
        var map = new HashMap<String, Object>(
            Map.of(
                "dimensions",
                123,
                "dimensions_set_by_user",
                false,
                "similarity",
                SimilarityMeasure.COSINE.toString(),
                ELEMENT_TYPE_FIELD,
                DenseVectorFieldMapper.ElementType.FLOAT.toString()
            )
        );
        ElasticTextEmbeddingPayload.ApiServiceSettings.fromMap(map, ConfigurationParseContext.REQUEST, validationException);
        validationException.throwIfValidationErrorsExist();
        assertThat(map, hasKey("dimensions_set_by_user"));
    }

    public void testFromStorage_MissingDimensionsSetByUser_AddsValidationError() {
        // Persisted configs have always contained the field for this class, so a missing value indicates corrupt data.
        var validationException = new ValidationException();
        ElasticTextEmbeddingPayload.ApiServiceSettings.fromMap(
            new HashMap<String, Object>(
                Map.of(
                    "dimensions",
                    123,
                    "similarity",
                    SimilarityMeasure.COSINE.toString(),
                    ELEMENT_TYPE_FIELD,
                    DenseVectorFieldMapper.ElementType.FLOAT.toString()
                )
            ),
            ConfigurationParseContext.PERSISTENT,
            validationException
        );
        var exception = expectThrows(ValidationException.class, validationException::throwIfValidationErrorsExist);
        assertThat(
            exception.getMessage(),
            containsString("[service_settings] does not contain the required setting [dimensions_set_by_user]")
        );
    }

    public void testFromStorage_ReadsDimensionsSetByUser() {
        var validationException = new ValidationException();
        var map = new HashMap<String, Object>(
            Map.of(
                "dimensions",
                123,
                "dimensions_set_by_user",
                false,
                "similarity",
                SimilarityMeasure.COSINE.toString(),
                ELEMENT_TYPE_FIELD,
                DenseVectorFieldMapper.ElementType.FLOAT.toString()
            )
        );
        var settings = ElasticTextEmbeddingPayload.ApiServiceSettings.fromMap(
            map,
            ConfigurationParseContext.PERSISTENT,
            validationException
        );
        validationException.throwIfValidationErrorsExist();
        assertThat(settings.dimensionsSetByUser(), is(false));
        assertThat(map, not(hasKey("dimensions_set_by_user")));
    }

    @Override
    protected ElasticTextEmbeddingPayload.ApiServiceSettings fromMutableMap(Map<String, Object> mutableMap) {
        var validationException = new ValidationException();
        var settings = ElasticTextEmbeddingPayload.ApiServiceSettings.fromMap(
            mutableMap,
            ConfigurationParseContext.PERSISTENT,
            validationException
        );
        validationException.throwIfValidationErrorsExist();
        return settings;
    }

    @Override
    protected Writeable.Reader<ElasticTextEmbeddingPayload.ApiServiceSettings> instanceReader() {
        return ElasticTextEmbeddingPayload.ApiServiceSettings::new;
    }

    @Override
    protected ElasticTextEmbeddingPayload.ApiServiceSettings createTestInstance() {
        return randomInstance();
    }

    static ElasticTextEmbeddingPayload.ApiServiceSettings randomInstance() {
        return randomInstance(randomFrom(DenseVectorFieldMapper.ElementType.values()));
    }

    static ElasticTextEmbeddingPayload.ApiServiceSettings randomInstance(DenseVectorFieldMapper.ElementType elementType) {
        return new ElasticTextEmbeddingPayload.ApiServiceSettings(
            randomIntBetween(1, 100),
            randomBoolean(),
            randomFrom(SimilarityMeasure.values()),
            elementType
        );
    }
}
