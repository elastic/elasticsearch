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
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;

import java.util.Map;

public class SageMakerElasticTextEmbeddingServiceSettingsTests extends InferenceSettingsTestCase<
    ElasticTextEmbeddingPayload.ApiServiceSettings> {

    @Override
    protected ElasticTextEmbeddingPayload.ApiServiceSettings fromMutableMap(Map<String, Object> mutableMap) {
        var validationException = new ValidationException();
        var settings = ElasticTextEmbeddingPayload.ApiServiceSettings.fromMap(mutableMap, validationException);
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
