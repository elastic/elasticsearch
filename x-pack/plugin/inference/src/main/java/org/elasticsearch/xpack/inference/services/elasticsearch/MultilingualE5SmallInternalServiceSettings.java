/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.InternalServiceSettings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.TransportVersions.ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED;

public class MultilingualE5SmallInternalServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "multilingual_e5_small_service_settings";

    static final int DIMENSIONS = 384;
    static final SimilarityMeasure SIMILARITY = SimilarityMeasure.COSINE;

    public MultilingualE5SmallInternalServiceSettings(int numAllocations, int numThreads, String modelId) {
        super(numAllocations, numThreads, modelId);
    }

    public MultilingualE5SmallInternalServiceSettings(StreamInput in) throws IOException {
        super(in.readVInt(), in.readVInt(), in.readString());
    }

    /**
     * Parse the MultilingualE5SmallServiceSettings from map and validate the setting values.
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containing the config
     * @return The {@code MultilingualE5SmallServiceSettings} builder
     */
    public static MultilingualE5SmallInternalServiceSettings.Builder fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var requestFields = extractRequestFields(map, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return createBuilder(requestFields);
    }

    private static RequestFields extractRequestFields(Map<String, Object> map, ValidationException validationException) {
        Integer numAllocations = ServiceUtils.removeAsType(map, NUM_ALLOCATIONS, Integer.class);
        Integer numThreads = ServiceUtils.removeAsType(map, NUM_THREADS, Integer.class);

        validateParameters(numAllocations, validationException, numThreads);

        String modelId = ServiceUtils.removeAsType(map, MODEL_ID, String.class);
        if (modelId != null) {
            if (ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_VALID_IDS.contains(modelId) == false) {
                validationException.addValidationError(
                    "unknown Multilingual-E5-Small model ID ["
                        + modelId
                        + "]. Valid IDs are "
                        + Arrays.toString(ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_VALID_IDS.toArray())
                );
            }
        }

        return new RequestFields(numAllocations, numThreads, modelId);
    }

    private static MultilingualE5SmallInternalServiceSettings.Builder createBuilder(RequestFields requestFields) {
        var builder = new InternalServiceSettings.Builder() {
            @Override
            public MultilingualE5SmallInternalServiceSettings build() {
                return new MultilingualE5SmallInternalServiceSettings(getNumAllocations(), getNumThreads(), getModelId());
            }
        };
        builder.setNumAllocations(requestFields.numAllocations);
        builder.setNumThreads(requestFields.numThreads);
        builder.setModelId(requestFields.modelId);
        return builder;
    }

    private record RequestFields(@Nullable Integer numAllocations, @Nullable Integer numThreads, @Nullable String modelId) {}

    @Override
    public boolean isFragment() {
        return super.isFragment();
    }

    @Override
    public String getWriteableName() {
        return MultilingualE5SmallInternalServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_TEXT_EMBEDDING_INFERENCE_SERVICE_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public SimilarityMeasure similarity() {
        return SIMILARITY;
    }

    @Override
    public Integer dimensions() {
        return DIMENSIONS;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }
}
