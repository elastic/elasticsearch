/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86;

public class MultilingualE5SmallInternalServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "multilingual_e5_small_service_settings";

    static final int DIMENSIONS = 384;
    static final SimilarityMeasure SIMILARITY = SimilarityMeasure.COSINE;

    public static MinimalServiceSettings minimalServiceSettings() {
        return MinimalServiceSettings.textEmbedding(
            ElasticsearchInternalService.NAME,
            DIMENSIONS,
            SIMILARITY,
            DenseVectorFieldMapper.ElementType.FLOAT
        );
    }

    public static MultilingualE5SmallInternalServiceSettings defaultEndpointSettings(boolean useLinuxOptimizedModel) {
        return new MultilingualE5SmallInternalServiceSettings(
            null,
            1,
            useLinuxOptimizedModel ? MULTILINGUAL_E5_SMALL_MODEL_ID_LINUX_X86 : MULTILINGUAL_E5_SMALL_MODEL_ID,
            new AdaptiveAllocationsSettings(Boolean.TRUE, 0, 32)
        );
    }

    public MultilingualE5SmallInternalServiceSettings(ElasticsearchInternalServiceSettings other) {
        super(other);
    }

    MultilingualE5SmallInternalServiceSettings(
        Integer numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        super(numAllocations, numThreads, modelId, adaptiveAllocationsSettings, null);
    }

    public MultilingualE5SmallInternalServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Parse the MultilingualE5SmallServiceSettings from map and validate the setting values.
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containing the config
     * @return The builder
     */
    public static ElasticsearchInternalServiceSettings.Builder fromRequestMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var baseSettings = ElasticsearchInternalServiceSettings.fromMap(map, validationException);

        String modelId = baseSettings.getModelId();
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

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return baseSettings;
    }

    @Override
    public String getWriteableName() {
        return MultilingualE5SmallInternalServiceSettings.NAME;
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
