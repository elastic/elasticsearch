/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import java.io.IOException;
import java.util.Map;

public class ElasticRerankerServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "elastic_reranker_service_settings";

    public ElasticRerankerServiceSettings(ElasticsearchInternalServiceSettings other) {
        super(other);
    }

    public ElasticRerankerServiceSettings(
        Integer numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        super(numAllocations, numThreads, modelId, adaptiveAllocationsSettings);
    }

    public ElasticRerankerServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Parse the ElasticRerankerServiceSettings from map and validate the setting values.
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containing the config
     * @return The builder
     */
    public static Builder fromRequestMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var baseSettings = ElasticsearchInternalServiceSettings.fromMap(map, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return baseSettings;
    }

    @Override
    public String getWriteableName() {
        return ElasticRerankerServiceSettings.NAME;
    }
}
