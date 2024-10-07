/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class ElserInternalServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "elser_mlnode_service_settings";

    public static Builder fromRequestMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var baseSettings = ElasticsearchInternalServiceSettings.fromMap(map, validationException);

        String modelId = baseSettings.getModelId();
        if (modelId != null && ElserModels.isValidModel(modelId) == false) {
            var ve = new ValidationException();
            ve.addValidationError(
                "Unknown ELSER model ID [" + modelId + "]. Valid models are " + Arrays.toString(ElserModels.VALID_ELSER_MODEL_IDS.toArray())
            );
            throw ve;
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return baseSettings;
    }

    public ElserInternalServiceSettings(ElasticsearchInternalServiceSettings other) {
        super(other);
    }

    public ElserInternalServiceSettings(
        Integer numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        this(new ElasticsearchInternalServiceSettings(numAllocations, numThreads, modelId, adaptiveAllocationsSettings));
    }

    public ElserInternalServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ElserInternalServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_11_X;
    }
}
