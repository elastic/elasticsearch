/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.InternalServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

public class ElasticsearchInternalServiceSettings extends InternalServiceSettings {

    public static final String NAME = "text_embedding_internal_service_settings";
    private static final int FAILED_INT_PARSE_VALUE = -1;

    public static ElasticsearchInternalServiceSettings fromMap(Map<String, Object> map, ValidationException validationException) {
        Integer numAllocations = extractRequiredPositiveInteger(
            map,
            NUM_ALLOCATIONS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        Integer numThreads = extractRequiredPositiveInteger(map, NUM_THREADS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        AdaptiveAllocationsSettings adaptiveAllocationsSettings = ServiceUtils.removeAsAdaptiveAllocationsSettings(
            map,
            ADAPTIVE_ALLOCATIONS
        );
        if (adaptiveAllocationsSettings != null) {
            ActionRequestValidationException exception = adaptiveAllocationsSettings.validate();
            if (exception != null) {
                validationException.addValidationErrors(exception.validationErrors());
            }
        }
        String modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        // if an error occurred while parsing, we'll set these to an invalid value, so we don't accidentally get a
        // null pointer when doing unboxing
        return new ElasticsearchInternalServiceSettings(
            Objects.requireNonNullElse(numAllocations, FAILED_INT_PARSE_VALUE),
            Objects.requireNonNullElse(numThreads, FAILED_INT_PARSE_VALUE),
            modelId,
            adaptiveAllocationsSettings
        );
    }

    public ElasticsearchInternalServiceSettings(
        int numAllocations,
        int numThreads,
        String modelVariant,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        super(numAllocations, numThreads, modelVariant, adaptiveAllocationsSettings);
    }

    public ElasticsearchInternalServiceSettings(StreamInput in) throws IOException {
        super(
            in.readVInt(),
            in.readVInt(),
            in.readString(),
            in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)
                ? in.readOptionalWriteable(AdaptiveAllocationsSettings::new)
                : null
        );
    }

    @Override
    public String getWriteableName() {
        return ElasticsearchInternalServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_13_0;
    }

}
