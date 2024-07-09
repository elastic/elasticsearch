/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.InternalServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredPositiveInteger;

public class ElserInternalServiceSettings extends InternalServiceSettings {

    public static final String NAME = "elser_mlnode_service_settings";

    /**
     * Parse the Elser service setting from map and validate the setting values.
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containing the config
     * @return The {@code ElserInternalServiceSettings}
     */
    public static ElserInternalServiceSettings.Builder fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

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
        String modelId = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        if (modelId != null && ElserInternalService.VALID_ELSER_MODEL_IDS.contains(modelId) == false) {
            validationException.addValidationError("unknown ELSER model id [" + modelId + "]");
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        var builder = new InternalServiceSettings.Builder() {
            @Override
            public ElserInternalServiceSettings build() {
                return new ElserInternalServiceSettings(
                    getNumAllocations(),
                    getNumThreads(),
                    getModelId(),
                    getAdaptiveAllocationsSettings()
                );
            }
        };
        builder.setNumAllocations(numAllocations);
        builder.setNumThreads(numThreads);
        builder.setAdaptiveAllocationsSettings(adaptiveAllocationsSettings);
        builder.setModelId(modelId);
        return builder;
    }

    public ElserInternalServiceSettings(
        int numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        super(numAllocations, numThreads, modelId, adaptiveAllocationsSettings);
        Objects.requireNonNull(modelId);
    }

    public ElserInternalServiceSettings(StreamInput in) throws IOException {
        super(
            in.readVInt(),
            in.readVInt(),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X) ? in.readString() : ElserInternalService.ELSER_V2_MODEL,
            in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)
                ? in.readOptionalWriteable(AdaptiveAllocationsSettings::new)
                : null
        );
    }

    @Override
    public String getWriteableName() {
        return ElserInternalServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_11_X;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(getNumAllocations());
        out.writeVInt(getNumThreads());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
            out.writeString(getModelId());
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)) {
            out.writeOptionalWriteable(getAdaptiveAllocationsSettings());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(NAME, getNumAllocations(), getNumThreads(), getModelId(), getAdaptiveAllocationsSettings());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElserInternalServiceSettings that = (ElserInternalServiceSettings) o;
        return getNumAllocations() == that.getNumAllocations()
            && getNumThreads() == that.getNumThreads()
            && Objects.equals(getModelId(), that.getModelId())
            && Objects.equals(getAdaptiveAllocationsSettings(), that.getAdaptiveAllocationsSettings());
    }
}
