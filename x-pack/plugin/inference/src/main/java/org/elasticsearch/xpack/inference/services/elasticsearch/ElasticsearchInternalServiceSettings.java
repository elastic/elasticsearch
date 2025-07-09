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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredPositiveInteger;

public class ElasticsearchInternalServiceSettings implements ServiceSettings {

    public static final String NAME = "text_embedding_internal_service_settings";
    private static final int FAILED_INT_PARSE_VALUE = -1;

    public static final String NUM_ALLOCATIONS = "num_allocations";
    public static final String NUM_THREADS = "num_threads";
    public static final String MODEL_ID = "model_id";
    public static final String DEPLOYMENT_ID = "deployment_id";
    public static final String ADAPTIVE_ALLOCATIONS = "adaptive_allocations";

    private Integer numAllocations;
    private final int numThreads;
    private final String modelId;
    private final AdaptiveAllocationsSettings adaptiveAllocationsSettings;
    private final String deploymentId;

    public static ElasticsearchInternalServiceSettings fromPersistedMap(Map<String, Object> map) {
        return fromRequestMap(map).build();
    }

    /**
     * Parse the ElasticsearchInternalServiceSettings from the map.
     * Validates that present threading settings are of the right type and value,
     * The model id is optional, it is for the inference service to check and
     * potentially set a default value for the model id.
     * Throws an {@code ValidationException} on validation failures
     *
     * @param map The request map.
     * @return A builder to allow the settings to be modified.
     */
    public static ElasticsearchInternalServiceSettings.Builder fromRequestMap(Map<String, Object> map) {
        var validationException = new ValidationException();
        var builder = fromMap(map, validationException);
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }
        return builder;
    }

    protected static ElasticsearchInternalServiceSettings.Builder fromMap(
        Map<String, Object> map,
        ValidationException validationException
    ) {
        Integer numAllocations = extractOptionalPositiveInteger(
            map,
            NUM_ALLOCATIONS,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        Integer numThreads = extractRequiredPositiveInteger(map, NUM_THREADS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        AdaptiveAllocationsSettings adaptiveAllocationsSettings = ServiceUtils.removeAsAdaptiveAllocationsSettings(
            map,
            ADAPTIVE_ALLOCATIONS,
            validationException
        );

        // model id is optional as the ELSER service will default it. TODO make this a required field once the elser service is removed
        String modelId = extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        if (numAllocations == null && adaptiveAllocationsSettings == null) {
            validationException.addValidationError(
                ServiceUtils.missingOneOfSettingsErrorMsg(
                    List.of(NUM_ALLOCATIONS, ADAPTIVE_ALLOCATIONS),
                    ModelConfigurations.SERVICE_SETTINGS
                )
            );
        }

        String deploymentId = extractOptionalString(map, DEPLOYMENT_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);

        // if an error occurred while parsing, we'll set these to an invalid value, so we don't accidentally get a
        // null pointer when doing unboxing
        return new ElasticsearchInternalServiceSettings.Builder().setNumAllocations(numAllocations)
            .setNumThreads(Objects.requireNonNullElse(numThreads, FAILED_INT_PARSE_VALUE))
            .setModelId(modelId)
            .setAdaptiveAllocationsSettings(adaptiveAllocationsSettings)
            .setDeploymentId(deploymentId);
    }

    public ElasticsearchInternalServiceSettings(
        Integer numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings,
        @Nullable String deploymentId
    ) {
        this.numAllocations = numAllocations;
        this.numThreads = numThreads;
        this.modelId = Objects.requireNonNull(modelId);
        this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
        this.deploymentId = deploymentId;
    }

    protected ElasticsearchInternalServiceSettings(ElasticsearchInternalServiceSettings other) {
        this.numAllocations = other.numAllocations;
        this.numThreads = other.numThreads;
        this.modelId = other.modelId;
        this.adaptiveAllocationsSettings = other.adaptiveAllocationsSettings;
        this.deploymentId = other.deploymentId;
    }

    /**
     * Copy constructor with the ability to set the number of allocations. Used for Update API.
     * @param other the existing settings
     * @param numAllocations the new number of allocations
     */
    public ElasticsearchInternalServiceSettings(ElasticsearchInternalServiceSettings other, int numAllocations) {
        this.numAllocations = numAllocations;
        this.numThreads = other.numThreads;
        this.modelId = other.modelId;
        this.adaptiveAllocationsSettings = other.adaptiveAllocationsSettings;
        this.deploymentId = other.deploymentId;
    }

    public ElasticsearchInternalServiceSettings(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            this.numAllocations = in.readOptionalVInt();
        } else {
            this.numAllocations = in.readVInt();
        }
        this.numThreads = in.readVInt();
        this.modelId = in.readString();
        this.adaptiveAllocationsSettings = in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)
            ? in.readOptionalWriteable(AdaptiveAllocationsSettings::new)
            : null;
        this.deploymentId = in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readOptionalString() : null;
    }

    public void setNumAllocations(Integer numAllocations) {
        this.numAllocations = numAllocations;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeOptionalVInt(getNumAllocations());
        } else {
            out.writeVInt(getNumAllocations());
        }
        out.writeVInt(getNumThreads());
        out.writeString(modelId());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeOptionalWriteable(getAdaptiveAllocationsSettings());
            out.writeOptionalString(deploymentId);
        }
    }

    @Override
    public String modelId() {
        return modelId;
    }

    public String deloymentId() {
        return modelId;
    }

    public Integer getNumAllocations() {
        return numAllocations;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public AdaptiveAllocationsSettings getAdaptiveAllocationsSettings() {
        return adaptiveAllocationsSettings;
    }

    public String getDeploymentId() {
        return deploymentId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        addInternalSettingsToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected void addInternalSettingsToXContent(XContentBuilder builder, Params params) throws IOException {
        if (numAllocations != null) {
            builder.field(NUM_ALLOCATIONS, numAllocations);
        }
        builder.field(NUM_THREADS, getNumThreads());
        builder.field(MODEL_ID, modelId());
        if (adaptiveAllocationsSettings != null) {
            builder.field(ADAPTIVE_ALLOCATIONS, adaptiveAllocationsSettings);
        }
        if (deploymentId != null) {
            builder.field(DEPLOYMENT_ID, deploymentId);
        }
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public String getWriteableName() {
        return ElasticsearchInternalServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_13_0;
    }

    public static class Builder {
        private Integer numAllocations;
        private int numThreads;
        private String modelId;
        private AdaptiveAllocationsSettings adaptiveAllocationsSettings;
        private String deploymentId;

        public ElasticsearchInternalServiceSettings build() {
            return new ElasticsearchInternalServiceSettings(numAllocations, numThreads, modelId, adaptiveAllocationsSettings, deploymentId);
        }

        public Builder setNumAllocations(Integer numAllocations) {
            this.numAllocations = numAllocations;
            return this;
        }

        public Builder setNumThreads(int numThreads) {
            this.numThreads = numThreads;
            return this;
        }

        public Builder setModelId(String modelId) {
            this.modelId = modelId;
            return this;
        }

        public Builder setDeploymentId(String deploymentId) {
            this.deploymentId = deploymentId;
            return this;
        }

        public Builder setAdaptiveAllocationsSettings(AdaptiveAllocationsSettings adaptiveAllocationsSettings) {
            this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
            return this;
        }

        public String getModelId() {
            return modelId;
        }

        public Integer getNumAllocations() {
            return numAllocations;
        }

        public int getNumThreads() {
            return numThreads;
        }

        public AdaptiveAllocationsSettings getAdaptiveAllocationsSettings() {
            return adaptiveAllocationsSettings;
        }
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchInternalServiceSettings that = (ElasticsearchInternalServiceSettings) o;
        return Objects.equals(numAllocations, that.numAllocations)
            && numThreads == that.numThreads
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(adaptiveAllocationsSettings, that.adaptiveAllocationsSettings)
            && Objects.equals(deploymentId, that.deploymentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numAllocations, numThreads, modelId, adaptiveAllocationsSettings, deploymentId);
    }
}
