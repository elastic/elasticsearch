/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.MlNodeDeployedServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ElserMlNodeServiceSettings extends MlNodeDeployedServiceSettings {

    public static final String NAME = "elser_mlnode_service_settings";

    /**
     * Parse the Elser service setting from map and validate the setting values.
     *
     * If required setting are missing or the values are invalid an
     * {@link ValidationException} is thrown.
     *
     * @param map Source map containg the config
     * @return The {@code ElserMlNodeServiceSettings}
     */
    public static ElserMlNodeServiceSettings.Builder fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        Integer numAllocations = ServiceUtils.removeAsType(map, NUM_ALLOCATIONS, Integer.class);
        Integer numThreads = ServiceUtils.removeAsType(map, NUM_THREADS, Integer.class);

        validateParameters(numAllocations, validationException, numThreads);

        String version = ServiceUtils.removeAsType(map, MODEL_VERSION, String.class);
        if (version != null && ElserMlNodeService.VALID_ELSER_MODELS.contains(version) == false) {
            validationException.addValidationError("unknown ELSER model version [" + version + "]");
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        var builder = new MlNodeDeployedServiceSettings.Builder() {
            @Override
            public ElserMlNodeServiceSettings build() {
                return new ElserMlNodeServiceSettings(getNumAllocations(), getNumThreads(), getModelVariant());
            }
        };
        builder.setNumAllocations(numAllocations);
        builder.setNumThreads(numThreads);
        builder.setModelVariant(version);
        return builder;
    }

    public ElserMlNodeServiceSettings(int numAllocations, int numThreads, String variant) {
        super(numAllocations, numThreads, variant);
        Objects.requireNonNull(variant);
    }

    public ElserMlNodeServiceSettings(StreamInput in) throws IOException {
        super(
            in.readVInt(),
            in.readVInt(),
            transportVersionIsCompatibleWithElserModelVersion(in.getTransportVersion())
                ? in.readString()
                : ElserMlNodeService.ELSER_V2_MODEL
        );
    }

    static boolean transportVersionIsCompatibleWithElserModelVersion(TransportVersion transportVersion) {
        var nextNonPatchVersion = TransportVersions.PLUGIN_DESCRIPTOR_OPTIONAL_CLASSNAME;

        if (transportVersion.onOrAfter(TransportVersions.ELSER_SERVICE_MODEL_VERSION_ADDED)) {
            return true;
        } else {
            return transportVersion.onOrAfter(TransportVersions.ELSER_SERVICE_MODEL_VERSION_ADDED_PATCH)
                && transportVersion.before(nextNonPatchVersion);
        }
    }

    @Override
    public String getWriteableName() {
        return ElserMlNodeServiceSettings.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_500_074;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(getNumAllocations());
        out.writeVInt(getNumThreads());
        if (transportVersionIsCompatibleWithElserModelVersion(out.getTransportVersion())) {
            out.writeString(getModelVariant());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(NAME, getNumAllocations(), getNumThreads(), getModelVariant());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElserMlNodeServiceSettings that = (ElserMlNodeServiceSettings) o;
        return getNumAllocations() == that.getNumAllocations()
            && getNumThreads() == that.getNumThreads()
            && Objects.equals(getModelVariant(), that.getModelVariant());
    }

}
