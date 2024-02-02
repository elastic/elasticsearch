/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.Objects;

public abstract class MlNodeServiceSettings implements ServiceSettings {

    public static final String NUM_ALLOCATIONS = "num_allocations";
    public static final String NUM_THREADS = "num_threads";
    public static final String MODEL_VERSION = "model_version";

    private final int numAllocations;
    private final int numThreads;
    private final String modelVariant;

    public MlNodeServiceSettings(int numAllocations, int numThreads, String modelVariant) {
        this.numAllocations = numAllocations;
        this.numThreads = numThreads;
        this.modelVariant = modelVariant;
    }

    protected static void validateParameters(Integer numAllocations, ValidationException validationException, Integer numThreads) {
        if (numAllocations == null) {
            validationException.addValidationError(
                ServiceUtils.missingSettingErrorMsg(NUM_ALLOCATIONS, ModelConfigurations.SERVICE_SETTINGS)
            );
        } else if (numAllocations < 1) {
            validationException.addValidationError(ServiceUtils.mustBeAPositiveNumberErrorMessage(NUM_ALLOCATIONS, numAllocations));
        }

        if (numThreads == null) {
            validationException.addValidationError(ServiceUtils.missingSettingErrorMsg(NUM_THREADS, ModelConfigurations.SERVICE_SETTINGS));
        } else if (numThreads < 1) {
            validationException.addValidationError(ServiceUtils.mustBeAPositiveNumberErrorMessage(NUM_THREADS, numThreads));
        }
    }

    public int getNumAllocations() {
        return numAllocations;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public String getModelVariant() {
        return modelVariant;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MlNodeServiceSettings that = (MlNodeServiceSettings) o;
        return numAllocations == that.numAllocations && numThreads == that.numThreads && Objects.equals(modelVariant, that.modelVariant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numAllocations, numThreads, modelVariant);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUM_ALLOCATIONS, getNumAllocations());
        builder.field(NUM_THREADS, getNumThreads());
        builder.field(MODEL_VERSION, getModelVariant());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isFragment() {
        return ServiceSettings.super.isFragment();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(getNumAllocations());
        out.writeVInt(getNumThreads());
        out.writeString(getModelVariant());
    }

    public abstract static class Builder {
        private int numAllocations;
        private int numThreads;
        private String modelVariant;

        public abstract MlNodeServiceSettings build();

        public void setNumAllocations(int numAllocations) {
            this.numAllocations = numAllocations;
        }

        public void setNumThreads(int numThreads) {
            this.numThreads = numThreads;
        }

        public void setModelVariant(String modelVariant) {
            this.modelVariant = modelVariant;
        }

        public String getModelVariant() {
            return modelVariant;
        }

        public int getNumAllocations() {
            return numAllocations;
        }

        public int getNumThreads() {
            return numThreads;
        }
    }
}
