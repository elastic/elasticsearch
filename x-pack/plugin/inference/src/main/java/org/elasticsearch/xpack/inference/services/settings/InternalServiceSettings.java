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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.Objects;

public abstract class InternalServiceSettings implements ServiceSettings {

    public static final String NUM_ALLOCATIONS = "num_allocations";
    public static final String NUM_THREADS = "num_threads";
    public static final String MODEL_ID = "model_id";

    private final int numAllocations;
    private final int numThreads;
    private final String modelId;

    public InternalServiceSettings(int numAllocations, int numThreads, String modelId) {
        this.numAllocations = numAllocations;
        this.numThreads = numThreads;
        this.modelId = modelId;
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

    public String getModelId() {
        return modelId;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalServiceSettings that = (InternalServiceSettings) o;
        return numAllocations == that.numAllocations && numThreads == that.numThreads && Objects.equals(modelId, that.modelId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numAllocations, numThreads, modelId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUM_ALLOCATIONS, getNumAllocations());
        builder.field(NUM_THREADS, getNumThreads());
        builder.field(MODEL_ID, getModelId());
        builder.endObject();
        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public boolean isFragment() {
        return ServiceSettings.super.isFragment();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(getNumAllocations());
        out.writeVInt(getNumThreads());
        out.writeString(getModelId());
    }

    public abstract static class Builder {
        private int numAllocations;
        private int numThreads;
        private String modelId;

        public abstract InternalServiceSettings build();

        public void setNumAllocations(int numAllocations) {
            this.numAllocations = numAllocations;
        }

        public void setNumThreads(int numThreads) {
            this.numThreads = numThreads;
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public String getModelId() {
            return modelId;
        }

        public int getNumAllocations() {
            return numAllocations;
        }

        public int getNumThreads() {
            return numThreads;
        }
    }
}
