/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import java.io.IOException;
import java.util.Objects;

public abstract class InternalServiceSettings implements ServiceSettings {

    public static final String NUM_ALLOCATIONS = "num_allocations";
    public static final String NUM_THREADS = "num_threads";
    public static final String MODEL_ID = "model_id";
    public static final String ADAPTIVE_ALLOCATIONS = "adaptive_allocations";

    private final Integer numAllocations;
    private final int numThreads;
    private final String modelId;
    private final AdaptiveAllocationsSettings adaptiveAllocationsSettings;

    public InternalServiceSettings(
        Integer numAllocations,
        int numThreads,
        String modelId,
        AdaptiveAllocationsSettings adaptiveAllocationsSettings
    ) {
        this.numAllocations = numAllocations;
        this.numThreads = numThreads;
        this.modelId = modelId;
        this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
    }

    public Integer getNumAllocations() {
        return numAllocations;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public String getModelId() {
        return modelId;
    }

    public AdaptiveAllocationsSettings getAdaptiveAllocationsSettings() {
        return adaptiveAllocationsSettings;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalServiceSettings that = (InternalServiceSettings) o;
        return Objects.equals(numAllocations, that.numAllocations)
            && numThreads == that.numThreads
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(adaptiveAllocationsSettings, that.adaptiveAllocationsSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numAllocations, numThreads, modelId, adaptiveAllocationsSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        addXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    public void addXContentFragment(XContentBuilder builder, Params params) throws IOException {
        if (numAllocations != null) {
            builder.field(NUM_ALLOCATIONS, getNumAllocations());
        }
        builder.field(NUM_THREADS, getNumThreads());
        builder.field(MODEL_ID, getModelId());
        if (adaptiveAllocationsSettings != null) {
            builder.field(ADAPTIVE_ALLOCATIONS, getAdaptiveAllocationsSettings());
        }
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)) {
            out.writeOptionalVInt(getNumAllocations());
        } else {
            out.writeVInt(getNumAllocations());
        }
        out.writeVInt(getNumThreads());
        out.writeString(getModelId());
        if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_ADAPTIVE_ALLOCATIONS)) {
            out.writeOptionalWriteable(getAdaptiveAllocationsSettings());
        }
    }

    public abstract static class Builder {
        private Integer numAllocations;
        private int numThreads;
        private String modelId;
        private AdaptiveAllocationsSettings adaptiveAllocationsSettings;

        public abstract InternalServiceSettings build();

        public void setNumAllocations(Integer numAllocations) {
            this.numAllocations = numAllocations;
        }

        public void setNumThreads(int numThreads) {
            this.numThreads = numThreads;
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setAdaptiveAllocationsSettings(AdaptiveAllocationsSettings adaptiveAllocationsSettings) {
            this.adaptiveAllocationsSettings = adaptiveAllocationsSettings;
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
}
