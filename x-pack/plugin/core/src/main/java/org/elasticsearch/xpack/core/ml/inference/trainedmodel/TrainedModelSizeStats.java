/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;

import java.io.IOException;
import java.util.Objects;

public class TrainedModelSizeStats implements ToXContentObject, Writeable {

    private static final ParseField MODEL_SIZE_BYTES = new ParseField("model_size_bytes");
    private static final ParseField REQUIRED_NATIVE_MEMORY_BYTES = new ParseField("required_native_memory_bytes");
    private static final ParseField PER_DEPLOYMENT_MEMORY_BYTES = new ParseField("per_deployment_memory_bytes");
    private static final ParseField PER_ALLOCATION_MEMORY_BYTES = new ParseField("per_allocation_memory_bytes");

    private final long modelSizeBytes;
    private final long requiredNativeMemoryBytes;
    private final long perDeploymentMemoryBytes;
    private final long perAllocationMemoryBytes;

    public TrainedModelSizeStats(
        long modelSizeBytes,
        long requiredNativeMemoryBytes,
        long perDeploymentMemoryBytes,
        long perAllocationMemoryBytes
    ) {
        this.modelSizeBytes = modelSizeBytes;
        this.requiredNativeMemoryBytes = requiredNativeMemoryBytes;
        this.perDeploymentMemoryBytes = perDeploymentMemoryBytes;
        this.perAllocationMemoryBytes = perAllocationMemoryBytes;
    }

    public TrainedModelSizeStats(StreamInput in) throws IOException {
        modelSizeBytes = in.readLong();
        requiredNativeMemoryBytes = in.readLong();
        if (in.getTransportVersion().onOrAfter(TrainedModelConfig.VERSION_ALLOCATION_MEMORY_ADDED)) {
            perDeploymentMemoryBytes = in.readLong();
            perAllocationMemoryBytes = in.readLong();
        } else {
            perDeploymentMemoryBytes = 0;
            perAllocationMemoryBytes = 0;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(modelSizeBytes);
        out.writeLong(requiredNativeMemoryBytes);
        if (perDeploymentMemoryBytes > 0) {
            out.writeLong(perDeploymentMemoryBytes);
        }
        if (perAllocationMemoryBytes > 0) {
            out.writeLong(perAllocationMemoryBytes);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.humanReadableField(MODEL_SIZE_BYTES.getPreferredName(), "model_size", ByteSizeValue.ofBytes(modelSizeBytes));
        builder.humanReadableField(
            REQUIRED_NATIVE_MEMORY_BYTES.getPreferredName(),
            "required_native_memory",
            ByteSizeValue.ofBytes(requiredNativeMemoryBytes)
        );
        if (perDeploymentMemoryBytes > 0) {
            builder.humanReadableField(
                PER_DEPLOYMENT_MEMORY_BYTES.getPreferredName(),
                "per_deployment_memory",
                ByteSizeValue.ofBytes(perDeploymentMemoryBytes)
            );
        }
        if (perAllocationMemoryBytes > 0) {
            builder.humanReadableField(
                PER_ALLOCATION_MEMORY_BYTES.getPreferredName(),
                "per_allocation_memory",
                ByteSizeValue.ofBytes(perAllocationMemoryBytes)
            );
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelSizeStats that = (TrainedModelSizeStats) o;
        return Objects.equals(modelSizeBytes, that.modelSizeBytes)
            && Objects.equals(requiredNativeMemoryBytes, that.requiredNativeMemoryBytes)
            && Objects.equals(perDeploymentMemoryBytes, that.perDeploymentMemoryBytes)
            && Objects.equals(perAllocationMemoryBytes, that.perAllocationMemoryBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelSizeBytes, requiredNativeMemoryBytes, perDeploymentMemoryBytes, perAllocationMemoryBytes);
    }

    public long getModelSizeBytes() {
        return modelSizeBytes;
    }

    public static class Builder {
        private Long modelSizeBytes;
        private Long requiredNativeMemoryBytes;
        private Long perDeploymentMemoryBytes;
        private Long perAllocationMemoryBytes;

        public Builder() {}

        public Builder(TrainedModelSizeStats stats) {
            this.modelSizeBytes = stats.modelSizeBytes;
            this.requiredNativeMemoryBytes = stats.requiredNativeMemoryBytes;
            this.perDeploymentMemoryBytes = stats.perDeploymentMemoryBytes;
            this.perAllocationMemoryBytes = stats.perAllocationMemoryBytes;
        }

        public Builder setModelSizeBytes(long modelSizeBytes) {
            this.modelSizeBytes = modelSizeBytes;
            return this;
        }

        public long getModelSizeBytes() {
            return modelSizeBytes;
        }

        public Builder setRequiredNativeMemoryBytes(long requiredNativeMemoryBytes) {
            this.requiredNativeMemoryBytes = requiredNativeMemoryBytes;
            return this;
        }

        public long getRequiredNativeMemoryBytes() {
            return requiredNativeMemoryBytes;
        }

        public Builder setPerDeploymentMemoryBytes(long perDeploymentMemoryBytes) {
            this.perDeploymentMemoryBytes = perDeploymentMemoryBytes;
            return this;
        }

        public long getPerDeploymentMemoryBytes() {
            return perDeploymentMemoryBytes;
        }

        public Builder setPerAllocationMemoryBytes(long perAllocationMemoryBytes) {
            this.perAllocationMemoryBytes = perAllocationMemoryBytes;
            return this;
        }

        public long getPerAllocationMemoryBytes() {
            return perAllocationMemoryBytes;
        }

        public TrainedModelSizeStats build() {
            return new TrainedModelSizeStats(
                modelSizeBytes == null ? 0 : modelSizeBytes,
                requiredNativeMemoryBytes == null ? 0 : requiredNativeMemoryBytes,
                perDeploymentMemoryBytes == null ? 0 : perDeploymentMemoryBytes,
                perAllocationMemoryBytes == null ? 0 : perAllocationMemoryBytes
            );
        }
    }
}
