/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class TrainedModelSizeStats implements ToXContentObject, Writeable {

    static final TransportVersion RUNTIME_NATIVE_MEMORY_STATS = TransportVersion.fromName("ml_runtime_native_memory_stats");

    private static final ParseField MODEL_SIZE_BYTES = new ParseField("model_size_bytes");
    private static final ParseField REQUIRED_NATIVE_MEMORY_BYTES = new ParseField("required_native_memory_bytes");
    private static final ParseField RUNTIME_NATIVE_MEMORY_BYTES = new ParseField("runtime_native_memory_bytes");
    private static final ParseField PEAK_RUNTIME_NATIVE_MEMORY_BYTES = new ParseField("peak_runtime_native_memory_bytes");

    private final long modelSizeBytes;
    private final long requiredNativeMemoryBytes;
    /**
     * The actual, OS-reported native memory (resident set size) currently used by the deployed model's native
     * process, aggregated across all of its allocations. {@code 0} when the model is not deployed or the native
     * process has not yet reported any memory use. Unlike {@link #requiredNativeMemoryBytes} (an a priori estimate)
     * this reflects measured runtime memory.
     */
    private final long runtimeNativeMemoryBytes;
    /**
     * The peak {@link #runtimeNativeMemoryBytes} observed since the deployment started. {@code 0} when unavailable.
     */
    private final long peakRuntimeNativeMemoryBytes;

    public TrainedModelSizeStats(long modelSizeBytes, long requiredNativeMemoryBytes) {
        this(modelSizeBytes, requiredNativeMemoryBytes, 0L, 0L);
    }

    public TrainedModelSizeStats(
        long modelSizeBytes,
        long requiredNativeMemoryBytes,
        long runtimeNativeMemoryBytes,
        long peakRuntimeNativeMemoryBytes
    ) {
        this.modelSizeBytes = modelSizeBytes;
        this.requiredNativeMemoryBytes = requiredNativeMemoryBytes;
        this.runtimeNativeMemoryBytes = runtimeNativeMemoryBytes;
        this.peakRuntimeNativeMemoryBytes = peakRuntimeNativeMemoryBytes;
    }

    public TrainedModelSizeStats(StreamInput in) throws IOException {
        modelSizeBytes = in.readLong();
        requiredNativeMemoryBytes = in.readLong();
        if (in.getTransportVersion().supports(RUNTIME_NATIVE_MEMORY_STATS)) {
            runtimeNativeMemoryBytes = in.readVLong();
            peakRuntimeNativeMemoryBytes = in.readVLong();
        } else {
            runtimeNativeMemoryBytes = 0L;
            peakRuntimeNativeMemoryBytes = 0L;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(modelSizeBytes);
        out.writeLong(requiredNativeMemoryBytes);
        if (out.getTransportVersion().supports(RUNTIME_NATIVE_MEMORY_STATS)) {
            out.writeVLong(runtimeNativeMemoryBytes);
            out.writeVLong(peakRuntimeNativeMemoryBytes);
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
        // Only surface the runtime memory once the native process has reported it, to avoid implying a deployed
        // model is using no memory when we simply have not received a measurement yet.
        if (runtimeNativeMemoryBytes > 0) {
            builder.humanReadableField(
                RUNTIME_NATIVE_MEMORY_BYTES.getPreferredName(),
                "runtime_native_memory",
                ByteSizeValue.ofBytes(runtimeNativeMemoryBytes)
            );
        }
        if (peakRuntimeNativeMemoryBytes > 0) {
            builder.humanReadableField(
                PEAK_RUNTIME_NATIVE_MEMORY_BYTES.getPreferredName(),
                "peak_runtime_native_memory",
                ByteSizeValue.ofBytes(peakRuntimeNativeMemoryBytes)
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
        return modelSizeBytes == that.modelSizeBytes
            && requiredNativeMemoryBytes == that.requiredNativeMemoryBytes
            && runtimeNativeMemoryBytes == that.runtimeNativeMemoryBytes
            && peakRuntimeNativeMemoryBytes == that.peakRuntimeNativeMemoryBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelSizeBytes, requiredNativeMemoryBytes, runtimeNativeMemoryBytes, peakRuntimeNativeMemoryBytes);
    }

    public long getModelSizeBytes() {
        return modelSizeBytes;
    }

    public long getRequiredNativeMemoryBytes() {
        return requiredNativeMemoryBytes;
    }

    public long getRuntimeNativeMemoryBytes() {
        return runtimeNativeMemoryBytes;
    }

    public long getPeakRuntimeNativeMemoryBytes() {
        return peakRuntimeNativeMemoryBytes;
    }
}
