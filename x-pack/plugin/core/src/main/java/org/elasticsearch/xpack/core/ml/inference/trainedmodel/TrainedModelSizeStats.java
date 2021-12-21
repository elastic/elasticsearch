/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class TrainedModelSizeStats implements ToXContentObject, Writeable {

    private static final ParseField MODEL_SIZE_BYTES = new ParseField("model_size_bytes");
    private static final ParseField REQUIRED_NATIVE_MEMORY_BYTES = new ParseField("required_native_memory_bytes");

    private final long modelSizeBytes;
    private final long requiredNativeMemoryBytes;

    public TrainedModelSizeStats(long modelSizeBytes, long requiredNativeMemoryBytes) {
        this.modelSizeBytes = modelSizeBytes;
        this.requiredNativeMemoryBytes = requiredNativeMemoryBytes;
    }

    public TrainedModelSizeStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_8_1_0)) {
            modelSizeBytes = in.readLong();
            requiredNativeMemoryBytes = in.readLong();
        } else {
            modelSizeBytes = 0;
            requiredNativeMemoryBytes = 0;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_8_1_0)) {
            out.writeLong(modelSizeBytes);
            out.writeLong(requiredNativeMemoryBytes);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_SIZE_BYTES.getPreferredName(), modelSizeBytes);
        builder.field(REQUIRED_NATIVE_MEMORY_BYTES.getPreferredName(), requiredNativeMemoryBytes);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TrainedModelSizeStats that = (TrainedModelSizeStats) o;
        return modelSizeBytes == that.modelSizeBytes && requiredNativeMemoryBytes == that.requiredNativeMemoryBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelSizeBytes, requiredNativeMemoryBytes);
    }
}
