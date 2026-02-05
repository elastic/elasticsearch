/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.gpu;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Per-node GPU statistics for the {@code _xpack/usage} response.
 */
public class GpuNodeStats implements ToXContentObject, Writeable {

    private final String type;
    private final long memoryInBytes;
    private final boolean enabled;
    private final long indexBuildCount;

    public GpuNodeStats(String type, long memoryInBytes, boolean enabled, long indexBuildCount) {
        this.type = type;
        this.memoryInBytes = memoryInBytes;
        this.enabled = enabled;
        this.indexBuildCount = indexBuildCount;
    }

    public GpuNodeStats(StreamInput in) throws IOException {
        this.type = in.readOptionalString();
        this.memoryInBytes = in.readVLong();
        this.enabled = in.readBoolean();
        this.indexBuildCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(type);
        out.writeVLong(memoryInBytes);
        out.writeBoolean(enabled);
        out.writeVLong(indexBuildCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        builder.field("memory_in_bytes", memoryInBytes);
        builder.field("enabled", enabled);
        builder.field("index_build_count", indexBuildCount);
        builder.endObject();
        return builder;
    }

    public String getType() {
        return type;
    }

    public long getMemoryInBytes() {
        return memoryInBytes;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public long getIndexBuildCount() {
        return indexBuildCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GpuNodeStats that = (GpuNodeStats) o;
        return memoryInBytes == that.memoryInBytes
            && enabled == that.enabled
            && indexBuildCount == that.indexBuildCount
            && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, memoryInBytes, enabled, indexBuildCount);
    }
}
