/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.gpu;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GpuVectorIndexingFeatureSetUsage extends XPackFeatureUsage {

    private static final TransportVersion GPU_VECTOR_INDEXING_TELEMETRY = TransportVersion.fromName("gpu_vector_indexing_telemetry");

    private final long indexBuildCount;
    private final int nodesWithGpu;
    private final List<GpuNodeStats> nodes;

    public GpuVectorIndexingFeatureSetUsage(
        boolean available,
        boolean enabled,
        long indexBuildCount,
        int nodesWithGpu,
        List<GpuNodeStats> nodes
    ) {
        super(XPackField.GPU_VECTOR_INDEXING, available, enabled);
        this.indexBuildCount = indexBuildCount;
        this.nodesWithGpu = nodesWithGpu;
        this.nodes = nodes;
    }

    public GpuVectorIndexingFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.indexBuildCount = in.readVLong();
        this.nodesWithGpu = in.readVInt();
        this.nodes = in.readCollectionAsList(GpuNodeStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(indexBuildCount);
        out.writeVInt(nodesWithGpu);
        out.writeCollection(nodes);
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("index_build_count", indexBuildCount);
        builder.field("nodes_with_gpu", nodesWithGpu);
        builder.xContentList("nodes", nodes);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return GPU_VECTOR_INDEXING_TELEMETRY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        GpuVectorIndexingFeatureSetUsage that = (GpuVectorIndexingFeatureSetUsage) o;
        return indexBuildCount == that.indexBuildCount && nodesWithGpu == that.nodesWithGpu && Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), indexBuildCount, nodesWithGpu, nodes);
    }
}
