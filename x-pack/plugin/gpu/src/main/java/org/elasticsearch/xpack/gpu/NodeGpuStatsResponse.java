/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodeGpuStatsResponse extends BaseNodeResponse {

    private final boolean gpuSupported;
    private final boolean gpuSettingEnabled;
    private final long gpuUsageCount;
    private final long totalGpuMemoryInBytes;
    private final String gpuName;

    public NodeGpuStatsResponse(
        DiscoveryNode node,
        boolean gpuSupported,
        boolean gpuSettingEnabled,
        long gpuUsageCount,
        long totalGpuMemoryInBytes,
        String gpuName
    ) {
        super(node);
        this.gpuSupported = gpuSupported;
        this.gpuSettingEnabled = gpuSettingEnabled;
        this.gpuUsageCount = gpuUsageCount;
        this.totalGpuMemoryInBytes = totalGpuMemoryInBytes;
        this.gpuName = gpuName;
    }

    public NodeGpuStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.gpuSupported = in.readBoolean();
        this.gpuSettingEnabled = in.readBoolean();
        this.gpuUsageCount = in.readVLong();
        this.totalGpuMemoryInBytes = in.readVLong();
        this.gpuName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(gpuSupported);
        out.writeBoolean(gpuSettingEnabled);
        out.writeVLong(gpuUsageCount);
        out.writeVLong(totalGpuMemoryInBytes);
        out.writeOptionalString(gpuName);
    }

    public boolean isGpuSupported() {
        return gpuSupported;
    }

    /**
     * Returns whether GPU indexing is enabled on this node.
     * True if GPU hardware is available and the node hasn't explicitly disabled it
     * via {@code vectors.indexing.use_gpu = false}.
     */
    public boolean isGpuSettingEnabled() {
        return gpuSettingEnabled;
    }

    public long getGpuUsageCount() {
        return gpuUsageCount;
    }

    public long getTotalGpuMemoryInBytes() {
        return totalGpuMemoryInBytes;
    }

    public String getGpuName() {
        return gpuName;
    }
}
