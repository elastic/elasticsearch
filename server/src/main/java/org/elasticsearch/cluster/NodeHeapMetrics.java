/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Represents metrics related to heap capacity and usage for a particular node
 */
public record NodeHeapMetrics(String nodeId, long totalBytes, NodeHeapEstimates nodeHeapEstimates) implements Writeable {

    public static final TransportVersion SHARD_HEAP_USAGE_IN_ESTIMATED_HEAP_USAGE = TransportVersion.fromName(
        "shard_heap_usage_in_estimated_heap_usage"
    );

    public NodeHeapMetrics {
        assert totalBytes >= 0;
        assert nodeHeapEstimates != null;
    }

    public static NodeHeapMetrics readFrom(StreamInput in) throws IOException {
        final var nodeId = in.readString();
        final var totalBytes = in.readVLong();
        if (in.getTransportVersion().supports(SHARD_HEAP_USAGE_IN_ESTIMATED_HEAP_USAGE)) {
            final var nodeHeapEstimate = new NodeHeapEstimates(in);
            return new NodeHeapMetrics(nodeId, totalBytes, nodeHeapEstimate);
        } else {
            final long totalHeapUsage = in.readVLong();
            return new NodeHeapMetrics(nodeId, totalBytes, new NodeHeapEstimates(totalHeapUsage, 0));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeVLong(this.totalBytes);
        if (out.getTransportVersion().supports(SHARD_HEAP_USAGE_IN_ESTIMATED_HEAP_USAGE)) {
            out.writeWriteable(this.nodeHeapEstimates);
        } else {
            out.writeVLong(this.nodeHeapEstimates.totalHeapUsage());
        }
    }

    public long estimatedFreeBytes() {
        return totalBytes - nodeHeapEstimates.totalHeapUsage();
    }

    public double estimatedFreeBytesAsPercentage() {
        return 100.0 - estimatedUsageAsPercentage();
    }

    public double estimatedUsageAsPercentage() {
        return 100.0 * estimatedUsageAsRatio();
    }

    public double estimatedUsageAsRatio() {
        return nodeHeapEstimates.totalHeapUsage() / (double) totalBytes;
    }

    public NodeHeapMetrics updateEstimatedUsage(long indexMetadataUsageDelta, long hostedShardsUsageDelta) {
        return new NodeHeapMetrics(
            nodeId,
            totalBytes,
            new NodeHeapEstimates(
                Math.addExact(Math.addExact(nodeHeapEstimates.totalHeapUsage(), indexMetadataUsageDelta), hostedShardsUsageDelta),
                Math.addExact(nodeHeapEstimates.hostedShardsHeapUsage(), hostedShardsUsageDelta)
            )
        );
    }
}
