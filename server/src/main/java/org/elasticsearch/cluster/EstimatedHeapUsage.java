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
 * Represents an estimate of the heap used by allocated shards and ongoing merges on a particular node
 */
public record EstimatedHeapUsage(String nodeId, long totalBytes, NodeHeapEstimate nodeHeapEstimate) implements Writeable {

    public static final TransportVersion SHARD_HEAP_USAGE_IN_ESTIMATED_HEAP_USAGE = TransportVersion.fromName(
        "shard_heap_usage_in_estimated_heap_usage"
    );

    public EstimatedHeapUsage {
        assert totalBytes >= 0;
        assert nodeHeapEstimate != null;
    }

    public static EstimatedHeapUsage readFrom(StreamInput in) throws IOException {
        final var nodeId = in.readString();
        final var totalBytes = in.readVLong();
        if (in.getTransportVersion().supports(SHARD_HEAP_USAGE_IN_ESTIMATED_HEAP_USAGE)) {
            final var nodeHeapEstimate = new NodeHeapEstimate(in);
            return new EstimatedHeapUsage(nodeId, totalBytes, nodeHeapEstimate);
        } else {
            final long totalHeapUsage = in.readVLong();
            return new EstimatedHeapUsage(nodeId, totalBytes, new NodeHeapEstimate(totalHeapUsage, 0));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeVLong(this.totalBytes);
        if (out.getTransportVersion().supports(SHARD_HEAP_USAGE_IN_ESTIMATED_HEAP_USAGE)) {
            out.writeWriteable(this.nodeHeapEstimate);
        } else {
            out.writeVLong(this.nodeHeapEstimate.totalHeapUsage());
        }
    }

    public long estimatedFreeBytes() {
        return totalBytes - nodeHeapEstimate.totalHeapUsage();
    }

    public double estimatedFreeBytesAsPercentage() {
        return 100.0 - estimatedUsageAsPercentage();
    }

    public double estimatedUsageAsPercentage() {
        return 100.0 * estimatedUsageAsRatio();
    }

    public double estimatedUsageAsRatio() {
        return nodeHeapEstimate.totalHeapUsage() / (double) totalBytes;
    }

    public EstimatedHeapUsage updateEstimatedUsage(long indexMetadataUsageDelta, long hostedShardsUsageDelta) {
        return new EstimatedHeapUsage(
            nodeId,
            totalBytes,
            new NodeHeapEstimate(
                nodeHeapEstimate.totalHeapUsage() + indexMetadataUsageDelta + hostedShardsUsageDelta,
                nodeHeapEstimate.hostedShardsHeapUsage() + hostedShardsUsageDelta
            )
        );
    }
}
