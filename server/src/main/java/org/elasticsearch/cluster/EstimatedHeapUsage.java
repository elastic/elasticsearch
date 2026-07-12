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
public record EstimatedHeapUsage(String nodeId, long totalBytes, NodeHeapEstimate estimatedUsageBytes) implements Writeable {

    public static final TransportVersion SHARD_HEAP_USAGE_IN_ESTIMATED_HEAP_USAGE = TransportVersion.fromName(
        "shard_heap_usage_in_estimated_heap_usage"
    );

    public EstimatedHeapUsage {
        assert totalBytes >= 0;
        assert estimatedUsageBytes != null;
    }

    public static EstimatedHeapUsage readFrom(StreamInput in) throws IOException {
        final var nodeId = in.readString();
        final var totalBytes = in.readVLong();
        final var totalHeapUsage = in.readVLong();
        if (in.getTransportVersion().supports(SHARD_HEAP_USAGE_IN_ESTIMATED_HEAP_USAGE)) {
            final var shardHeapUsage = in.readVLong();
            return new EstimatedHeapUsage(nodeId, totalBytes, new NodeHeapEstimate(totalHeapUsage, shardHeapUsage));
        } else {
            return new EstimatedHeapUsage(nodeId, totalBytes, new NodeHeapEstimate(totalHeapUsage, 0));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeVLong(this.totalBytes);
        out.writeVLong(this.estimatedUsageBytes.totalHeapUsage());
        if (out.getTransportVersion().supports(SHARD_HEAP_USAGE_IN_ESTIMATED_HEAP_USAGE)) {
            out.writeVLong(this.estimatedUsageBytes.shardsOnlyHeapUsage());
        }
    }

    public long estimatedFreeBytes() {
        return totalBytes - estimatedUsageBytes.totalHeapUsage();
    }

    public double estimatedFreeBytesAsPercentage() {
        return 100.0 - estimatedUsageAsPercentage();
    }

    public double estimatedUsageAsPercentage() {
        return 100.0 * estimatedUsageAsRatio();
    }

    public double estimatedUsageAsRatio() {
        return estimatedUsageBytes.totalHeapUsage() / (double) totalBytes;
    }

    public EstimatedHeapUsage updateEstimatedUsage(long indexUsageDelta, long shardUsageDelta) {
        return new EstimatedHeapUsage(
            nodeId,
            totalBytes,
            new NodeHeapEstimate(
                estimatedUsageBytes.totalHeapUsage() + indexUsageDelta + shardUsageDelta,
                estimatedUsageBytes.shardsOnlyHeapUsage() + shardUsageDelta
            )
        );
    }
}
