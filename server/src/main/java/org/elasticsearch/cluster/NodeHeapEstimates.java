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
 * The estimated heap in use by a node
 *
 * @param totalHeapUsage The total estimated heap usage. When calculated by {@link NodeHeapUsageCalculator}, this is
 *                       {@code nonShardHeapUsage + shardHeapUsage + indexHeapUsage + maxPostingsHeapUsage}.
 * @param hostedShardsHeapUsage The estimated heap usage attributable to hosted shards only
 * @param nonShardHeapUsage The total estimated heap usage that is not derived from the node's hosted shard allocation
 */
public record NodeHeapEstimates(long totalHeapUsage, long hostedShardsHeapUsage, long nonShardHeapUsage) implements Writeable {

    public static final TransportVersion EXPLICIT_HEAP_ESTIMATE_COMPONENTS = TransportVersion.fromName("explicit_heap_estimate_components");
    private static final long UNKNOWN_NON_SHARD_HEAP_USAGE = -1L;

    public NodeHeapEstimates(long totalHeapUsage, long hostedShardsHeapUsage) {
        this(totalHeapUsage, hostedShardsHeapUsage, Math.max(0, totalHeapUsage - hostedShardsHeapUsage));
    }

    public NodeHeapEstimates {
        if (nonShardHeapUsage == UNKNOWN_NON_SHARD_HEAP_USAGE) {
            nonShardHeapUsage = Math.max(0, totalHeapUsage - hostedShardsHeapUsage);
        }
        assert totalHeapUsage >= 0;
        assert hostedShardsHeapUsage >= 0;
        assert nonShardHeapUsage >= 0;
    }

    public NodeHeapEstimates(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            in.readVLong(),
            in.getTransportVersion().supports(EXPLICIT_HEAP_ESTIMATE_COMPONENTS) ? in.readVLong() : UNKNOWN_NON_SHARD_HEAP_USAGE
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalHeapUsage);
        out.writeVLong(hostedShardsHeapUsage);
        if (out.getTransportVersion().supports(EXPLICIT_HEAP_ESTIMATE_COMPONENTS)) {
            out.writeVLong(nonShardHeapUsage);
        }
    }
}
