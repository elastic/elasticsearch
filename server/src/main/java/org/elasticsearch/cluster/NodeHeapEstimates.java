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
 * @param totalHeapUsage The total estimated heap usage, including things like index metadata, hosted shards, indexing infrastructure, etc.
 *                       This is only produced by indexing nodes, the search tier doesn't model total heap usage so this will be 0 for them.
 *                       Indexing nodes will always be > 0 because the total usage includes a floor of
 *                       {@code StatelessMemoryMetricsService#WORKLOAD_MEMORY_OVERHEAD}.
 * @param hostedShardsHeapUsage The estimated heap usage attributable to hosted shards only, populated for both indexing and search nodes.
 * @param nonShardHeapUsage Heap usage that does not move with shard placement. For the indexing tier, this includes cluster-wide
 *                          index-metadata overhead, a floor of {@code StatelessMemoryMetricsService#WORKLOAD_MEMORY_OVERHEAD},
 *                          merge memory, and heap reserved for large indexing operations.
 */
public record NodeHeapEstimates(long totalHeapUsage, long hostedShardsHeapUsage, long nonShardHeapUsage) implements Writeable {

    public static final TransportVersion NON_SHARD_HEAP_USAGE = TransportVersion.fromName("non_shard_heap_usage");

    public NodeHeapEstimates {
        assert totalHeapUsage >= 0;
        assert hostedShardsHeapUsage >= 0;
        assert nonShardHeapUsage >= 0;
    }

    public NodeHeapEstimates(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.getTransportVersion().supports(NON_SHARD_HEAP_USAGE) ? in.readVLong() : 0L);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalHeapUsage);
        out.writeVLong(hostedShardsHeapUsage);
        if (out.getTransportVersion().supports(NON_SHARD_HEAP_USAGE)) {
            out.writeVLong(nonShardHeapUsage);
        }
    }
}
