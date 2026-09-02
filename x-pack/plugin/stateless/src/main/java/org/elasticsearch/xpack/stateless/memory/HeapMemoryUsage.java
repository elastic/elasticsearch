/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.cluster.ClusterState.UNKNOWN_VERSION;

public record HeapMemoryUsage(long publicationSeqNo, Map<ShardId, ShardMappingSize> shardMappingSizes, long clusterStateVersion)
    implements
        Writeable {

    public HeapMemoryUsage(long publicationSeqNo, Map<ShardId, ShardMappingSize> shardMappingSizes) {
        this(publicationSeqNo, shardMappingSizes, UNKNOWN_VERSION);
    }

    public static HeapMemoryUsage from(StreamInput in) throws IOException {
        return new HeapMemoryUsage(in.readVLong(), in.readMap(ShardId::new, ShardMappingSize::from), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(publicationSeqNo);
        out.writeMap(shardMappingSizes, StreamOutput::writeWriteable, StreamOutput::writeWriteable);
        out.writeLong(clusterStateVersion);
    }
}
