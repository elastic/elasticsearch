/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public record ShardMappingSize(
    long mappingSizeInBytes,
    int numSegments,
    int totalFields,
    long postingsInMemoryBytes,
    long liveDocsBytes,
    long pointsInMemoryBytes,
    // Value that can be passed to the master to indicate a more accurate overhead for the shard.
    // Use UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES to indicate that the node doesn't provide this value.
    long shardMemoryOverheadBytes,
    String nodeId
) implements Writeable {

    public static final long UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES = 0L;

    private static final TransportVersion POINTS_IN_SHARD_MAPPING_SIZE = TransportVersion.fromName("points_in_shard_mapping_size");

    public static ShardMappingSize from(StreamInput in) throws IOException {
        return new ShardMappingSize(
            in.readVLong(),
            in.readVInt(),
            in.readVInt(),
            in.readVLong(),
            in.readVLong(),
            in.getTransportVersion().supports(POINTS_IN_SHARD_MAPPING_SIZE) ? in.readVLong() : 0,
            in.readVLong(),
            in.readString()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(mappingSizeInBytes);
        out.writeVInt(numSegments);
        out.writeVInt(totalFields);
        out.writeVLong(postingsInMemoryBytes);
        out.writeVLong(liveDocsBytes);
        if (out.getTransportVersion().supports(POINTS_IN_SHARD_MAPPING_SIZE)) {
            out.writeVLong(pointsInMemoryBytes);
        }
        out.writeVLong(shardMemoryOverheadBytes);
        out.writeString(nodeId);
    }
}
