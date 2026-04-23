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

import java.io.IOException;

public record ShardMappingSize(
    long mappingSizeInBytes,
    int numSegments,
    int totalFields,
    long postingsInMemoryBytes,
    long liveDocsBytes,
    // Value that can be passed to the master to indicate a more accurate overhead for the shard.
    // Use UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES to indicate that the node doesn't provide this value.
    long shardMemoryOverheadBytes,
    String nodeId
) implements Writeable {

    public static final long UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES = 0L;

    public static ShardMappingSize from(StreamInput in) throws IOException {
        return new ShardMappingSize(
            in.readVLong(),
            in.readVInt(),
            in.readVInt(),
            in.readVLong(),
            in.readVLong(),
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
        out.writeVLong(shardMemoryOverheadBytes);
        out.writeString(nodeId);
    }
}
