/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
