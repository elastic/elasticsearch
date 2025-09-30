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

package co.elastic.elasticsearch.stateless.autoscaling.memory;

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
    // Value that can be passed to the master to indicate a more accurate overhead for the shard.
    // Use UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES to indicate that the node doesn't provide this value.
    long shardMemoryOverheadBytes,
    String nodeId
) implements Writeable {

    private static final TransportVersion TRACK_LIVE_DOCS_IN_MEMORY_BYTES = TransportVersion.fromName("track_live_docs_in_memory_bytes");
    private static final TransportVersion SHARD_MEMORY_OVERHEAD_IN_SHARD_MAPPING_SIZE = TransportVersion.fromName(
        "shard_memory_overhead_in_shard_mapping_size"
    );

    public static final long UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES = 0L;

    public static ShardMappingSize from(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(SHARD_MEMORY_OVERHEAD_IN_SHARD_MAPPING_SIZE)) {
            return new ShardMappingSize(
                in.readVLong(),
                in.readVInt(),
                in.readVInt(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readString()
            );
        } else if (in.getTransportVersion().supports(TRACK_LIVE_DOCS_IN_MEMORY_BYTES)) {
            return new ShardMappingSize(
                in.readVLong(),
                in.readVInt(),
                in.readVInt(),
                in.readVLong(),
                in.readVLong(),
                UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                in.readString()
            );
        } else {
            return new ShardMappingSize(
                in.readVLong(),
                in.readVInt(),
                in.readVInt(),
                in.readVLong(),
                0L,
                UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                in.readString()
            );
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(mappingSizeInBytes);
        out.writeVInt(numSegments);
        out.writeVInt(totalFields);
        out.writeVLong(postingsInMemoryBytes);
        if (out.getTransportVersion().supports(TRACK_LIVE_DOCS_IN_MEMORY_BYTES)) {
            out.writeVLong(liveDocsBytes);
        }
        if (out.getTransportVersion().supports(SHARD_MEMORY_OVERHEAD_IN_SHARD_MAPPING_SIZE)) {
            out.writeVLong(shardMemoryOverheadBytes);
        }
        out.writeString(nodeId);
    }
}
