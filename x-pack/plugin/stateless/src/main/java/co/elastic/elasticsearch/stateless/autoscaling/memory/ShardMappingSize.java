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

import co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions;

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
    String nodeId
) implements Writeable {

    public static ShardMappingSize from(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(ServerlessTransportVersions.TRACK_LIVE_DOCS_IN_MEMORY_BYTES)) {
            return new ShardMappingSize(in.readVLong(), in.readVInt(), in.readVInt(), in.readVLong(), in.readVLong(), in.readString());
        } else {
            return new ShardMappingSize(in.readVLong(), in.readVInt(), in.readVInt(), in.readVLong(), 0L, in.readString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(mappingSizeInBytes);
        out.writeVInt(numSegments);
        out.writeVInt(totalFields);
        out.writeVLong(postingsInMemoryBytes);
        if (out.getTransportVersion().onOrAfter(ServerlessTransportVersions.TRACK_LIVE_DOCS_IN_MEMORY_BYTES)) {
            out.writeVLong(liveDocsBytes);
        }
        out.writeString(nodeId);
    }
}
