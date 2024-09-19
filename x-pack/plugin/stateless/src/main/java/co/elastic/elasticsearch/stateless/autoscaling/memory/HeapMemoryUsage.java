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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.cluster.ClusterState.UNKNOWN_VERSION;

public record HeapMemoryUsage(long publicationSeqNo, Map<ShardId, ShardMappingSize> shardMappingSizes, long clusterStateVersion)
    implements
        Writeable {

    HeapMemoryUsage(long publicationSeqNo, Map<ShardId, ShardMappingSize> shardMappingSizes) {
        this(publicationSeqNo, shardMappingSizes, UNKNOWN_VERSION);
    }

    public static HeapMemoryUsage from(StreamInput in) throws IOException {
        final Writeable.Reader<ShardId> keyReader;
        if (in.getTransportVersion().onOrAfter(ServerlessTransportVersions.SHARD_FIELD_INFOS)) {
            keyReader = ShardId::new;
        } else {
            keyReader = is -> new ShardId(new Index(is), 0);
        }
        return new HeapMemoryUsage(in.readVLong(), in.readMap(keyReader, ShardMappingSize::from), readClusterStateVersion(in));
    }

    private static long readClusterStateVersion(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(ServerlessTransportVersions.CLUSTER_STATE_VERSION_IN_PUBLISH_MEMORY_METRICS_REQUEST)) {
            return in.readLong();
        }
        return UNKNOWN_VERSION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(publicationSeqNo);
        final Writeable.Writer<ShardId> keyWriter;
        if (out.getTransportVersion().onOrAfter(ServerlessTransportVersions.SHARD_FIELD_INFOS)) {
            keyWriter = (o, v) -> v.writeTo(o);
        } else {
            keyWriter = (o, v) -> v.getIndex().writeTo(o);
        }
        out.writeMap(shardMappingSizes, keyWriter, (o, shard) -> shard.writeTo(o));
        if (out.getTransportVersion().onOrAfter(ServerlessTransportVersions.CLUSTER_STATE_VERSION_IN_PUBLISH_MEMORY_METRICS_REQUEST)) {
            out.writeLong(clusterStateVersion);
        }
    }
}
