/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * ClusterInfo is an object representing a map of nodes to {@link DiskUsage}
 * and a map of shard ids to shard sizes.
 */
public class ClusterInfo implements ToXContentFragment, Writeable {

    /**
     * Only needed to support shard sizes keyed by String as in {@link Version#V_7_0_0} - TODO remove this in v9.
     */
    private static final String BWC_SHARD_ID_UUID = "_bwc_shard_id_uuid_";

    private final ImmutableOpenMap<String, DiskUsage> leastAvailableSpaceUsage;
    private final ImmutableOpenMap<String, DiskUsage> mostAvailableSpaceUsage;
    final ImmutableOpenMap<ShardId, Long> shardSizes;
    public static final ClusterInfo EMPTY = new ClusterInfo();
    final ImmutableOpenMap<ShardRouting, String> routingToDataPath;

    protected ClusterInfo() {
       this(ImmutableOpenMap.of(), ImmutableOpenMap.of(), ImmutableOpenMap.of(), ImmutableOpenMap.of());
    }

    /**
     * Creates a new ClusterInfo instance.
     *
     * @param leastAvailableSpaceUsage a node id to disk usage mapping for the path that has the least available space on the node.
     * @param mostAvailableSpaceUsage  a node id to disk usage mapping for the path that has the most available space on the node.
     * @param shardSizes a shardkey to size in bytes mapping per shard.
     * @param routingToDataPath the shard routing to datapath mapping
     */
    public ClusterInfo(ImmutableOpenMap<String, DiskUsage> leastAvailableSpaceUsage,
            ImmutableOpenMap<String, DiskUsage> mostAvailableSpaceUsage, ImmutableOpenMap<ShardId, Long> shardSizes,
            ImmutableOpenMap<ShardRouting, String> routingToDataPath) {
        this.leastAvailableSpaceUsage = leastAvailableSpaceUsage;
        this.shardSizes = shardSizes;
        this.mostAvailableSpaceUsage = mostAvailableSpaceUsage;
        this.routingToDataPath = routingToDataPath;
    }

    private static ShardId readShardId(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            return new ShardId(in);
        } else {
            // earlier versions used a string of the form "[indexname][0][p]" or "[indexname][0][r]" to distinguish primaries from replicas
            // but the only time we use a serialized ClusterInfo is for allocation explanation, not for allocation calculations, so we can
            // use a placeholder with a fake UUID and shard number and use the index name field for the original string:
            final String shardKey = in.readString();
            return new ShardId(shardKey, BWC_SHARD_ID_UUID, 0);
        }
    }

    public ClusterInfo(StreamInput in) throws IOException {
        Map<String, DiskUsage> leastMap = in.readMap(StreamInput::readString, DiskUsage::new);
        Map<String, DiskUsage> mostMap = in.readMap(StreamInput::readString, DiskUsage::new);
        Map<ShardId, Long> sizeMap = in.readMap(ClusterInfo::readShardId, StreamInput::readLong);
        Map<ShardRouting, String> routingMap = in.readMap(ShardRouting::new, StreamInput::readString);

        ImmutableOpenMap.Builder<String, DiskUsage> leastBuilder = ImmutableOpenMap.builder();
        this.leastAvailableSpaceUsage = leastBuilder.putAll(leastMap).build();
        ImmutableOpenMap.Builder<String, DiskUsage> mostBuilder = ImmutableOpenMap.builder();
        this.mostAvailableSpaceUsage = mostBuilder.putAll(mostMap).build();
        ImmutableOpenMap.Builder<ShardId, Long> sizeBuilder = ImmutableOpenMap.builder();
        this.shardSizes = sizeBuilder.putAll(sizeMap).build();
        ImmutableOpenMap.Builder<ShardRouting, String> routingBuilder = ImmutableOpenMap.builder();
        this.routingToDataPath = routingBuilder.putAll(routingMap).build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.leastAvailableSpaceUsage.size());
        for (ObjectObjectCursor<String, DiskUsage> c : this.leastAvailableSpaceUsage) {
            out.writeString(c.key);
            c.value.writeTo(out);
        }
        out.writeVInt(this.mostAvailableSpaceUsage.size());
        for (ObjectObjectCursor<String, DiskUsage> c : this.mostAvailableSpaceUsage) {
            out.writeString(c.key);
            c.value.writeTo(out);
        }
        out.writeVInt(this.shardSizes.size());
        for (ObjectObjectCursor<ShardId, Long> c : this.shardSizes) {
            if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
                c.key.writeTo(out);
            } else {
                out.writeString(c.key.toString());
            }
            if (c.value == null) {
                out.writeLong(-1);
            } else {
                out.writeLong(c.value);
            }
        }
        out.writeVInt(this.routingToDataPath.size());
        for (ObjectObjectCursor<ShardRouting, String> c : this.routingToDataPath) {
            c.key.writeTo(out);
            out.writeString(c.value);
        }
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes"); {
            for (ObjectObjectCursor<String, DiskUsage> c : this.leastAvailableSpaceUsage) {
                builder.startObject(c.key); { // node
                    builder.field("node_name", c.value.getNodeName());
                    builder.startObject("least_available"); {
                        c.value.toShortXContent(builder);
                    }
                    builder.endObject(); // end "least_available"
                    builder.startObject("most_available"); {
                        DiskUsage most = this.mostAvailableSpaceUsage.get(c.key);
                        if (most != null) {
                            most.toShortXContent(builder);
                        }
                    }
                    builder.endObject(); // end "most_available"
                }
                builder.endObject(); // end $nodename
            }
        }
        builder.endObject(); // end "nodes"
        builder.startObject("shard_sizes"); {
            for (ObjectObjectCursor<ShardId, Long> c : this.shardSizes) {
                final String shardKey;
                if (BWC_SHARD_ID_UUID.equals(c.key.getIndex().getUUID())) {
                    shardKey = c.key.getIndexName();
                } else {
                    shardKey = c.key.toString();
                }
                builder.humanReadableField(shardKey + "_bytes", shardKey, new ByteSizeValue(c.value));
            }
        }
        builder.endObject(); // end "shard_sizes"
        builder.startObject("shard_paths"); {
            for (ObjectObjectCursor<ShardRouting, String> c : this.routingToDataPath) {
                builder.field(c.key.toString(), c.value);
            }
        }
        builder.endObject(); // end "shard_paths"
        return builder;
    }

    /**
     * Returns a node id to disk usage mapping for the path that has the least available space on the node.
     */
    public ImmutableOpenMap<String, DiskUsage> getNodeLeastAvailableDiskUsages() {
        return this.leastAvailableSpaceUsage;
    }

    /**
     * Returns a node id to disk usage mapping for the path that has the most available space on the node.
     */
    public ImmutableOpenMap<String, DiskUsage> getNodeMostAvailableDiskUsages() {
        return this.mostAvailableSpaceUsage;
    }

    /**
     * Returns the shard size for the given shard routing or <code>null</code> it that metric is not available.
     */
    public Long getShardSize(ShardRouting shardRouting) {
        // this ClusterInfo instance was never serialized and sent over the wire, so it does not have any fake shard IDs.
        assert StreamSupport.stream(shardSizes.keys().spliterator(), false)
            .noneMatch(c -> BWC_SHARD_ID_UUID.equals(c.value.getIndex().getUUID())) : shardSizes;
        return shardSizes.get(shardRouting.shardId());
    }

    /**
     * Returns the nodes absolute data-path the given shard is allocated on or <code>null</code> if the information is not available.
     */
    public String getDataPath(ShardRouting shardRouting) {
        return routingToDataPath.get(shardRouting);
    }

    /**
     * Returns the shard size for the given shard routing or <code>defaultValue</code> it that metric is not available.
     */
    public long getShardSize(ShardRouting shardRouting, long defaultValue) {
        Long shardSize = getShardSize(shardRouting);
        return shardSize == null ? defaultValue : shardSize;
    }

}
