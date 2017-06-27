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
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
/**
 * ClusterInfo is an object representing a map of nodes to {@link DiskUsage}
 * and a map of shard ids to shard sizes, see
 * <code>InternalClusterInfoService.shardIdentifierFromRouting(String)</code>
 * for the key used in the shardSizes map
 */
public class ClusterInfo implements ToXContent, Writeable {
    private final ImmutableOpenMap<String, DiskUsage> leastAvailableSpaceUsage;
    private final ImmutableOpenMap<String, DiskUsage> mostAvailableSpaceUsage;
    final ImmutableOpenMap<String, Long> shardSizes;
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
     * @see #shardIdentifierFromRouting
     */
    public ClusterInfo(ImmutableOpenMap<String, DiskUsage> leastAvailableSpaceUsage,
            ImmutableOpenMap<String, DiskUsage> mostAvailableSpaceUsage, ImmutableOpenMap<String, Long> shardSizes,
            ImmutableOpenMap<ShardRouting, String> routingToDataPath) {
        this.leastAvailableSpaceUsage = leastAvailableSpaceUsage;
        this.shardSizes = shardSizes;
        this.mostAvailableSpaceUsage = mostAvailableSpaceUsage;
        this.routingToDataPath = routingToDataPath;
    }

    public ClusterInfo(StreamInput in) throws IOException {
        Map<String, DiskUsage> leastMap = in.readMap(StreamInput::readString, DiskUsage::new);
        Map<String, DiskUsage> mostMap = in.readMap(StreamInput::readString, DiskUsage::new);
        Map<String, Long> sizeMap = in.readMap(StreamInput::readString, StreamInput::readLong);
        Map<ShardRouting, String> routingMap = in.readMap(ShardRouting::new, StreamInput::readString);

        ImmutableOpenMap.Builder<String, DiskUsage> leastBuilder = ImmutableOpenMap.builder();
        this.leastAvailableSpaceUsage = leastBuilder.putAll(leastMap).build();
        ImmutableOpenMap.Builder<String, DiskUsage> mostBuilder = ImmutableOpenMap.builder();
        this.mostAvailableSpaceUsage = mostBuilder.putAll(mostMap).build();
        ImmutableOpenMap.Builder<String, Long> sizeBuilder = ImmutableOpenMap.builder();
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
        for (ObjectObjectCursor<String, Long> c : this.shardSizes) {
            out.writeString(c.key);
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
                        c.value.toShortXContent(builder, params);
                    }
                    builder.endObject(); // end "least_available"
                    builder.startObject("most_available"); {
                        DiskUsage most = this.mostAvailableSpaceUsage.get(c.key);
                        if (most != null) {
                            most.toShortXContent(builder, params);
                        }
                    }
                    builder.endObject(); // end "most_available"
                }
                builder.endObject(); // end $nodename
            }
        }
        builder.endObject(); // end "nodes"
        builder.startObject("shard_sizes"); {
            for (ObjectObjectCursor<String, Long> c : this.shardSizes) {
                builder.byteSizeField(c.key + "_bytes", c.key, c.value);
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
        return shardSizes.get(shardIdentifierFromRouting(shardRouting));
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

    /**
     * Method that incorporates the ShardId for the shard into a string that
     * includes a 'p' or 'r' depending on whether the shard is a primary.
     */
    static String shardIdentifierFromRouting(ShardRouting shardRouting) {
        return shardRouting.shardId().toString() + "[" + (shardRouting.primary() ? "p" : "r") + "]";
    }
}
