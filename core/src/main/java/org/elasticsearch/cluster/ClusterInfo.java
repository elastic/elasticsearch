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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

/**
 * ClusterInfo is an object representing information about nodes and shards in
 * the cluster. This includes a map of nodes to disk usage, primary shards to
 * shard size, and classifications for the relative sizes of indices in the
 * cluster.
 *
 * It is a custom cluster state object. This means it is sent to all nodes in
 * the cluster via the cluster state, however, it is not written to disk since
 * it is dynamically generated.
 */
public class ClusterInfo extends AbstractDiffable<ClusterState.Custom> implements ClusterState.Custom {

    public static final String TYPE = "cluster_info";
    public static final ClusterInfo PROTO = new ClusterInfo();
    
    private final ImmutableMap<String, DiskUsage> usages;
    private final ImmutableMap<ShardId, Long> shardSizes;
    private final IndexClassification indexClassification;

    public ClusterInfo() {
        this.usages = ImmutableMap.of();
        this.shardSizes = ImmutableMap.of();
        this.indexClassification = new IndexClassification(ImmutableMap.<String, IndexSize>of());
    }

    public ClusterInfo(Map<String, DiskUsage> usages, Map<ShardId, Long> shardSizes,
                       IndexClassification indexClassification) {
        this.usages = ImmutableMap.copyOf(usages);
        this.shardSizes = ImmutableMap.copyOf(shardSizes);
        this.indexClassification = indexClassification;
    }

    public ClusterInfo(Map<String, DiskUsage> usages, Map<ShardId, Long> shardSizes,
                       Map<String, IndexSize> indexClassifications) {
        this(usages, shardSizes, new IndexClassification(indexClassifications));
    }

    /**
     * Return a map of node ids to disk usage
     */
    public Map<String, DiskUsage> getNodeDiskUsages() {
        return this.usages;
    }

    /**
     * Return a map of shard id to shard size. Note this is the size of the
     * primary shard.
     */
    public Map<ShardId, Long> getShardSizes() {
        return this.shardSizes;
    }

    /**
     * Return a map of index name to index size classification.
     */
    public Map<String, IndexSize> getIndexClassification() {
        return this.indexClassification.getIndexClassifications();
    }

    public String type() {
        return TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("node_disk_usage");
        for (Map.Entry<String, DiskUsage> entry : usages.entrySet()) {
            builder.startObject(entry.getKey());
            builder.field("free_bytes");
            builder.value(entry.getValue().getFreeBytes());
            builder.field("used_bytes");
            builder.value(entry.getValue().getUsedBytes());
            builder.endObject();
        }
        builder.endObject();
        builder.startObject("shard_size");
        for (Map.Entry<ShardId, Long> entry : shardSizes.entrySet()) {
            builder.field(entry.getKey().toString());
            builder.value(entry.getValue());
        }
        builder.endObject();
        indexClassification.toXContent(builder, params);
        return builder;
    }

    @Override
    public ClusterInfo readFrom(StreamInput in) throws IOException {
        long mapSize = in.readVLong();
        Map<String, DiskUsage> newUsages = Maps.newHashMap();
        for (int i = 0; i < mapSize; i++) {
            String index = in.readString();
            String nodeId = in.readString();
            String nodeName = in.readString();
            long totalBytes = in.readLong();
            long freeBytes = in.readLong();
            DiskUsage usage = new DiskUsage(nodeId, nodeName, totalBytes, freeBytes);
            newUsages.put(index, usage);
        }

        mapSize = in.readVLong();
        Map<ShardId, Long> newSizes = Maps.newHashMap();
        for (int i = 0; i < mapSize; i++) {
            String idx = in.readString();
            int id = in.readVInt();
            long size = in.readLong();
            newSizes.put(new ShardId(idx, id), size);
        }

        IndexClassification newClassifications = new IndexClassification();
        newClassifications.readFrom(in);
        return new ClusterInfo(newUsages, newSizes, newClassifications);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(usages.size());
        for (Map.Entry<String, DiskUsage> entry : usages.entrySet()) {
            String index = entry.getKey();
            DiskUsage usage = entry.getValue();
            out.writeString(index);
            out.writeString(usage.getNodeId());
            out.writeString(usage.getNodeName());
            out.writeLong(usage.getTotalBytes());
            out.writeLong(usage.getFreeBytes());
        }

        out.writeVLong(shardSizes.size());
        for (Map.Entry<ShardId, Long> entry : shardSizes.entrySet()) {
            out.writeString(entry.getKey().getIndex());
            out.writeVInt(entry.getKey().getId());
            out.writeLong(entry.getValue());
        }

        indexClassification.writeTo(out);
    }

    @Override
    public int hashCode() {
        return usages.hashCode() ^
                31 * shardSizes.hashCode() ^
                31 * indexClassification.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof ClusterInfo) {
            ClusterInfo other = (ClusterInfo) obj;
            return usages.equals(other.usages) &&
                    shardSizes.equals(other.shardSizes) &&
                    indexClassification.equals(other.indexClassification);
        } else {
            return false;
        }
    }

    /**
     * An Enumeration representing the classification for the size of an index.
     * {@code SMALLEST} corresponds to the smallest index and {@code LARGEST}
     * corresponds to the largest, with the other sizes falling equidistantly
     * between them.
     */
    public enum IndexSize {
        SMALLEST,
        SMALL,
        MEDIUM,
        LARGE,
        LARGEST
    }
}
