/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * ClusterInfo is an object representing a map of nodes to {@link DiskUsage}
 * and a map of shard ids to shard sizes, see
 * <code>InternalClusterInfoService.shardIdentifierFromRouting(String)</code>
 * for the key used in the shardSizes map
 */
public class ClusterInfo implements ToXContentFragment, Writeable {

    public static final Version DATA_SET_SIZE_SIZE_VERSION = Version.V_7_13_0;

    private final Map<String, DiskUsage> leastAvailableSpaceUsage;
    private final Map<String, DiskUsage> mostAvailableSpaceUsage;
    final Map<String, Long> shardSizes;
    final Map<ShardId, Long> shardDataSetSizes;
    public static final ClusterInfo EMPTY = new ClusterInfo();
    final Map<ShardRouting, String> routingToDataPath;
    final Map<NodeAndPath, ReservedSpace> reservedSpace;

    protected ClusterInfo() {
        this(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    /**
     * Creates a new ClusterInfo instance.
     *
     * @param leastAvailableSpaceUsage a node id to disk usage mapping for the path that has the least available space on the node.
     * @param mostAvailableSpaceUsage  a node id to disk usage mapping for the path that has the most available space on the node.
     * @param shardSizes a shardkey to size in bytes mapping per shard.
     * @param shardDataSetSizes a shard id to data set size in bytes mapping per shard
     * @param routingToDataPath the shard routing to datapath mapping
     * @param reservedSpace reserved space per shard broken down by node and data path
     * @see #shardIdentifierFromRouting
     */
    public ClusterInfo(
        Map<String, DiskUsage> leastAvailableSpaceUsage,
        Map<String, DiskUsage> mostAvailableSpaceUsage,
        Map<String, Long> shardSizes,
        Map<ShardId, Long> shardDataSetSizes,
        Map<ShardRouting, String> routingToDataPath,
        Map<NodeAndPath, ReservedSpace> reservedSpace
    ) {
        this.leastAvailableSpaceUsage = Map.copyOf(leastAvailableSpaceUsage);
        this.shardSizes = Map.copyOf(shardSizes);
        this.shardDataSetSizes = Map.copyOf(shardDataSetSizes);
        this.mostAvailableSpaceUsage = Map.copyOf(mostAvailableSpaceUsage);
        this.routingToDataPath = Map.copyOf(routingToDataPath);
        this.reservedSpace = Map.copyOf(reservedSpace);
    }

    public ClusterInfo(StreamInput in) throws IOException {
        this.leastAvailableSpaceUsage = in.readImmutableMap(StreamInput::readString, DiskUsage::new);
        this.mostAvailableSpaceUsage = in.readImmutableMap(StreamInput::readString, DiskUsage::new);
        this.shardSizes = in.readImmutableMap(StreamInput::readString, StreamInput::readLong);
        if (in.getVersion().onOrAfter(DATA_SET_SIZE_SIZE_VERSION)) {
            this.shardDataSetSizes = in.readImmutableMap(ShardId::new, StreamInput::readLong);
        } else {
            this.shardDataSetSizes = Map.of();
        }
        this.routingToDataPath = in.readImmutableMap(ShardRouting::new, StreamInput::readString);
        if (in.getVersion().onOrAfter(StoreStats.RESERVED_BYTES_VERSION)) {
            this.reservedSpace = in.readImmutableMap(NodeAndPath::new, ReservedSpace::new);
        } else {
            this.reservedSpace = Map.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.leastAvailableSpaceUsage, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeMap(this.mostAvailableSpaceUsage, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeMap(this.shardSizes, StreamOutput::writeString, (o, v) -> o.writeLong(v == null ? -1 : v));
        if (out.getVersion().onOrAfter(DATA_SET_SIZE_SIZE_VERSION)) {
            out.writeMap(this.shardDataSetSizes, (o, s) -> s.writeTo(o), StreamOutput::writeLong);
        }
        out.writeMap(this.routingToDataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        if (out.getVersion().onOrAfter(StoreStats.RESERVED_BYTES_VERSION)) {
            out.writeMap(this.reservedSpace);
        }
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        {
            for (Map.Entry<String, DiskUsage> c : this.leastAvailableSpaceUsage.entrySet()) {
                builder.startObject(c.getKey());
                { // node
                    builder.field("node_name", c.getValue().getNodeName());
                    builder.startObject("least_available");
                    {
                        c.getValue().toShortXContent(builder);
                    }
                    builder.endObject(); // end "least_available"
                    builder.startObject("most_available");
                    {
                        DiskUsage most = this.mostAvailableSpaceUsage.get(c.getKey());
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
        builder.startObject("shard_sizes");
        {
            for (Map.Entry<String, Long> c : this.shardSizes.entrySet()) {
                builder.humanReadableField(c.getKey() + "_bytes", c.getKey(), new ByteSizeValue(c.getValue()));
            }
        }
        builder.endObject(); // end "shard_sizes"
        builder.startObject("shard_data_set_sizes");
        {
            for (Map.Entry<ShardId, Long> c : this.shardDataSetSizes.entrySet()) {
                builder.humanReadableField(c.getKey() + "_bytes", c.getKey().toString(), new ByteSizeValue(c.getValue()));
            }
        }
        builder.endObject(); // end "shard_data_set_sizes"
        builder.startObject("shard_paths");
        {
            for (Map.Entry<ShardRouting, String> c : this.routingToDataPath.entrySet()) {
                builder.field(c.getKey().toString(), c.getValue());
            }
        }
        builder.endObject(); // end "shard_paths"
        builder.startArray("reserved_sizes");
        {
            for (Map.Entry<NodeAndPath, ReservedSpace> c : this.reservedSpace.entrySet()) {
                builder.startObject();
                {
                    builder.field("node_id", c.getKey().nodeId);
                    builder.field("path", c.getKey().path);
                    c.getValue().toXContent(builder, params);
                }
                builder.endObject(); // NodeAndPath
            }
        }
        builder.endArray(); // end "reserved_sizes"
        return builder;
    }

    /**
     * Returns a node id to disk usage mapping for the path that has the least available space on the node.
     * Note that this does not take account of reserved space: there may be another path with less available _and unreserved_ space.
     */
    public Map<String, DiskUsage> getNodeLeastAvailableDiskUsages() {
        return this.leastAvailableSpaceUsage;
    }

    /**
     * Returns a node id to disk usage mapping for the path that has the most available space on the node.
     * Note that this does not take account of reserved space: there may be another path with more available _and unreserved_ space.
     */
    public Map<String, DiskUsage> getNodeMostAvailableDiskUsages() {
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

    public Optional<Long> getShardDataSetSize(ShardId shardId) {
        return Optional.ofNullable(shardDataSetSizes.get(shardId));
    }

    /**
     * Returns the reserved space for each shard on the given node/path pair
     */
    public ReservedSpace getReservedSpace(String nodeId, String dataPath) {
        final ReservedSpace result = reservedSpace.get(new NodeAndPath(nodeId, dataPath));
        return result == null ? ReservedSpace.EMPTY : result;
    }

    /**
     * Method that incorporates the ShardId for the shard into a string that
     * includes a 'p' or 'r' depending on whether the shard is a primary.
     */
    public static String shardIdentifierFromRouting(ShardRouting shardRouting) {
        return shardRouting.shardId().toString() + "[" + (shardRouting.primary() ? "p" : "r") + "]";
    }

    /**
     * Represents a data path on a node
     */
    public static class NodeAndPath implements Writeable {
        public final String nodeId;
        public final String path;

        public NodeAndPath(String nodeId, String path) {
            this.nodeId = Objects.requireNonNull(nodeId);
            this.path = Objects.requireNonNull(path);
        }

        public NodeAndPath(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.path = in.readString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeAndPath that = (NodeAndPath) o;
            return nodeId.equals(that.nodeId) && path.equals(that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, path);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeString(path);
        }
    }

    /**
     * Represents the total amount of "reserved" space on a particular data path, together with the set of shards considered.
     */
    public static class ReservedSpace implements Writeable {

        public static final ReservedSpace EMPTY = new ReservedSpace(0, new HashSet<>());

        private final long total;
        private final Set<ShardId> shardIds;

        private ReservedSpace(long total, HashSet<ShardId> shardIds) {
            this.total = total;
            this.shardIds = shardIds;
        }

        ReservedSpace(StreamInput in) throws IOException {
            total = in.readVLong();
            final int shardIdCount = in.readVInt();
            shardIds = Sets.newHashSetWithExpectedSize(shardIdCount);
            for (int i = 0; i < shardIdCount; i++) {
                shardIds.add(new ShardId(in));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(total);
            out.writeCollection(shardIds);
        }

        public long getTotal() {
            return total;
        }

        public boolean containsShardId(ShardId shardId) {
            return shardIds.contains(shardId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReservedSpace that = (ReservedSpace) o;
            return total == that.total && shardIds.equals(that.shardIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(total, shardIds);
        }

        void toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("total", total);
            builder.startArray("shards");
            {
                for (ShardId shardIdCursor : shardIds) {
                    shardIdCursor.toXContent(builder, params);
                }
            }
            builder.endArray(); // end "shards"
        }

        public static class Builder {
            private long total;
            private HashSet<ShardId> shardIds = new HashSet<>();

            public ReservedSpace build() {
                assert shardIds != null : "already built";
                final ReservedSpace reservedSpace = new ReservedSpace(total, shardIds);
                shardIds = null;
                return reservedSpace;
            }

            public Builder add(ShardId shardId, long reservedBytes) {
                assert shardIds != null : "already built";
                assert reservedBytes >= 0 : reservedBytes;
                shardIds.add(shardId);
                total += reservedBytes;
                return this;
            }
        }
    }

}
