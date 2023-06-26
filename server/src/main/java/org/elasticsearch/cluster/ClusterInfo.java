/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
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

import static org.elasticsearch.cluster.routing.ShardRouting.newUnassigned;
import static org.elasticsearch.cluster.routing.UnassignedInfo.Reason.REINITIALIZED;

/**
 * ClusterInfo is an object representing a map of nodes to {@link DiskUsage}
 * and a map of shard ids to shard sizes, see
 * <code>InternalClusterInfoService.shardIdentifierFromRouting(String)</code>
 * for the key used in the shardSizes map
 */
public class ClusterInfo implements ToXContentFragment, Writeable {

    public static final ClusterInfo EMPTY = new ClusterInfo();

    public static final TransportVersion DATA_SET_SIZE_SIZE_VERSION = TransportVersion.V_7_13_0;
    public static final TransportVersion DATA_PATH_NEW_KEY_VERSION = TransportVersion.V_8_6_0;

    private final Map<String, DiskUsage> leastAvailableSpaceUsage;
    private final Map<String, DiskUsage> mostAvailableSpaceUsage;
    final Map<String, Long> shardSizes;
    final Map<ShardId, Long> shardDataSetSizes;
    final Map<NodeAndShard, String> dataPath;
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
     * @param dataPath the shard routing to datapath mapping
     * @param reservedSpace reserved space per shard broken down by node and data path
     * @see #shardIdentifierFromRouting
     */
    public ClusterInfo(
        Map<String, DiskUsage> leastAvailableSpaceUsage,
        Map<String, DiskUsage> mostAvailableSpaceUsage,
        Map<String, Long> shardSizes,
        Map<ShardId, Long> shardDataSetSizes,
        Map<NodeAndShard, String> dataPath,
        Map<NodeAndPath, ReservedSpace> reservedSpace
    ) {
        this.leastAvailableSpaceUsage = Map.copyOf(leastAvailableSpaceUsage);
        this.mostAvailableSpaceUsage = Map.copyOf(mostAvailableSpaceUsage);
        this.shardSizes = Map.copyOf(shardSizes);
        this.shardDataSetSizes = Map.copyOf(shardDataSetSizes);
        this.dataPath = Map.copyOf(dataPath);
        this.reservedSpace = Map.copyOf(reservedSpace);
    }

    public ClusterInfo(StreamInput in) throws IOException {
        this.leastAvailableSpaceUsage = in.readImmutableMap(DiskUsage::new);
        this.mostAvailableSpaceUsage = in.readImmutableMap(DiskUsage::new);
        this.shardSizes = in.readImmutableMap(StreamInput::readLong);
        this.shardDataSetSizes = in.getTransportVersion().onOrAfter(DATA_SET_SIZE_SIZE_VERSION)
            ? in.readImmutableMap(ShardId::new, StreamInput::readLong)
            : Map.of();
        this.dataPath = in.getTransportVersion().onOrAfter(DATA_PATH_NEW_KEY_VERSION)
            ? in.readImmutableMap(NodeAndShard::new, StreamInput::readString)
            : in.readImmutableMap(nested -> NodeAndShard.from(new ShardRouting(nested)), StreamInput::readString);
        this.reservedSpace = in.getTransportVersion().onOrAfter(StoreStats.RESERVED_BYTES_VERSION)
            ? in.readImmutableMap(NodeAndPath::new, ReservedSpace::new)
            : Map.of();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.leastAvailableSpaceUsage, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeMap(this.mostAvailableSpaceUsage, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeMap(this.shardSizes, StreamOutput::writeString, (o, v) -> o.writeLong(v == null ? -1 : v));
        if (out.getTransportVersion().onOrAfter(DATA_SET_SIZE_SIZE_VERSION)) {
            out.writeMap(this.shardDataSetSizes, (o, s) -> s.writeTo(o), StreamOutput::writeLong);
        }
        if (out.getTransportVersion().onOrAfter(DATA_PATH_NEW_KEY_VERSION)) {
            out.writeMap(this.dataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        } else {
            out.writeMap(this.dataPath, (o, k) -> createFakeShardRoutingFromNodeAndShard(k).writeTo(o), StreamOutput::writeString);
        }
        if (out.getTransportVersion().onOrAfter(StoreStats.RESERVED_BYTES_VERSION)) {
            out.writeMap(this.reservedSpace);
        }
    }

    /**
     * This creates a fake ShardRouting from limited info available in NodeAndShard.
     * This will not be the same as real shard, however this is fine as ClusterInfo is only written
     * in TransportClusterAllocationExplainAction when handling an allocation explain request with includeDiskInfo during upgrade
     * that is later presented to the user and is not used by any code.
     */
    private static ShardRouting createFakeShardRoutingFromNodeAndShard(NodeAndShard nodeAndShard) {
        return newUnassigned(
            nodeAndShard.shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(REINITIALIZED, "fake"),
            ShardRouting.Role.DEFAULT // ok, this is only used prior to DATA_PATH_NEW_KEY_VERSION which has no other roles
        ).initialize(nodeAndShard.nodeId, null, 0L).moveToStarted(0L);
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
                builder.humanReadableField(c.getKey() + "_bytes", c.getKey(), ByteSizeValue.ofBytes(c.getValue()));
            }
        }
        builder.endObject(); // end "shard_sizes"
        builder.startObject("shard_data_set_sizes");
        {
            for (Map.Entry<ShardId, Long> c : this.shardDataSetSizes.entrySet()) {
                builder.humanReadableField(c.getKey() + "_bytes", c.getKey().toString(), ByteSizeValue.ofBytes(c.getValue()));
            }
        }
        builder.endObject(); // end "shard_data_set_sizes"
        builder.startObject("shard_paths");
        {
            for (Map.Entry<NodeAndShard, String> c : this.dataPath.entrySet()) {
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
     * Returns the shard size for the given shardId or <code>null</code> if that metric is not available.
     */
    public Long getShardSize(ShardId shardId, boolean primary) {
        return shardSizes.get(shardIdentifierFromRouting(shardId, primary));
    }

    /**
     * Returns the shard size for the given shard routing or <code>null</code> if that metric is not available.
     */
    public Long getShardSize(ShardRouting shardRouting) {
        return getShardSize(shardRouting.shardId(), shardRouting.primary());
    }

    /**
     * Returns the shard size for the given shard routing or <code>defaultValue</code> it that metric is not available.
     */
    public long getShardSize(ShardRouting shardRouting, long defaultValue) {
        Long shardSize = getShardSize(shardRouting);
        return shardSize == null ? defaultValue : shardSize;
    }

    /**
     * Returns the nodes absolute data-path the given shard is allocated on or <code>null</code> if the information is not available.
     */
    public String getDataPath(ShardRouting shardRouting) {
        return dataPath.get(NodeAndShard.from(shardRouting));
    }

    public String getDataPath(NodeAndShard nodeAndShard) {
        return dataPath.get(nodeAndShard);
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
        return shardIdentifierFromRouting(shardRouting.shardId(), shardRouting.primary());
    }

    public static String shardIdentifierFromRouting(ShardId shardId, boolean primary) {
        return shardId.toString() + "[" + (primary ? "p" : "r") + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterInfo that = (ClusterInfo) o;
        return leastAvailableSpaceUsage.equals(that.leastAvailableSpaceUsage)
            && mostAvailableSpaceUsage.equals(that.mostAvailableSpaceUsage)
            && shardSizes.equals(that.shardSizes)
            && shardDataSetSizes.equals(that.shardDataSetSizes)
            && dataPath.equals(that.dataPath)
            && reservedSpace.equals(that.reservedSpace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, shardDataSetSizes, dataPath, reservedSpace);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, false);
    }

    public record NodeAndShard(String nodeId, ShardId shardId) implements Writeable {

        public NodeAndShard {
            Objects.requireNonNull(nodeId);
            Objects.requireNonNull(shardId);
        }

        public NodeAndShard(StreamInput in) throws IOException {
            this(in.readString(), new ShardId(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            shardId.writeTo(out);
        }

        public static NodeAndShard from(ShardRouting shardRouting) {
            return new NodeAndShard(shardRouting.currentNodeId(), shardRouting.shardId());
        }
    }

    /**
     * Represents a data path on a node
     */
    public record NodeAndPath(String nodeId, String path) implements Writeable {

        public NodeAndPath {
            Objects.requireNonNull(nodeId);
            Objects.requireNonNull(path);
        }

        public NodeAndPath(StreamInput in) throws IOException {
            this(in.readString(), in.readString());
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
    public record ReservedSpace(long total, Set<ShardId> shardIds) implements Writeable {

        public static final ReservedSpace EMPTY = new ReservedSpace(0, new HashSet<>());

        ReservedSpace(StreamInput in) throws IOException {
            this(in.readVLong(), in.readSet(ShardId::new));
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
