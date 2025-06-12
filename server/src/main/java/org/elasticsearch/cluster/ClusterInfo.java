/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRouting.newUnassigned;
import static org.elasticsearch.cluster.routing.UnassignedInfo.Reason.REINITIALIZED;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.endArray;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.startObject;

/**
 * ClusterInfo is an object representing a map of nodes to {@link DiskUsage}
 * and a map of shard ids to shard sizes, see
 * <code>InternalClusterInfoService.shardIdentifierFromRouting(String)</code>
 * for the key used in the shardSizes map
 */
public class ClusterInfo implements ChunkedToXContent, Writeable {

    public static final ClusterInfo EMPTY = new ClusterInfo();

    public static final TransportVersion DATA_PATH_NEW_KEY_VERSION = TransportVersions.V_8_6_0;

    private final Map<String, DiskUsage> leastAvailableSpaceUsage;
    private final Map<String, DiskUsage> mostAvailableSpaceUsage;
    final Map<String, Long> shardSizes;
    final Map<ShardId, Long> shardDataSetSizes;
    final Map<NodeAndShard, String> dataPath;
    final Map<NodeAndPath, ReservedSpace> reservedSpace;
    final Map<String, ShardHeapUsage> shardHeapUsages;

    protected ClusterInfo() {
        this(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
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
     * @param shardHeapUsages shard heap usage broken down by node
     * @see #shardIdentifierFromRouting
     */
    public ClusterInfo(
        Map<String, DiskUsage> leastAvailableSpaceUsage,
        Map<String, DiskUsage> mostAvailableSpaceUsage,
        Map<String, Long> shardSizes,
        Map<ShardId, Long> shardDataSetSizes,
        Map<NodeAndShard, String> dataPath,
        Map<NodeAndPath, ReservedSpace> reservedSpace,
        Map<String, ShardHeapUsage> shardHeapUsages
    ) {
        this.leastAvailableSpaceUsage = Map.copyOf(leastAvailableSpaceUsage);
        this.mostAvailableSpaceUsage = Map.copyOf(mostAvailableSpaceUsage);
        this.shardSizes = Map.copyOf(shardSizes);
        this.shardDataSetSizes = Map.copyOf(shardDataSetSizes);
        this.dataPath = Map.copyOf(dataPath);
        this.reservedSpace = Map.copyOf(reservedSpace);
        this.shardHeapUsages = Map.copyOf(shardHeapUsages);
    }

    public ClusterInfo(StreamInput in) throws IOException {
        this.leastAvailableSpaceUsage = in.readImmutableMap(DiskUsage::new);
        this.mostAvailableSpaceUsage = in.readImmutableMap(DiskUsage::new);
        this.shardSizes = in.readImmutableMap(StreamInput::readLong);
        this.shardDataSetSizes = in.readImmutableMap(ShardId::new, StreamInput::readLong);
        this.dataPath = in.getTransportVersion().onOrAfter(DATA_PATH_NEW_KEY_VERSION)
            ? in.readImmutableMap(NodeAndShard::new, StreamInput::readString)
            : in.readImmutableMap(nested -> NodeAndShard.from(new ShardRouting(nested)), StreamInput::readString);
        this.reservedSpace = in.readImmutableMap(NodeAndPath::new, ReservedSpace::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.HEAP_USAGE_IN_CLUSTER_INFO)) {
            this.shardHeapUsages = in.readImmutableMap(ShardHeapUsage::new);
        } else {
            this.shardHeapUsages = Map.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.leastAvailableSpaceUsage, StreamOutput::writeWriteable);
        out.writeMap(this.mostAvailableSpaceUsage, StreamOutput::writeWriteable);
        out.writeMap(this.shardSizes, (o, v) -> o.writeLong(v == null ? -1 : v));
        out.writeMap(this.shardDataSetSizes, StreamOutput::writeWriteable, StreamOutput::writeLong);
        if (out.getTransportVersion().onOrAfter(DATA_PATH_NEW_KEY_VERSION)) {
            out.writeMap(this.dataPath, StreamOutput::writeWriteable, StreamOutput::writeString);
        } else {
            out.writeMap(this.dataPath, (o, k) -> createFakeShardRoutingFromNodeAndShard(k).writeTo(o), StreamOutput::writeString);
        }
        out.writeMap(this.reservedSpace);
        if (out.getTransportVersion().onOrAfter(TransportVersions.HEAP_USAGE_IN_CLUSTER_INFO)) {
            out.writeMap(this.shardHeapUsages, StreamOutput::writeWriteable);
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

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(startObject("nodes"), Iterators.map(leastAvailableSpaceUsage.entrySet().iterator(), c -> (builder, p) -> {
            builder.startObject(c.getKey());
            { // node
                builder.field("node_name", c.getValue().nodeName());
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
            return builder.endObject(); // end $nodename
        }),
            chunk(
                (builder, p) -> builder.endObject() // end "nodes"
                    .startObject("shard_sizes")
            ),

            Iterators.map(
                shardSizes.entrySet().iterator(),
                c -> (builder, p) -> builder.humanReadableField(c.getKey() + "_bytes", c.getKey(), ByteSizeValue.ofBytes(c.getValue()))
            ),
            chunk(
                (builder, p) -> builder.endObject() // end "shard_sizes"
                    .startObject("shard_data_set_sizes")
            ),
            Iterators.map(
                shardDataSetSizes.entrySet().iterator(),
                c -> (builder, p) -> builder.humanReadableField(
                    c.getKey() + "_bytes",
                    c.getKey().toString(),
                    ByteSizeValue.ofBytes(c.getValue())
                )
            ),
            chunk(
                (builder, p) -> builder.endObject() // end "shard_data_set_sizes"
                    .startObject("shard_paths")
            ),
            Iterators.map(dataPath.entrySet().iterator(), c -> (builder, p) -> builder.field(c.getKey().toString(), c.getValue())),
            chunk(
                (builder, p) -> builder.endObject() // end "shard_paths"
                    .startArray("reserved_sizes")
            ),
            Iterators.map(reservedSpace.entrySet().iterator(), c -> (builder, p) -> {
                builder.startObject();
                {
                    builder.field("node_id", c.getKey().nodeId);
                    builder.field("path", c.getKey().path);
                    c.getValue().toXContent(builder, params);
                }
                return builder.endObject(); // NodeAndPath
            }),
            endArray() // end "reserved_sizes"
            // NOTE: We don't serialize shardHeapUsages at this stage, to avoid
            // committing to API payloads until the feature is settled
        );
    }

    /**
     * Returns a node id to estimated heap usage mapping for all nodes that we have such data for.
     * Note that these estimates should be considered minimums. They may be used to determine whether
     * there IS NOT capacity to do something, but not to determine that there IS capacity to do something.
     * Also note that the map may not be complete, it may contain none, or a subset of the nodes in
     * the cluster at any time. It may also contain entries for nodes that have since left the cluster.
     */
    public Map<String, ShardHeapUsage> getShardHeapUsages() {
        return shardHeapUsages;
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
     * Returns the shard size for the given shard routing or <code>defaultValue</code> it that metric is not available.
     */
    public long getShardSize(ShardId shardId, boolean primary, long defaultValue) {
        Long shardSize = getShardSize(shardId, primary);
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

    // exposed for tests, computed here rather than exposing all the collections separately
    int getChunkCount() {
        return leastAvailableSpaceUsage.size() + shardSizes.size() + shardDataSetSizes.size() + dataPath.size() + reservedSpace.size() + 6;
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
            this(in.readVLong(), in.readCollectionAsSet(ShardId::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(total);
            out.writeCollection(shardIds);
        }

        public boolean containsShardId(ShardId shardId) {
            return shardIds.contains(shardId);
        }

        void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
