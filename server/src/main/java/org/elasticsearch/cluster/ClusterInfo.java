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
import org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
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
 * and a map of shard ids to shard sizes and shard write-loads, see
 * <code>InternalClusterInfoService.shardIdentifierFromRouting(String)</code>
 * for the key used in the shardSizes map
 */
public class ClusterInfo implements ChunkedToXContent, Writeable, ExpectedShardSizeEstimator.ShardSizeProvider {

    public static final ClusterInfo EMPTY = new ClusterInfo();

    private static final TransportVersion HEAP_USAGE_IN_CLUSTER_INFO = TransportVersion.fromName("heap_usage_in_cluster_info");
    private static final TransportVersion NODE_USAGE_STATS_FOR_THREAD_POOLS_IN_CLUSTER_INFO = TransportVersion.fromName(
        "node_usage_stats_for_thread_pools_in_cluster_info"
    );
    private static final TransportVersion SHARD_WRITE_LOAD_IN_CLUSTER_INFO = TransportVersion.fromName("shard_write_load_in_cluster_info");
    private static final TransportVersion MAX_HEAP_SIZE_PER_NODE_IN_CLUSTER_INFO = TransportVersion.fromName(
        "max_heap_size_per_node_in_cluster_info"
    );

    private final Map<String, DiskUsage> leastAvailableSpaceUsage;
    private final Map<String, DiskUsage> mostAvailableSpaceUsage;
    final Map<String, Long> shardSizes;
    final Map<ShardId, Long> shardDataSetSizes;
    final Map<NodeAndShard, String> dataPath;
    final Map<NodeAndPath, ReservedSpace> reservedSpace;
    final Map<String, EstimatedHeapUsage> estimatedHeapUsages;
    final Map<String, NodeUsageStatsForThreadPools> nodeUsageStatsForThreadPools;
    final Map<ShardId, Double> shardWriteLoads;
    // max heap size per node ID
    final Map<String, ByteSizeValue> maxHeapSizePerNode;
    private final Map<ShardId, Set<String>> shardToNodeIds;

    protected ClusterInfo() {
        this(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
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
     * @param estimatedHeapUsages estimated heap usage broken down by node
     * @param nodeUsageStatsForThreadPools node-level usage stats (operational load) broken down by node
     * @see #shardIdentifierFromRouting
     * @param maxHeapSizePerNode node id to max heap size
     */
    public ClusterInfo(
        Map<String, DiskUsage> leastAvailableSpaceUsage,
        Map<String, DiskUsage> mostAvailableSpaceUsage,
        Map<String, Long> shardSizes,
        Map<ShardId, Long> shardDataSetSizes,
        Map<NodeAndShard, String> dataPath,
        Map<NodeAndPath, ReservedSpace> reservedSpace,
        Map<String, EstimatedHeapUsage> estimatedHeapUsages,
        Map<String, NodeUsageStatsForThreadPools> nodeUsageStatsForThreadPools,
        Map<ShardId, Double> shardWriteLoads,
        Map<String, ByteSizeValue> maxHeapSizePerNode
    ) {
        this(
            leastAvailableSpaceUsage,
            mostAvailableSpaceUsage,
            shardSizes,
            shardDataSetSizes,
            dataPath,
            reservedSpace,
            estimatedHeapUsages,
            nodeUsageStatsForThreadPools,
            shardWriteLoads,
            maxHeapSizePerNode,
            computeShardToNodeIds(dataPath)
        );
    }

    private ClusterInfo(
        Map<String, DiskUsage> leastAvailableSpaceUsage,
        Map<String, DiskUsage> mostAvailableSpaceUsage,
        Map<String, Long> shardSizes,
        Map<ShardId, Long> shardDataSetSizes,
        Map<NodeAndShard, String> dataPath,
        Map<NodeAndPath, ReservedSpace> reservedSpace,
        Map<String, EstimatedHeapUsage> estimatedHeapUsages,
        Map<String, NodeUsageStatsForThreadPools> nodeUsageStatsForThreadPools,
        Map<ShardId, Double> shardWriteLoads,
        Map<String, ByteSizeValue> maxHeapSizePerNode,
        Map<ShardId, Set<String>> shardToNodeIds
    ) {
        this.leastAvailableSpaceUsage = Map.copyOf(leastAvailableSpaceUsage);
        this.mostAvailableSpaceUsage = Map.copyOf(mostAvailableSpaceUsage);
        this.shardSizes = Map.copyOf(shardSizes);
        this.shardDataSetSizes = Map.copyOf(shardDataSetSizes);
        this.dataPath = Map.copyOf(dataPath);
        this.reservedSpace = Map.copyOf(reservedSpace);
        this.estimatedHeapUsages = Map.copyOf(estimatedHeapUsages);
        this.nodeUsageStatsForThreadPools = Map.copyOf(nodeUsageStatsForThreadPools);
        this.shardWriteLoads = Map.copyOf(shardWriteLoads);
        this.maxHeapSizePerNode = Map.copyOf(maxHeapSizePerNode);
        this.shardToNodeIds = shardToNodeIds;
    }

    public ClusterInfo(StreamInput in) throws IOException {
        this.leastAvailableSpaceUsage = in.readImmutableMap(DiskUsage::new);
        this.mostAvailableSpaceUsage = in.readImmutableMap(DiskUsage::new);
        this.shardSizes = in.readImmutableMap(StreamInput::readLong);
        this.shardDataSetSizes = in.readImmutableMap(ShardId::new, StreamInput::readLong);
        this.dataPath = in.readImmutableMap(NodeAndShard::new, StreamInput::readString);
        this.reservedSpace = in.readImmutableMap(NodeAndPath::new, ReservedSpace::new);
        if (in.getTransportVersion().supports(HEAP_USAGE_IN_CLUSTER_INFO)) {
            this.estimatedHeapUsages = in.readImmutableMap(EstimatedHeapUsage::new);
        } else {
            this.estimatedHeapUsages = Map.of();
        }
        if (in.getTransportVersion().supports(NODE_USAGE_STATS_FOR_THREAD_POOLS_IN_CLUSTER_INFO)) {
            this.nodeUsageStatsForThreadPools = in.readImmutableMap(NodeUsageStatsForThreadPools::new);
        } else {
            this.nodeUsageStatsForThreadPools = Map.of();
        }
        if (in.getTransportVersion().supports(SHARD_WRITE_LOAD_IN_CLUSTER_INFO)) {
            this.shardWriteLoads = in.readImmutableMap(ShardId::new, StreamInput::readDouble);
        } else {
            this.shardWriteLoads = Map.of();
        }
        if (in.getTransportVersion().supports(MAX_HEAP_SIZE_PER_NODE_IN_CLUSTER_INFO)) {
            this.maxHeapSizePerNode = in.readImmutableMap(ByteSizeValue::readFrom);
        } else {
            this.maxHeapSizePerNode = Map.of();
        }
        this.shardToNodeIds = computeShardToNodeIds(dataPath);
    }

    ClusterInfo updateWith(
        Map<String, DiskUsage> leastAvailableSpaceUsage,
        Map<String, DiskUsage> mostAvailableSpaceUsage,
        Map<String, Long> shardSizes,
        Map<NodeAndPath, ReservedSpace> reservedSpace,
        Map<String, EstimatedHeapUsage> estimatedHeapUsages,
        Map<String, NodeUsageStatsForThreadPools> nodeUsageStatsForThreadPools
    ) {
        return new ClusterInfo(
            leastAvailableSpaceUsage,
            mostAvailableSpaceUsage,
            shardSizes,
            shardDataSetSizes,
            dataPath,
            reservedSpace,
            estimatedHeapUsages,
            nodeUsageStatsForThreadPools,
            shardWriteLoads,
            maxHeapSizePerNode,
            shardToNodeIds
        );
    }

    private static Map<ShardId, Set<String>> computeShardToNodeIds(Map<NodeAndShard, String> dataPath) {
        if (dataPath.isEmpty()) {
            return Map.of();
        }
        final var shardToNodeIds = new HashMap<ShardId, Set<String>>();
        for (NodeAndShard nodeAndShard : dataPath.keySet()) {
            shardToNodeIds.computeIfAbsent(nodeAndShard.shardId, ignore -> new HashSet<>()).add(nodeAndShard.nodeId);
        }
        return Collections.unmodifiableMap(Maps.transformValues(shardToNodeIds, Collections::unmodifiableSet));
    }

    public Set<String> getNodeIdsForShard(ShardId shardId) {
        assert shardToNodeIds != null : "shardToNodeIds not computed for simulations, make sure this ClusterInfo is from polling";
        return shardToNodeIds.getOrDefault(shardId, Set.of());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.leastAvailableSpaceUsage, StreamOutput::writeWriteable);
        out.writeMap(this.mostAvailableSpaceUsage, StreamOutput::writeWriteable);
        out.writeMap(this.shardSizes, (o, v) -> o.writeLong(v == null ? -1 : v));
        out.writeMap(this.shardDataSetSizes, StreamOutput::writeWriteable, StreamOutput::writeLong);
        out.writeMap(this.dataPath, StreamOutput::writeWriteable, StreamOutput::writeString);
        out.writeMap(this.reservedSpace);
        if (out.getTransportVersion().supports(HEAP_USAGE_IN_CLUSTER_INFO)) {
            out.writeMap(this.estimatedHeapUsages, StreamOutput::writeWriteable);
        }
        if (out.getTransportVersion().supports(NODE_USAGE_STATS_FOR_THREAD_POOLS_IN_CLUSTER_INFO)) {
            out.writeMap(this.nodeUsageStatsForThreadPools, StreamOutput::writeWriteable);
        }
        if (out.getTransportVersion().supports(SHARD_WRITE_LOAD_IN_CLUSTER_INFO)) {
            out.writeMap(this.shardWriteLoads, StreamOutput::writeWriteable, StreamOutput::writeDouble);
        }
        if (out.getTransportVersion().supports(MAX_HEAP_SIZE_PER_NODE_IN_CLUSTER_INFO)) {
            out.writeMap(this.maxHeapSizePerNode, StreamOutput::writeWriteable);
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
            // NOTE: We don't serialize estimatedHeapUsages/nodeUsageStatsForThreadPools/shardWriteLoads/maxHeapSizePerNode at this stage,
            // to avoid committing to API payloads until the features are settled
        );
    }

    /**
     * Returns a node id to estimated heap usage mapping for all nodes that we have such data for.
     * Note that these estimates should be considered minimums. They may be used to determine whether
     * there IS NOT capacity to do something, but not to determine that there IS capacity to do something.
     * Also note that the map may not be complete, it may contain none, or a subset of the nodes in
     * the cluster at any time. It may also contain entries for nodes that have since left the cluster.
     */
    public Map<String, EstimatedHeapUsage> getEstimatedHeapUsages() {
        return estimatedHeapUsages;
    }

    /**
     * Returns a map containing thread pool usage stats for each node, keyed by node ID.
     */
    public Map<String, NodeUsageStatsForThreadPools> getNodeUsageStatsForThreadPools() {
        return nodeUsageStatsForThreadPools;
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
     * Returns a map of shard IDs to the write-loads for use in balancing. The write-loads can be interpreted
     * as the average number of threads that ingestion to the shard will consume.
     * This information may be partial or missing altogether under some circumstances. The absence of a shard
     * write load from the map should be interpreted as "unknown".
     */
    public Map<ShardId, Double> getShardWriteLoads() {
        return shardWriteLoads;
    }

    /**
     * Returns the shard size for the given shardId or <code>null</code> if that metric is not available.
     */
    @Override
    public Long getShardSize(ShardId shardId, boolean primary) {
        return shardSizes.get(shardIdentifierFromRouting(shardId, primary));
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

    public Map<String, ByteSizeValue> getMaxHeapSizePerNode() {
        return this.maxHeapSizePerNode;
    }

    /**
     * Return true if the shard has moved since the time ClusterInfo was created.
     */
    public boolean hasShardMoved(ShardRouting shardRouting) {
        // We use dataPath to find out whether a shard is allocated on a node.
        // TODO: DataPath is sent with disk usages but thread pool usage is sent separately so that local shard allocation
        // may change between the two calls.
        return getDataPath(shardRouting) == null;
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
            && reservedSpace.equals(that.reservedSpace)
            && estimatedHeapUsages.equals(that.estimatedHeapUsages)
            && nodeUsageStatsForThreadPools.equals(that.nodeUsageStatsForThreadPools)
            && shardWriteLoads.equals(that.shardWriteLoads)
            && maxHeapSizePerNode.equals(that.maxHeapSizePerNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            leastAvailableSpaceUsage,
            mostAvailableSpaceUsage,
            shardSizes,
            shardDataSetSizes,
            dataPath,
            reservedSpace,
            estimatedHeapUsages,
            nodeUsageStatsForThreadPools,
            shardWriteLoads,
            maxHeapSizePerNode
        );
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Map<String, DiskUsage> leastAvailableSpaceUsage = Map.of();
        private Map<String, DiskUsage> mostAvailableSpaceUsage = Map.of();
        private Map<String, Long> shardSizes = Map.of();
        private Map<ShardId, Long> shardDataSetSizes = Map.of();
        private Map<NodeAndShard, String> dataPath = Map.of();
        private Map<NodeAndPath, ReservedSpace> reservedSpace = Map.of();
        private Map<String, EstimatedHeapUsage> estimatedHeapUsages = Map.of();
        private Map<String, NodeUsageStatsForThreadPools> nodeUsageStatsForThreadPools = Map.of();
        private Map<ShardId, Double> shardWriteLoads = Map.of();
        private Map<String, ByteSizeValue> maxHeapSizePerNode = Map.of();

        public ClusterInfo build() {
            return new ClusterInfo(
                leastAvailableSpaceUsage,
                mostAvailableSpaceUsage,
                shardSizes,
                shardDataSetSizes,
                dataPath,
                reservedSpace,
                estimatedHeapUsages,
                nodeUsageStatsForThreadPools,
                shardWriteLoads,
                maxHeapSizePerNode
            );
        }

        public Builder leastAvailableSpaceUsage(Map<String, DiskUsage> leastAvailableSpaceUsage) {
            this.leastAvailableSpaceUsage = leastAvailableSpaceUsage;
            return this;
        }

        public Builder mostAvailableSpaceUsage(Map<String, DiskUsage> mostAvailableSpaceUsage) {
            this.mostAvailableSpaceUsage = mostAvailableSpaceUsage;
            return this;
        }

        public Builder shardSizes(Map<String, Long> shardSizes) {
            this.shardSizes = shardSizes;
            return this;
        }

        public Builder shardDataSetSizes(Map<ShardId, Long> shardDataSetSizes) {
            this.shardDataSetSizes = shardDataSetSizes;
            return this;
        }

        public Builder dataPath(Map<NodeAndShard, String> dataPath) {
            this.dataPath = dataPath;
            return this;
        }

        public Builder reservedSpace(Map<NodeAndPath, ReservedSpace> reservedSpace) {
            this.reservedSpace = reservedSpace;
            return this;
        }

        public Builder estimatedHeapUsages(Map<String, EstimatedHeapUsage> estimatedHeapUsages) {
            this.estimatedHeapUsages = estimatedHeapUsages;
            return this;
        }

        public Builder nodeUsageStatsForThreadPools(Map<String, NodeUsageStatsForThreadPools> nodeUsageStatsForThreadPools) {
            this.nodeUsageStatsForThreadPools = nodeUsageStatsForThreadPools;
            return this;
        }

        public Builder shardWriteLoads(Map<ShardId, Double> shardWriteLoads) {
            this.shardWriteLoads = shardWriteLoads;
            return this;
        }

        public Builder maxHeapSizePerNode(Map<String, ByteSizeValue> maxHeapSizePerNode) {
            this.maxHeapSizePerNode = maxHeapSizePerNode;
            return this;
        }
    }
}
