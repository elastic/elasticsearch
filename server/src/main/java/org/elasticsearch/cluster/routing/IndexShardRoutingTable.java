/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.ResponseCollectorService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * {@link IndexShardRoutingTable} encapsulates all instances of a single shard.
 * Each Elasticsearch index consists of multiple shards, each shard encapsulates
 * a disjoint set of the index data and each shard has one or more instances
 * referred to as replicas of a shard. Given that, this class encapsulates all
 * replicas (instances) for a single index shard.
 */
public class IndexShardRoutingTable {

    final ShardShuffler shuffler;
    final ShardId shardId;
    final ShardRouting[] shards;
    final ShardRouting primary;
    final List<ShardRouting> replicas;
    final List<ShardRouting> activeShards;
    final List<ShardRouting> assignedShards;
    private final List<ShardRouting> unpromotableShards;
    /**
     * The initializing list, including ones that are initializing on a target node because of relocation.
     * If we can come up with a better variable name, it would be nice...
     */
    final List<ShardRouting> allInitializingShards;
    final boolean allShardsStarted;
    final int activeSearchShardCount;
    final int totalSearchShardCount;

    IndexShardRoutingTable(ShardId shardId, List<ShardRouting> shards) {
        this.shuffler = new RotationShardShuffler(Randomness.get().nextInt());
        this.shardId = shardId;
        this.shards = shards.toArray(ShardRouting[]::new);

        ShardRouting primary = null;
        List<ShardRouting> replicas = new ArrayList<>();
        List<ShardRouting> activeShards = new ArrayList<>();
        List<ShardRouting> assignedShards = new ArrayList<>();
        List<ShardRouting> unpromotableShards = new ArrayList<>();
        List<ShardRouting> allInitializingShards = new ArrayList<>();
        boolean allShardsStarted = true;
        int activeSearchShardCount = 0;
        int totalSearchShardCount = 0;
        for (ShardRouting shard : this.shards) {
            if (shard.primary()) {
                assert primary == null : "duplicate primary: " + primary + " vs " + shard;
                primary = shard;
            } else {
                replicas.add(shard);
            }
            if (shard.active()) {
                activeShards.add(shard);
                if (shard.role().isSearchable()) {
                    activeSearchShardCount++;
                }
            }
            if (shard.role().isSearchable()) {
                totalSearchShardCount++;
            }
            if (shard.initializing()) {
                allInitializingShards.add(shard);
            }
            if (shard.relocating()) {
                // create the target initializing shard routing on the node the shard is relocating to
                allInitializingShards.add(shard.getTargetRelocatingShard());
                assert shard.assignedToNode() : "relocating from unassigned " + shard;
                assert shard.getTargetRelocatingShard().assignedToNode() : "relocating to unassigned " + shard.getTargetRelocatingShard();
                assignedShards.add(shard.getTargetRelocatingShard());
                if (shard.getTargetRelocatingShard().isPromotableToPrimary() == false) {
                    unpromotableShards.add(shard.getTargetRelocatingShard());
                }
            }
            if (shard.assignedToNode()) {
                assignedShards.add(shard);
                if (shard.isPromotableToPrimary() == false) {
                    unpromotableShards.add(shard);
                }
            }
            if (shard.state() != ShardRoutingState.STARTED) {
                allShardsStarted = false;
            }
        }
        this.primary = primary;
        this.replicas = CollectionUtils.wrapUnmodifiableOrEmptySingleton(replicas);
        this.activeShards = CollectionUtils.wrapUnmodifiableOrEmptySingleton(activeShards);
        this.assignedShards = CollectionUtils.wrapUnmodifiableOrEmptySingleton(assignedShards);
        this.unpromotableShards = CollectionUtils.wrapUnmodifiableOrEmptySingleton(unpromotableShards);
        this.allInitializingShards = CollectionUtils.wrapUnmodifiableOrEmptySingleton(allInitializingShards);
        this.allShardsStarted = allShardsStarted;
        this.activeSearchShardCount = activeSearchShardCount;
        this.totalSearchShardCount = totalSearchShardCount;
    }

    /**
     * Returns the shards id
     *
     * @return id of the shard
     */
    public ShardId shardId() {
        return shardId;
    }

    /**
     * Returns the number of this shards instances.
     */
    public int size() {
        return shards.length;
    }

    public ShardRouting shard(int idx) {
        return shards[idx];
    }

    public Stream<ShardRouting> allShards() {
        return Stream.of(shards);
    }

    /**
     * Returns a {@link List} of active shards
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> activeShards() {
        return this.activeShards;
    }

    /**
     * Returns a {@link List} of all initializing shards, including target shards of relocations
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> getAllInitializingShards() {
        return this.allInitializingShards;
    }

    /**
     * Returns a {@link List} of assigned shards, including relocation targets
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> assignedShards() {
        return this.assignedShards;
    }

    /**
     * Returns a {@link List} of assigned unpromotable shards, including relocation targets
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> unpromotableShards() {
        return this.unpromotableShards;
    }

    public ShardIterator shardsRandomIt() {
        return new PlainShardIterator(shardId, shuffler.shuffle(Arrays.asList(shards)));
    }

    public ShardIterator shardsIt(int seed) {
        return new PlainShardIterator(shardId, shuffler.shuffle(Arrays.asList(shards), seed));
    }

    /**
     * Returns an iterator over active and initializing shards. Making sure though that
     * its random within the active shards, and initializing shards are the last to iterate through.
     */
    public ShardIterator activeInitializingShardsRandomIt() {
        return activeInitializingShardsIt(shuffler.nextSeed());
    }

    /**
     * Returns an iterator over active and initializing shards. Making sure though that
     * its random within the active shards, and initializing shards are the last to iterate through.
     */
    public ShardIterator activeInitializingShardsIt(int seed) {
        if (allInitializingShards.isEmpty()) {
            return new PlainShardIterator(shardId, shuffler.shuffle(activeShards, seed));
        }
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        ordered.addAll(shuffler.shuffle(activeShards, seed));
        ordered.addAll(allInitializingShards);
        return new PlainShardIterator(shardId, ordered);
    }

    /**
     * Returns an iterator over active and initializing shards, ordered by the adaptive replica
     * selection formula. Making sure though that its random within the active shards of the same
     * (or missing) rank, and initializing shards are the last to iterate through.
     */
    public ShardIterator activeInitializingShardsRankedIt(
        @Nullable ResponseCollectorService collector,
        @Nullable Map<String, Long> nodeSearchCounts
    ) {
        final int seed = shuffler.nextSeed();
        if (allInitializingShards.isEmpty()) {
            return new PlainShardIterator(
                shardId,
                rankShardsAndUpdateStats(shuffler.shuffle(activeShards, seed), collector, nodeSearchCounts)
            );
        }

        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        List<ShardRouting> rankedActiveShards = rankShardsAndUpdateStats(shuffler.shuffle(activeShards, seed), collector, nodeSearchCounts);
        ordered.addAll(rankedActiveShards);
        List<ShardRouting> rankedInitializingShards = rankShardsAndUpdateStats(allInitializingShards, collector, nodeSearchCounts);
        ordered.addAll(rankedInitializingShards);
        return new PlainShardIterator(shardId, ordered);
    }

    private static Set<String> getAllNodeIds(final List<ShardRouting> shards) {
        final Set<String> nodeIds = new HashSet<>();
        for (ShardRouting shard : shards) {
            nodeIds.add(shard.currentNodeId());
        }
        return nodeIds;
    }

    private static Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> getNodeStats(
        final Set<String> nodeIds,
        final ResponseCollectorService collector
    ) {

        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = Maps.newMapWithExpectedSize(nodeIds.size());
        for (String nodeId : nodeIds) {
            nodeStats.put(nodeId, collector.getNodeStatistics(nodeId));
        }
        return nodeStats;
    }

    private static Map<String, Double> rankNodes(
        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats,
        final Map<String, Long> nodeSearchCounts
    ) {
        final Map<String, Double> nodeRanks = Maps.newMapWithExpectedSize(nodeStats.size());
        for (Map.Entry<String, Optional<ResponseCollectorService.ComputedNodeStats>> entry : nodeStats.entrySet()) {
            Optional<ResponseCollectorService.ComputedNodeStats> maybeStats = entry.getValue();
            maybeStats.ifPresent(stats -> {
                final String nodeId = entry.getKey();
                nodeRanks.put(nodeId, stats.rank(nodeSearchCounts.getOrDefault(nodeId, 0L)));
            });
        }
        return nodeRanks;
    }

    /**
     * Adjust the for all other nodes' collected stats. In the original ranking paper there is no need to adjust other nodes' stats because
     * Cassandra sends occasional requests to all copies of the data, so their stats will be updated during that broadcast phase. In
     * Elasticsearch, however, we do not have that sort of broadcast-to-all behavior. In order to prevent a node that gets a high score and
     * then never gets any more requests, we must ensure it eventually returns to a more normal score and can be a candidate for serving
     * requests.
     *
     * This adjustment takes the "winning" node's statistics and adds the average of those statistics with each non-winning node. Let's say
     * the winning node had a queue size of 10 and a non-winning node had a queue of 18. The average queue size is (10 + 18) / 2 = 14 so the
     * non-winning node will have statistics added for a queue size of 14. This is repeated for the response time and service times as well.
     */
    private static void adjustStats(
        final ResponseCollectorService collector,
        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats,
        final String minNodeId,
        final ResponseCollectorService.ComputedNodeStats minStats
    ) {
        if (minNodeId != null) {
            for (Map.Entry<String, Optional<ResponseCollectorService.ComputedNodeStats>> entry : nodeStats.entrySet()) {
                final String nodeId = entry.getKey();
                final Optional<ResponseCollectorService.ComputedNodeStats> maybeStats = entry.getValue();
                if (nodeId.equals(minNodeId) == false && maybeStats.isPresent()) {
                    final ResponseCollectorService.ComputedNodeStats stats = maybeStats.get();
                    final int updatedQueue = (minStats.queueSize + stats.queueSize) / 2;
                    final long updatedResponse = (long) (minStats.responseTime + stats.responseTime) / 2;

                    ExponentiallyWeightedMovingAverage avgServiceTime = new ExponentiallyWeightedMovingAverage(
                        ResponseCollectorService.ALPHA,
                        stats.serviceTime
                    );
                    avgServiceTime.addValue((minStats.serviceTime + stats.serviceTime) / 2);
                    final long updatedService = (long) avgServiceTime.getAverage();

                    collector.addNodeStatistics(nodeId, updatedQueue, updatedResponse, updatedService);
                }
            }
        }
    }

    private static List<ShardRouting> rankShardsAndUpdateStats(
        List<ShardRouting> shards,
        final ResponseCollectorService collector,
        final Map<String, Long> nodeSearchCounts
    ) {
        if (collector == null || nodeSearchCounts == null || shards.size() <= 1) {
            return shards;
        }

        // Retrieve which nodes we can potentially send the query to
        final Set<String> nodeIds = getAllNodeIds(shards);
        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = getNodeStats(nodeIds, collector);

        // Retrieve all the nodes the shards exist on
        final Map<String, Double> nodeRanks = rankNodes(nodeStats, nodeSearchCounts);

        // sort all shards based on the shard rank
        ArrayList<ShardRouting> sortedShards = new ArrayList<>(shards);
        Collections.sort(sortedShards, new NodeRankComparator(nodeRanks));

        // adjust the non-winner nodes' stats so they will get a chance to receive queries
        if (sortedShards.size() > 1) {
            ShardRouting minShard = sortedShards.get(0);
            // If the winning shard is not started we are ranking initializing
            // shards, don't bother to do adjustments
            if (minShard.started()) {
                String minNodeId = minShard.currentNodeId();
                Optional<ResponseCollectorService.ComputedNodeStats> maybeMinStats = nodeStats.get(minNodeId);
                if (maybeMinStats.isPresent()) {
                    adjustStats(collector, nodeStats, minNodeId, maybeMinStats.get());
                    // Increase the number of searches for the "winning" node by one.
                    // Note that this doesn't actually affect the "real" counts, instead
                    // it only affects the captured node search counts, which is
                    // captured once for each query in TransportSearchAction
                    nodeSearchCounts.compute(minNodeId, (id, conns) -> conns == null ? 1 : conns + 1);
                }
            }
        }

        return sortedShards;
    }

    private static class NodeRankComparator implements Comparator<ShardRouting> {
        private final Map<String, Double> nodeRanks;

        NodeRankComparator(Map<String, Double> nodeRanks) {
            this.nodeRanks = nodeRanks;
        }

        @Override
        public int compare(ShardRouting s1, ShardRouting s2) {
            if (s1.currentNodeId().equals(s2.currentNodeId())) {
                // these shards on the same node
                return 0;
            }
            Double shard1rank = nodeRanks.get(s1.currentNodeId());
            Double shard2rank = nodeRanks.get(s2.currentNodeId());
            if (shard1rank != null) {
                if (shard2rank != null) {
                    return shard1rank.compareTo(shard2rank);
                } else {
                    // place non-nulls after null values
                    return 1;
                }
            } else {
                if (shard2rank != null) {
                    // place nulls before non-null values
                    return -1;
                } else {
                    // Both nodes do not have stats, they are equal
                    return 0;
                }
            }
        }
    }

    /**
     * Returns an iterator only on the primary shard.
     */
    public ShardIterator primaryShardIt() {
        if (primary != null) {
            return new PlainShardIterator(shardId, Collections.singletonList(primary));
        }
        return new PlainShardIterator(shardId, Collections.emptyList());
    }

    public ShardIterator onlyNodeActiveInitializingShardsIt(String nodeId) {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        int seed = shuffler.nextSeed();
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards, seed)) {
            if (nodeId.equals(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        for (ShardRouting shardRouting : shuffler.shuffle(allInitializingShards, seed)) {
            if (nodeId.equals(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator onlyNodeSelectorActiveInitializingShardsIt(String nodeAttributes, DiscoveryNodes discoveryNodes) {
        return onlyNodeSelectorActiveInitializingShardsIt(new String[] { nodeAttributes }, discoveryNodes);
    }

    /**
     * Returns shards based on nodeAttributes given  such as node name , node attribute, node IP
     * Supports node specifications in cluster API
     */
    public ShardIterator onlyNodeSelectorActiveInitializingShardsIt(String[] nodeAttributes, DiscoveryNodes discoveryNodes) {
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        Set<String> selectedNodes = Sets.newHashSet(discoveryNodes.resolveNodes(nodeAttributes));
        int seed = shuffler.nextSeed();
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards, seed)) {
            if (selectedNodes.contains(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        for (ShardRouting shardRouting : shuffler.shuffle(allInitializingShards, seed)) {
            if (selectedNodes.contains(shardRouting.currentNodeId())) {
                ordered.add(shardRouting);
            }
        }
        if (ordered.isEmpty()) {
            final String message = String.format(
                Locale.ROOT,
                "no data nodes with %s [%s] found for shard: %s",
                nodeAttributes.length == 1 ? "criteria" : "criterion",
                String.join(",", nodeAttributes),
                shardId()
            );
            throw new IllegalArgumentException(message);
        }
        return new PlainShardIterator(shardId, ordered);
    }

    public ShardIterator preferNodeActiveInitializingShardsIt(Set<String> nodeIds) {
        ArrayList<ShardRouting> preferred = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        ArrayList<ShardRouting> notPreferred = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        // fill it in a randomized fashion
        for (ShardRouting shardRouting : shuffler.shuffle(activeShards)) {
            if (nodeIds.contains(shardRouting.currentNodeId())) {
                preferred.add(shardRouting);
            } else {
                notPreferred.add(shardRouting);
            }
        }
        preferred.addAll(notPreferred);
        if (allInitializingShards.isEmpty() == false) {
            preferred.addAll(allInitializingShards);
        }
        return new PlainShardIterator(shardId, preferred);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexShardRoutingTable that = (IndexShardRoutingTable) o;

        if (shardId.equals(that.shardId) == false) return false;
        return Arrays.equals(shards, that.shards);
    }

    @Override
    public int hashCode() {
        int result = shardId.hashCode();
        result = 31 * result + Arrays.hashCode(shards);
        return result;
    }

    /**
     * Returns <code>true</code> iff all shards in the routing table are started otherwise <code>false</code>
     */
    public boolean allShardsStarted() {
        return allShardsStarted;
    }

    /**
     * @return the count of active searchable shards
     */
    public int getActiveSearchShardCount() {
        return activeSearchShardCount;
    }

    /**
     * @return the total count of searchable shards
     */
    public int getTotalSearchShardCount() {
        return totalSearchShardCount;
    }

    public boolean hasSearchShards() {
        return totalSearchShardCount > 0;
    }

    @Nullable
    public ShardRouting getByAllocationId(String allocationId) {
        for (ShardRouting shardRouting : assignedShards()) {
            if (shardRouting.allocationId().getId().equals(allocationId)) {
                return shardRouting;
            }
        }
        return null;
    }

    public Set<String> getPromotableAllocationIds() {
        assert MasterService.assertNotMasterUpdateThread("not using this on the master thread so we don't have to pre-compute this");
        Set<String> allAllocationIds = new HashSet<>();
        for (ShardRouting shard : shards) {
            if (shard.isPromotableToPrimary()) {
                if (shard.relocating()) {
                    allAllocationIds.add(shard.getTargetRelocatingShard().allocationId().getId());
                }
                if (shard.assignedToNode()) {
                    allAllocationIds.add(shard.allocationId().getId());
                }
            }
        }
        return allAllocationIds;
    }

    record AttributesKey(List<String> attributes) {}

    public ShardRouting primaryShard() {
        return primary;
    }

    public List<ShardRouting> replicaShards() {
        return this.replicas;
    }

    public List<ShardRouting> replicaShardsWithState(ShardRoutingState... states) {
        List<ShardRouting> shards = new ArrayList<>();
        for (ShardRouting shardEntry : replicas) {
            for (ShardRoutingState state : states) {
                if (shardEntry.state() == state) {
                    shards.add(shardEntry);
                }
            }
        }
        return shards;
    }

    public List<ShardRouting> shardsWithState(ShardRoutingState state) {
        if (state == ShardRoutingState.INITIALIZING) {
            return allInitializingShards;
        }
        List<ShardRouting> shards = new ArrayList<>();
        for (ShardRouting shardEntry : this.shards) {
            if (shardEntry.state() == state) {
                shards.add(shardEntry);
            }
        }
        return shards;
    }

    public static Builder builder(ShardId shardId) {
        return new Builder(shardId);
    }

    public static class Builder {

        private final ShardId shardId;
        private final List<ShardRouting> shards;

        public Builder(IndexShardRoutingTable indexShard) {
            this.shardId = indexShard.shardId;
            this.shards = new ArrayList<>(indexShard.size());
            Collections.addAll(this.shards, indexShard.shards);
        }

        public ShardId shardId() {
            return shardId;
        }

        public Builder(ShardId shardId) {
            this.shardId = shardId;
            this.shards = new ArrayList<>();
        }

        public Builder addShard(ShardRouting shardEntry) {
            assert shardEntry.shardId().equals(shardId) : "cannot add [" + shardEntry + "] to routing table for " + shardId;
            shards.add(shardEntry);
            return this;
        }

        public Builder removeShard(ShardRouting shardEntry) {
            shards.remove(shardEntry);
            return this;
        }

        public IndexShardRoutingTable build() {
            // don't allow more than one shard copy with same id to be allocated to same node
            assert distinctNodes(shards) : "more than one shard with same id assigned to same node (shards: " + shards + ")";
            assert noDuplicatePrimary(shards) : "expected but did not find unique primary in shard routing table: " + shards;
            assert noAssignedReplicaWithoutActivePrimary(shards) : "unexpected assigned replica with no active primary: " + shards;
            return new IndexShardRoutingTable(shardId, shards);
        }

        static boolean distinctNodes(List<ShardRouting> shards) {
            Set<String> nodes = new HashSet<>();
            for (ShardRouting shard : shards) {
                if (shard.assignedToNode()) {
                    if (nodes.add(shard.currentNodeId()) == false) {
                        return false;
                    }
                    if (shard.relocating()) {
                        if (nodes.add(shard.relocatingNodeId()) == false) {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        static boolean noDuplicatePrimary(List<ShardRouting> shards) {
            boolean seenPrimary = false;
            for (final var shard : shards) {
                if (shard.primary()) {
                    if (seenPrimary) {
                        return false;
                    }
                    seenPrimary = true;
                }
            }
            return seenPrimary;
        }

        static boolean noAssignedReplicaWithoutActivePrimary(List<ShardRouting> shards) {
            boolean seenAssignedReplica = false;
            for (final var shard : shards) {
                if (shard.currentNodeId() != null) {
                    if (shard.primary()) {
                        if (shard.active()) {
                            return true;
                        }
                    } else {
                        seenAssignedReplica = true;
                    }
                }
            }

            return seenAssignedReplica == false;
        }

        public static IndexShardRoutingTable.Builder readFrom(StreamInput in) throws IOException {
            Index index = new Index(in);
            return readFromThin(in, index);
        }

        public static IndexShardRoutingTable.Builder readFromThin(StreamInput in, Index index) throws IOException {
            int iShardId = in.readVInt();
            ShardId shardId = new ShardId(index, iShardId);
            Builder builder = new Builder(shardId);

            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                ShardRouting shard = new ShardRouting(shardId, in);
                builder.addShard(shard);
            }

            return builder;
        }

        public static void writeTo(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            indexShard.shardId().getIndex().writeTo(out);
            writeToThin(indexShard, out);
        }

        public static void writeToThin(IndexShardRoutingTable indexShard, StreamOutput out) throws IOException {
            out.writeVInt(indexShard.shardId.id());
            out.writeArray((o, v) -> v.writeToThin(o), indexShard.shards);
        }

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexShardRoutingTable(").append(shardId()).append("){");
        final int numShards = shards.length;
        for (int i = 0; i < numShards; i++) {
            sb.append(shards[i].shortSummary());
            if (i < numShards - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
