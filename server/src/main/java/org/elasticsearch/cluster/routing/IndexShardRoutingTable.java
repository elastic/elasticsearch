/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.HashMap;
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
    private final List<ShardRouting> assignedUnpromotableShards;
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
        List<ShardRouting> assignedUnpromotableShards = new ArrayList<>();
        List<ShardRouting> allInitializingShards = new ArrayList<>();
        List<ShardRouting> unpromotableShards = new ArrayList<>();
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
            if (shard.isPromotableToPrimary() == false) {
                unpromotableShards.add(shard);
            }
            if (shard.relocating()) {
                // create the target initializing shard routing on the node the shard is relocating to
                allInitializingShards.add(shard.getTargetRelocatingShard());
                assert shard.assignedToNode() : "relocating from unassigned " + shard;
                assert shard.getTargetRelocatingShard().assignedToNode() : "relocating to unassigned " + shard.getTargetRelocatingShard();
                assignedShards.add(shard.getTargetRelocatingShard());
                if (shard.getTargetRelocatingShard().isPromotableToPrimary() == false) {
                    assignedUnpromotableShards.add(shard.getTargetRelocatingShard());
                    unpromotableShards.add(shard.getTargetRelocatingShard());
                }
            }
            if (shard.assignedToNode()) {
                assignedShards.add(shard);
                if (shard.isPromotableToPrimary() == false) {
                    assignedUnpromotableShards.add(shard);
                }
            }
            if (shard.state() != ShardRoutingState.STARTED) {
                allShardsStarted = false;
            }
        }
        assert shards.isEmpty() == false : "cannot have an empty shard routing table";
        assert primary != null : shards;
        assert unpromotableShards.containsAll(assignedUnpromotableShards)
            : unpromotableShards + " does not contain all assigned unpromotable shards " + assignedUnpromotableShards;
        this.primary = primary;
        this.replicas = CollectionUtils.wrapUnmodifiableOrEmptySingleton(replicas);
        this.activeShards = CollectionUtils.wrapUnmodifiableOrEmptySingleton(activeShards);
        this.assignedShards = CollectionUtils.wrapUnmodifiableOrEmptySingleton(assignedShards);
        this.assignedUnpromotableShards = CollectionUtils.wrapUnmodifiableOrEmptySingleton(assignedUnpromotableShards);
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
    public List<ShardRouting> assignedUnpromotableShards() {
        return this.assignedUnpromotableShards;
    }

    /**
     * Returns a {@link List} of all unpromotable shards, including unassigned shards
     *
     * @return a {@link List} of shards
     */
    public List<ShardRouting> unpromotableShards() {
        return this.unpromotableShards;
    }

    public ShardIterator shardsRandomIt() {
        return new ShardIterator(shardId, shuffler.shuffle(Arrays.asList(shards)));
    }

    public ShardIterator shardsIt(int seed) {
        return new ShardIterator(shardId, shuffler.shuffle(Arrays.asList(shards), seed));
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
            return new ShardIterator(shardId, shuffler.shuffle(activeShards, seed));
        }
        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        ordered.addAll(shuffler.shuffle(activeShards, seed));
        ordered.addAll(allInitializingShards);
        return new ShardIterator(shardId, ordered);
    }

    /**
     * Returns an iterator over active and initializing shards, ordered by the adaptive replica
     * selection formula. Making sure though that its random within the active shards of the same
     * (or missing) rank, and initializing shards are the last to iterate through.
     */
    public ShardIterator activeInitializingShardsRankedIt(@Nullable OperationRouting.ArsContext arsContext) {
        if (arsContext == null) {
            return activeInitializingShardsRandomIt();
        }
        final int seed = shuffler.nextSeed();
        if (allInitializingShards.isEmpty()) {
            return new ShardIterator(shardId, rankShardsAndUpdateStats(shuffler.shuffle(activeShards, seed), arsContext));
        }

        ArrayList<ShardRouting> ordered = new ArrayList<>(activeShards.size() + allInitializingShards.size());
        ordered.addAll(rankShardsAndUpdateStats(shuffler.shuffle(activeShards, seed), arsContext));
        ordered.addAll(rankShardsAndUpdateStats(allInitializingShards, arsContext));
        return new ShardIterator(shardId, ordered);
    }

    private static Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> getNodeStats(
        List<ShardRouting> shardRoutings,
        final ResponseCollectorService collector
    ) {

        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>();
        for (ShardRouting shardRouting : shardRoutings) {
            nodeStats.computeIfAbsent(shardRouting.currentNodeId(), collector::getNodeStatistics);
        }
        return nodeStats;
    }

    /**
     * Computes a rank for each node based on its adaptive replica selection (ARS) stats.
     * Nodes with stats are ranked using the C3 formula via {@link ResponseCollectorService.ComputedNodeStats#rank}.
     * Nodes without stats (e.g. newly joined) are assigned {@code Math.nextDown(bestRank)} so they
     * are probed ahead of measured nodes, but only while their in-flight request count stays
     * below {@code probeInflightCap}. The cap is enforced against both counts independently — a
     * node is gated if either the snapshot count (within-search shard wins) or the live count
     * (real-time concurrent load across searches) reaches the cap.
     * <p>
     * When {@code warmupSamples > 0}, a peer is considered <em>warm</em> once its
     * {@link ResponseCollectorService.ComputedNodeStats#observationCount} reaches the threshold;
     * below the threshold it is <em>warming up</em>. Two protections apply during warmup:
     * <ul>
     *   <li><b>Inflight cap</b>: the same {@code probeInflightCap} that gates stat-less probing
     *       continues to apply to warming-up peers — at-or-above the cap they get no rank entry
     *       and sort last via {@code nullsLast}, so a sudden burst can never put more than
     *       {@code probeInflightCap} concurrent requests on a node before it has graduated.</li>
     *   <li><b>Rank clamp</b>: a warming-up peer below the cap whose bare rank is below the lowest
     *       warm peer's rank is clamped up to that lowest warm rank, so it ties with the best
     *       warm peer (~50/50 via comparator tie-break + shuffle) instead of looking fastest from
     *       sparse stats.</li>
     * </ul>
     * Once a peer's observation count reaches the threshold both protections release, and it ranks
     * on standard C3 terms.
     *
     * @param nodeStats        per-node EWMA stats; {@code Optional.empty()} for nodes without stats
     * @param searchCounts     mutable per-search snapshot of in-flight counts used by the ARS
     *                         formula and local multi-shard spreading
     * @param globalCounts     live (real-time) in-flight counts for the probe cap check
     * @param probeInflightCap maximum number of concurrent in-flight requests to a stat-less or
     *                         warming-up node before the in-flight gate kicks in
     * @param warmupSamples    minimum observation count for a peer to be considered warm; {@code 0}
     *                         disables both warmup protections (clamp and in-flight gate during warmup)
     * @return map of node ID to rank; stat-less nodes outside the cap, and all stat-less nodes when
     *         {@code probeInflightCap} is 0, have no entry and sort per the caller's comparator
     */
    static Map<String, Double> rankNodes(
        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats,
        final Map<String, Long> searchCounts,
        final Map<String, Long> globalCounts,
        final long probeInflightCap,
        final int warmupSamples
    ) {
        final Map<String, Double> nodeRanks = Maps.newMapWithExpectedSize(nodeStats.size());
        List<String> probeCandidates = null;
        List<String> warmingUpRanked = null;
        double bestPeerRank = Double.POSITIVE_INFINITY;
        double bestWarmRank = Double.POSITIVE_INFINITY;

        for (Map.Entry<String, Optional<ResponseCollectorService.ComputedNodeStats>> entry : nodeStats.entrySet()) {
            final String nodeId = entry.getKey();
            final Optional<ResponseCollectorService.ComputedNodeStats> maybeStats = entry.getValue();
            final long searchCount = searchCounts.getOrDefault(nodeId, 0L);

            if (maybeStats.isEmpty()) {
                // Stat-less: probe ahead of measured peers if below the cap, otherwise skip and
                // sort last via nullsLast.
                if (searchCount < probeInflightCap && globalCounts.getOrDefault(nodeId, 0L) < probeInflightCap) {
                    if (probeCandidates == null) {
                        probeCandidates = new ArrayList<>();
                    }
                    probeCandidates.add(nodeId);
                }
                continue;
            }

            final ResponseCollectorService.ComputedNodeStats stats = maybeStats.get();
            final boolean warmingUp = warmupSamples > 0 && stats.observationCount < warmupSamples;

            // Warming-up peers obey the same in-flight cap as stat-less probes. At-or-above the
            // cap they get no rank entry and sort last via nullsLast, hard-capping the burst load
            // a sparsely measured node can absorb before it has graduated to warm.
            if (warmingUp) {
                if (probeInflightCap > 0
                    && (searchCount >= probeInflightCap || globalCounts.getOrDefault(nodeId, 0L) >= probeInflightCap)) {
                    continue;
                }
                if (warmingUpRanked == null) {
                    warmingUpRanked = new ArrayList<>();
                }
                warmingUpRanked.add(nodeId);
            }

            final double bare = stats.rank(searchCount);
            nodeRanks.put(nodeId, bare);

            // bestPeerRank tracks the lowest bare rank across all peers; bestWarmRank tracks the
            // lowest among warm peers only.
            if (bare < bestPeerRank) {
                bestPeerRank = bare;
            }
            if (warmingUp == false && bare < bestWarmRank) {
                bestWarmRank = bare;
            }
        }

        // Clamp warming-up peers up to the best warm peer's rank so they tie with it (~50/50 via
        // the comparator's tie-break) instead of looking fastest from sparse stats.
        if (warmingUpRanked != null && bestWarmRank != Double.POSITIVE_INFINITY) {
            for (String nodeId : warmingUpRanked) {
                if (nodeRanks.get(nodeId) < bestWarmRank) {
                    nodeRanks.put(nodeId, bestWarmRank);
                }
            }
        }

        // Place stat-less probe candidates just ahead of the best known rank.
        if (probeCandidates != null && nodeRanks.isEmpty() == false) {
            final double probeRank = Math.nextDown(bestPeerRank);
            for (String nodeId : probeCandidates) {
                nodeRanks.put(nodeId, probeRank);
            }
        }
        return nodeRanks;
    }

    /**
     * Adjust all other nodes' collected stats. In the original ranking paper there is no need to adjust other nodes' stats because
     * Cassandra sends occasional requests to all copies of the data, so their stats will be updated during that broadcast phase. In
     * Elasticsearch, however, we do not have that sort of broadcast-to-all behavior. In order to prevent a node that gets a high score and
     * then never gets any more requests, we must ensure it eventually returns to a more normal score and can be a candidate for serving
     * requests.
     * <p>
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

    private static List<ShardRouting> rankShardsAndUpdateStats(List<ShardRouting> shards, final OperationRouting.ArsContext arsContext) {
        final ResponseCollectorService collector = arsContext.collector();
        final Map<String, Long> searchCounts = arsContext.searchCounts();
        final Map<String, Long> globalCounts = arsContext.globalCounts();
        if (collector == null || searchCounts == null || shards.size() <= 1) {
            return shards;
        }

        // Retrieve which nodes we can potentially send the query to
        final Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = getNodeStats(shards, collector);

        // Zeroing the cap suppresses probe candidates; probeEnabled also selects the null-rank
        // sort order and enables warmup smoothing.
        final long probeInflightCap = arsContext.probeEnabled() ? arsContext.probeInflightCap() : 0L;
        final int warmupSamples = arsContext.probeEnabled() ? arsContext.warmupSamples() : 0;

        // sort all shards based on the shard rank
        ArrayList<ShardRouting> sortedShards = new ArrayList<>(shards);
        sortedShards.sort(
            new NodeRankComparator(
                rankNodes(nodeStats, searchCounts, globalCounts, probeInflightCap, warmupSamples),
                arsContext.probeEnabled()
            )
        );

        // Blend the winner's stats into non-winner nodes so their stale EWMA values
        // gradually converge toward the winner's, preventing permanent starvation.
        ShardRouting minShard = sortedShards.getFirst();
        // If the winning shard is not started we are ranking initializing
        // shards, don't bother to do adjustments
        if (minShard.started()) {
            String minNodeId = minShard.currentNodeId();
            // Increase the number of searches for the "winning" node by one.
            // Note that this doesn't actually affect the "real" counts, instead
            // it only affects the snapshot, which is shared across shards within
            // one search for multi-shard spreading. This must happen outside the
            // stats check so that stat-less probe winners also increment the count,
            // making the probe cap effective for multi-shard indices.
            searchCounts.compute(minNodeId, (id, conns) -> conns == null ? 1 : conns + 1);
            Optional<ResponseCollectorService.ComputedNodeStats> maybeMinStats = nodeStats.get(minNodeId);
            maybeMinStats.ifPresent(computedNodeStats -> adjustStats(collector, nodeStats, minNodeId, computedNodeStats));
        }

        return sortedShards;
    }

    private static class NodeRankComparator implements Comparator<ShardRouting> {
        private static final Comparator<Double> PROBE_ON_COMPARATOR = Comparator.nullsLast(Double::compare);
        private static final Comparator<Double> PROBE_OFF_COMPARATOR = Comparator.nullsFirst(Double::compare);

        private final Map<String, Double> nodeRanks;
        private final Comparator<Double> rankComparator;

        NodeRankComparator(Map<String, Double> nodeRanks, boolean probeEnabled) {
            this.nodeRanks = nodeRanks;
            // When probing is on, unranked nodes (above cap or no stats) sort last.
            // When probing is off, unranked nodes sort first to preserve original ARS behavior.
            this.rankComparator = probeEnabled ? PROBE_ON_COMPARATOR : PROBE_OFF_COMPARATOR;
        }

        @Override
        public int compare(ShardRouting s1, ShardRouting s2) {
            return rankComparator.compare(nodeRanks.get(s1.currentNodeId()), nodeRanks.get(s2.currentNodeId()));
        }
    }

    /**
     * Returns an iterator only on the primary shard.
     */
    public ShardIterator primaryShardIt() {
        if (primary != null) {
            return new ShardIterator(shardId, Collections.singletonList(primary));
        }
        return new ShardIterator(shardId, Collections.emptyList());
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
        return new ShardIterator(shardId, ordered);
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
        return new ShardIterator(shardId, ordered);
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
        return new ShardIterator(shardId, preferred);
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
            assert shardEntry.shardId().equals(shardId)
                : "cannot add ["
                    + shardEntry
                    + "]/{"
                    + shardEntry.shardId().getIndex().getUUID()
                    + "} to routing table for "
                    + shardId
                    + "{"
                    + shardId.getIndex().getUUID()
                    + "}";
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
