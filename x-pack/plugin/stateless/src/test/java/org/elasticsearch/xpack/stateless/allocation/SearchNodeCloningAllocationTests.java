/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

/**
 * Allocation unit tests for the search-node-cloning feature. Verifies that:
 * <ul>
 *   <li>While the source node is still in the cluster, a replacement whose cache was restored
 *       from that source receives a lower weight (via {@link StatelessBalancingWeightsFactory}'s
 *       {@code CacheRestoreAwareWeightFunction}) and is therefore preferred when a new search
 *       shard needs to be placed.</li>
 *   <li>Nodes without the {@code es_cache_restored_from_node} attribute are allocated
 *       normally.</li>
 *   <li>Once the advertised source node has left the cluster, the replacement window ends:
 *       no weight boost and no parallel-clone {@code NO} from
 *       {@link CacheRestoredAllocationDecider}; the attribute is harmless and shards allocate
 *       normally.</li>
 * </ul>
 *
 * <p>All tests drive allocation through {@link MockAllocationService} backed by a
 * {@link BalancedShardsAllocator} that uses {@link StatelessBalancingWeightsFactory}
 * with {@link CacheRestoredAllocationDecider}, following the pattern established in
 * {@link StatelessBalancingWeightsFactoryTests}.
 */
public class SearchNodeCloningAllocationTests extends ESAllocationTestCase {

    private static final Set<DiscoveryNodeRole> SEARCH_ROLES = Set.of(DiscoveryNodeRole.SEARCH_ROLE);
    private static final Set<DiscoveryNodeRole> INDEXING_ROLES = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE);

    // -----------------------------------------------------------------------
    // Test 1: replacement node with attribute is preferred over a plain node
    // -----------------------------------------------------------------------

    /**
     * With two equally-loaded search nodes — one carrying the
     * {@code es_cache_restored_from_node} attribute and one without — a newly
     * unassigned search shard should be placed on the attributed (replacement) node
     * because {@code CacheRestoreAwareWeightFunction} applies a constant negative boost
     * to that node's weight, making it appear lighter to the balancer.
     */
    public void testCloningScenarioReplacementNodePreferred() {
        final String sourceNodeId = "source-node";
        final String replacementNodeId = "replacement-node";
        final String otherSearchNodeId = "other-search-node";

        // replacementNode advertises that its cache was restored from sourceNode.
        final DiscoveryNode replacementNode = DiscoveryNodeUtils.builder(replacementNodeId)
            .name(replacementNodeId)
            .attributes(Map.of(CacheRestoredAllocationDecider.CACHE_RESTORED_FROM_ATTR, sourceNodeId))
            .roles(SEARCH_ROLES)
            .build();
        final DiscoveryNode sourceNode = DiscoveryNodeUtils.builder(sourceNodeId).name(sourceNodeId).roles(SEARCH_ROLES).build();
        final DiscoveryNode otherSearchNode = newNode(otherSearchNodeId, SEARCH_ROLES);
        // A single indexing node is required by StatelessBalancingWeights (asserts
        // that every node has exactly one of SEARCH or INDEX role).
        final DiscoveryNode indexingNode = newNode("indexing-node", INDEXING_ROLES);

        // Build an index with one search (replica) shard unassigned.
        final IndexMetadata indexMetadata = IndexMetadata.builder("test-index")
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .build();

        final ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoverNodes(indexingNode, sourceNode, replacementNode, otherSearchNode))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(new StatelessShardRoutingRoleStrategy()).addAsNew(indexMetadata))
            .build();

        final MockAllocationService allocationService = buildAllocationService(buildClusterSettings());
        final ClusterState finalState = applyStartedShardsUntilNoChange(initialState, allocationService);

        assertThat(finalState.getRoutingNodes().hasUnassignedShards(), is(false));

        // The search shard (SEARCH_ONLY role) must land on the replacement node, not
        // the other search node, because the boost lowers its effective weight.
        final ShardRouting searchShard = findSearchShard(finalState, "test-index", 0);
        assertThat(searchShard, not(nullValue()));
        assertThat(
            "replacement node should be preferred due to cache-restore weight boost",
            searchShard.currentNodeId(),
            equalTo(replacementNodeId)
        );
    }

    // -----------------------------------------------------------------------
    // Test 2: normal allocation when no node carries the attribute
    // -----------------------------------------------------------------------

    /**
     * When no node carries {@code es_cache_restored_from_node}, allocation should
     * proceed normally and the shard should reach a started state on one of the
     * available search nodes.
     */
    public void testNormalAllocationWithoutAttribute() {
        final String searchNode1Id = "search-node-1";
        final String searchNode2Id = "search-node-2";

        final DiscoveryNode searchNode1 = newNode(searchNode1Id, SEARCH_ROLES);
        final DiscoveryNode searchNode2 = newNode(searchNode2Id, SEARCH_ROLES);
        final DiscoveryNode indexingNode = newNode("indexing-node", INDEXING_ROLES);

        final IndexMetadata indexMetadata = IndexMetadata.builder("plain-index")
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .build();

        final ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoverNodes(indexingNode, searchNode1, searchNode2))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(new StatelessShardRoutingRoleStrategy()).addAsNew(indexMetadata))
            .build();

        final MockAllocationService allocationService = buildAllocationService(buildClusterSettings());
        final ClusterState finalState = applyStartedShardsUntilNoChange(initialState, allocationService);

        assertThat(finalState.getRoutingNodes().hasUnassignedShards(), is(false));

        // The search shard must have landed on one of the two available search nodes.
        final ShardRouting searchShard = findSearchShard(finalState, "plain-index", 0);
        assertThat(searchShard, not(nullValue()));
        assertThat("shard should be on a known search node", searchShard.currentNodeId(), oneOf(searchNode1Id, searchNode2Id));
    }

    // -----------------------------------------------------------------------
    // Test 3: attribute is harmless when the source node is not in the cluster
    // -----------------------------------------------------------------------

    /**
     * A replacement node whose {@code es_cache_restored_from_node} attribute points to
     * a source node that is no longer present in the cluster should still receive shards
     * normally — the replacement window has ended, so neither the weight boost nor
     * parallel-clone {@code NO} applies.
     */
    public void testAttributeHarmlessAfterSourceLeaves() {
        final String absentSourceNodeId = "departed-source-node";
        final String replacementNodeId = "replacement-node";
        final String anotherSearchNodeId = "another-search-node";

        // replacementNode claims a cache restore from a node that is not in the cluster.
        final DiscoveryNode replacementNode = DiscoveryNodeUtils.builder(replacementNodeId)
            .name(replacementNodeId)
            .attributes(Map.of(CacheRestoredAllocationDecider.CACHE_RESTORED_FROM_ATTR, absentSourceNodeId))
            .roles(SEARCH_ROLES)
            .build();
        final DiscoveryNode anotherSearchNode = newNode(anotherSearchNodeId, SEARCH_ROLES);
        final DiscoveryNode indexingNode = newNode("indexing-node", INDEXING_ROLES);

        final IndexMetadata indexMetadata = IndexMetadata.builder("orphan-index")
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .build();

        final ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoverNodes(indexingNode, replacementNode, anotherSearchNode))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(new StatelessShardRoutingRoleStrategy()).addAsNew(indexMetadata))
            .build();

        final MockAllocationService allocationService = buildAllocationService(buildClusterSettings());
        final ClusterState finalState = applyStartedShardsUntilNoChange(initialState, allocationService);

        // No hard-block: the shard must be allocated despite the stale attribute.
        assertThat(finalState.getRoutingNodes().hasUnassignedShards(), is(false));

        final ShardRouting searchShard = findSearchShard(finalState, "orphan-index", 0);
        assertThat(searchShard, not(nullValue()));
        assertThat(
            "shard must land on one of the available search nodes even when source is absent",
            searchShard.currentNodeId(),
            oneOf(replacementNodeId, anotherSearchNodeId)
        );
    }

    // -----------------------------------------------------------------------
    // Test 4: boost directs relocation when an existing shard is rebalanced
    // -----------------------------------------------------------------------

    /**
     * Verifies that the weight boost also drives rebalancing. Two indices have their
     * search shards pre-assigned to {@code otherSearchNode}. A third node
     * ({@code replacementNode}) joins the cluster carrying the
     * {@code es_cache_restored_from_node} attribute. After reroute the balancer should
     * relocate one of the search shards to {@code replacementNode} rather than leaving
     * everything on the overloaded {@code otherSearchNode}.
     */
    public void testBoostDrivesRebalancingToReplacementNode() {
        final String sourceNodeId = "some-past-node";
        final String replacementNodeId = "replacement-node";
        final String otherSearchNodeId = "other-search-node";

        final DiscoveryNode replacementNode = DiscoveryNodeUtils.builder(replacementNodeId)
            .name(replacementNodeId)
            .attributes(Map.of(CacheRestoredAllocationDecider.CACHE_RESTORED_FROM_ATTR, sourceNodeId))
            .roles(SEARCH_ROLES)
            .build();
        final DiscoveryNode sourceNode = DiscoveryNodeUtils.builder(sourceNodeId).name(sourceNodeId).roles(SEARCH_ROLES).build();
        final DiscoveryNode otherSearchNode = newNode(otherSearchNodeId, SEARCH_ROLES);
        final DiscoveryNode indexingNode = newNode("indexing-node", INDEXING_ROLES);

        // Build two indices with search shards pre-started on otherSearchNode.
        final IndexMetadata index1 = buildIndexWithSearchShardOnNode("rebal-index-1", otherSearchNodeId);
        final IndexMetadata index2 = buildIndexWithSearchShardOnNode("rebal-index-2", otherSearchNodeId);

        final ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoverNodes(indexingNode, sourceNode, replacementNode, otherSearchNode))
            .metadata(Metadata.builder().put(index1, false).put(index2, false))
            .routingTable(buildStartedSearchRoutingTable(index1, index2, otherSearchNodeId))
            .build();

        final MockAllocationService allocationService = buildAllocationService(buildClusterSettings());
        final ClusterState finalState = applyStartedShardsUntilNoChange(initialState, allocationService);

        assertThat(finalState.getRoutingNodes().hasUnassignedShards(), is(false));

        // After rebalancing, replacementNode should hold at least one search shard.
        final long shardsOnReplacement = shardsOnNode(finalState, replacementNodeId);
        assertThat("replacement node should receive at least one search shard after rebalancing", shardsOnReplacement > 0, is(true));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static MockAllocationService buildAllocationService(ClusterSettings clusterSettings) {
        final BalancerSettings balancerSettings = new BalancerSettings(clusterSettings);
        return new MockAllocationService(
            new AllocationDeciders(
                List.of(
                    new CacheRestoredAllocationDecider(),
                    new StatelessAllocationDecider(),
                    new ReplicaAfterPrimaryActiveAllocationDecider()
                )
            ),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(
                balancerSettings,
                TEST_WRITE_LOAD_FORECASTER,
                new StatelessBalancingWeightsFactory(balancerSettings, clusterSettings)
            ),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
    }

    private static ClusterSettings buildClusterSettings() {
        final HashSet<Setting<?>> allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(StatelessBalancingWeightsFactory.INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING);
        allSettings.add(StatelessBalancingWeightsFactory.SEARCH_TIER_SHARD_BALANCE_FACTOR_SETTING);
        allSettings.add(StatelessBalancingWeightsFactory.INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING);
        allSettings.add(StatelessBalancingWeightsFactory.SEARCH_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING);
        allSettings.add(StatelessBalancingWeightsFactory.INDEXING_TIER_BALANCING_THRESHOLD_SETTING);
        allSettings.add(StatelessBalancingWeightsFactory.SEARCH_TIER_BALANCING_THRESHOLD_SETTING);
        return new ClusterSettings(Settings.EMPTY, allSettings);
    }

    /** Returns the SEARCH_ONLY shard for the given index and shard number, or null if not found. */
    private static ShardRouting findSearchShard(ClusterState state, String indexName, int shardNumber) {
        final var indexShardRouting = state.routingTable().index(indexName).shard(shardNumber);
        for (int i = 0; i < indexShardRouting.size(); i++) {
            final ShardRouting shard = indexShardRouting.shard(i);
            if (shard.role() == ShardRouting.Role.SEARCH_ONLY) {
                return shard;
            }
        }
        return null;
    }

    /**
     * Builds an {@link IndexMetadata} with a single primary (INDEX_ONLY) and one search
     * (SEARCH_ONLY) shard already in the in-sync set. Both allocation IDs are stored so
     * that the pre-started routing table built by
     * {@link #buildStartedShardRoutingTable} passes the in-sync validation performed by
     * {@link org.elasticsearch.cluster.routing.IndexRoutingTable#validate}.
     */
    private static IndexMetadata buildIndexWithSearchShardOnNode(String indexName, String nodeId) {
        final String indexAllocationId = UUIDs.randomBase64UUID();
        final String searchAllocationId = UUIDs.randomBase64UUID();
        return IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            // Both the primary (INDEX_ONLY) and the replica (SEARCH_ONLY) allocation IDs
            // must be present in the in-sync set; otherwise IndexRoutingTable.validate()
            // throws when the AllocationService first rerouts the pre-started cluster state.
            .putInSyncAllocationIds(0, Set.of(indexAllocationId, searchAllocationId))
            .build();
    }

    /**
     * Builds a routing table with the search shards for both indices pre-started on
     * {@code searchNodeId} and the index shards pre-started on the indexing node
     * (whose id is inferred as not {@code searchNodeId}).
     */
    private static RoutingTable buildStartedSearchRoutingTable(IndexMetadata index1, IndexMetadata index2, String searchNodeId) {
        return RoutingTable.builder(new StatelessShardRoutingRoleStrategy())
            .add(buildStartedShardRoutingTable(index1, searchNodeId))
            .add(buildStartedShardRoutingTable(index2, searchNodeId))
            .build();
    }

    private static IndexRoutingTable buildStartedShardRoutingTable(IndexMetadata indexMetadata, String searchNodeId) {
        // The in-sync set was populated with exactly two IDs: the first is used for the
        // INDEX_ONLY primary, the second for the SEARCH_ONLY replica.
        final var iter = indexMetadata.inSyncAllocationIds(0).iterator();
        final String indexAllocationId = iter.next();
        final String searchAllocationId = iter.next();
        return IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(
                shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), "indexing-node", true, ShardRoutingState.STARTED).withRole(
                    ShardRouting.Role.INDEX_ONLY
                ).withAllocationId(AllocationId.newInitializing(indexAllocationId)).build()
            )
            .addShard(
                shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), searchNodeId, false, ShardRoutingState.STARTED).withRole(
                    ShardRouting.Role.SEARCH_ONLY
                ).withAllocationId(AllocationId.newInitializing(searchAllocationId)).build()
            )
            .build();
    }

    private static long shardsOnNode(ClusterState state, String nodeId) {
        return state.getRoutingNodes().node(nodeId).numberOfOwningShards();
    }

    private static DiscoveryNodes.Builder discoverNodes(DiscoveryNode... nodes) {
        final var builder = DiscoveryNodes.builder();
        for (final DiscoveryNode node : nodes) {
            builder.add(node);
        }
        return builder;
    }
}
