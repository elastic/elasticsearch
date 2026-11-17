/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoveryPriority;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.IndexBalanceConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_PREFIX;
import static org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_PREFIX;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class IndexBalanceAllocationDeciderTests extends ESAllocationTestCase {

    private DiscoveryNode indexNodeOne;
    private DiscoveryNode indexNodeTwo;
    private DiscoveryNode searchNodeOne;
    private DiscoveryNode searchNodeTwo;
    private DiscoveryNode masterNode;
    private DiscoveryNode machineLearningNode;

    private RoutingNode routingIndexNodeOne;
    private RoutingNode routingIndexNodeTwo;
    private RoutingNode routingSearchNodeOne;
    private RoutingNode routingSearchNodeTwo;
    private RoutingNode routingMachineLearningNode;

    private List<DiscoveryNode> allNodes;
    private int numberOfPrimaryShards;
    private int replicationFactor;
    private ClusterState clusterState;
    private IndexMetadata indexMetadata;
    private RoutingAllocation routingAllocation;
    private IndexBalanceAllocationDecider indexBalanceAllocationDecider;
    private ShardRouting indexTierShardRouting;
    private ShardRouting searchTierShardRouting;
    private List<RoutingNode> indexTier;
    private List<RoutingNode> searchTier;

    private void setup(Settings clusterSettings, Supplier<Settings> indexSettings) {
        setup(clusterSettings, indexSettings, 2);
    }

    private void setup(Settings clusterSettings, Supplier<Settings> indexSettings, int replicationFactor) {
        assert replicationFactor == 1 || replicationFactor == 2 : "replicationFactor must be 1 or 2";
        final String indexName = "IndexBalanceAllocationDeciderIndex";
        final Map<DiscoveryNode, List<ShardRouting>> nodeToShardRoutings = new HashMap<>();

        Settings settings = Settings.builder()
            .put(clusterSettings)
            .put("stateless.enabled", "true")
            .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), "true")
            .build();

        numberOfPrimaryShards = randomIntBetween(2, 10) * 2;
        this.replicationFactor = replicationFactor;

        indexNodeOne = DiscoveryNodeUtils.builder("indexNodeOne").roles(Collections.singleton(DiscoveryNodeRole.INDEX_ROLE)).build();
        indexNodeTwo = DiscoveryNodeUtils.builder("indexNodeTwo").roles(Collections.singleton(DiscoveryNodeRole.INDEX_ROLE)).build();
        searchNodeOne = DiscoveryNodeUtils.builder("searchNodeOne").roles(Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE)).build();
        searchNodeTwo = DiscoveryNodeUtils.builder("searchNodeTwo").roles(Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE)).build();
        masterNode = DiscoveryNodeUtils.builder("masterNode").roles(Collections.singleton(DiscoveryNodeRole.MASTER_ROLE)).build();
        machineLearningNode = DiscoveryNodeUtils.builder("machineLearningNode")
            .roles(Collections.singleton(DiscoveryNodeRole.ML_ROLE))
            .build();
        allNodes = List.of(indexNodeOne, indexNodeTwo, searchNodeOne, searchNodeTwo, masterNode, machineLearningNode);

        DiscoveryNodes.Builder discoveryNodeBuilder = DiscoveryNodes.builder();
        for (DiscoveryNode node : allNodes) {
            discoveryNodeBuilder.add(node);
        }

        ProjectId projectId = ProjectId.fromId("test-IndexBalanceAllocationDecider");
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test-IndexBalanceAllocationDecider"));

        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);

        indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                indexSettings(IndexVersion.current(), numberOfPrimaryShards, replicationFactor).put(indexSettings.get())
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
                    .build()
            )
            .timestampRange(IndexLongFieldRange.UNKNOWN)
            .eventIngestedRange(IndexLongFieldRange.UNKNOWN)
            .build();

        projectBuilder.put(indexMetadata, false);
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();

        ShardId[] shardIds = new ShardId[numberOfPrimaryShards];
        for (int i = 0; i < numberOfPrimaryShards; i++) {
            shardIds[i] = new ShardId(indexMetadata.getIndex(), i);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = IndexShardRoutingTable.builder(shardIds[i]);

            DiscoveryNode indexNode = i % 2 == 0 ? indexNodeOne : indexNodeTwo;
            ShardRouting primaryShardRouting = TestShardRouting.newShardRouting(
                shardIds[i],
                indexNode.getId(),
                null,
                true,
                ShardRoutingState.STARTED
            );
            indexShardRoutingBuilder.addShard(primaryShardRouting);
            nodeToShardRoutings.putIfAbsent(indexNode, new ArrayList<>());
            nodeToShardRoutings.get(indexNode).add(primaryShardRouting);

            for (int j = 1; j <= replicationFactor; j++) {
                DiscoveryNode searchNode = (i + j) % 2 == 0 ? searchNodeOne : searchNodeTwo;
                ShardRouting replicaShardRouting = shardRoutingBuilder(shardIds[i], searchNode.getId(), false, ShardRoutingState.STARTED)
                    .withRole(ShardRouting.Role.DEFAULT)
                    .build();
                indexShardRoutingBuilder.addShard(replicaShardRouting);
                nodeToShardRoutings.putIfAbsent(searchNode, new ArrayList<>());
                nodeToShardRoutings.get(searchNode).add(replicaShardRouting);
            }
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        routingMachineLearningNode = RoutingNodesHelper.routingNode(machineLearningNode.getId(), machineLearningNode);
        metadataBuilder.put(projectBuilder).generateClusterUuidIfNeeded();
        state.nodes(discoveryNodeBuilder);
        state.metadata(metadataBuilder);
        state.routingTable(GlobalRoutingTable.builder().put(projectId, routingTableBuilder).build());
        clusterState = state.build();
        indexBalanceAllocationDecider = new IndexBalanceAllocationDecider(settings, createBuiltInClusterSettings(settings));

        refreshDerivedState();
    }

    /**
     * A lot of the test setup is derived from the cluster state, regenerate it any time we change the cluster state
     */
    private void refreshDerivedState() {
        routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).build();
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        indexTierShardRouting = randomFrom(
            clusterState.getRoutingNodes().node(randomFrom(indexNodeOne, indexNodeTwo).getId()).copyShards()
        );

        searchTierShardRouting = randomFrom(
            clusterState.getRoutingNodes().node(randomFrom(searchNodeOne, searchNodeTwo).getId()).copyShards()
        );

        routingIndexNodeOne = createRoutingNode(clusterState, indexNodeOne);
        routingIndexNodeTwo = createRoutingNode(clusterState, indexNodeTwo);
        routingSearchNodeOne = createRoutingNode(clusterState, searchNodeOne);
        routingSearchNodeTwo = createRoutingNode(clusterState, searchNodeTwo);

        indexTier = List.of(routingIndexNodeOne, routingIndexNodeTwo);
        searchTier = List.of(routingSearchNodeOne, routingSearchNodeTwo);
    }

    private static RoutingNode createRoutingNode(ClusterState clusterState, DiscoveryNode discoveryNode) {
        return RoutingNodesHelper.routingNode(
            discoveryNode.getId(),
            discoveryNode,
            clusterState.getRoutingNodes().node(discoveryNode.getId()).copyShards()
        );
    }

    public void testCanAllocateUnderThresholdWithExcessShards() {
        Settings clusterSettings = allowExcessShards(Settings.EMPTY);
        setup(clusterSettings, () -> Settings.EMPTY);

        ShardRouting newIndexShardRouting = TestShardRouting.newShardRouting(
            new ShardId("newIndex", "uuid", 1),
            indexNodeTwo.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );

        for (RoutingNode routingNode : List.of(
            routingIndexNodeOne,
            routingIndexNodeTwo,
            routingSearchNodeOne,
            routingSearchNodeTwo,
            routingMachineLearningNode
        )) {
            assertDecisionMatches(
                "Assigning a new index to a node should succeed",
                indexBalanceAllocationDecider.canAllocate(newIndexShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Node does not currently host this index."
            );
        }

        for (RoutingNode routingNode : searchTier) {
            assertDecisionMatches(
                "Assigning a new primary shard to a search tier node should succeed",
                indexBalanceAllocationDecider.canAllocate(indexTierShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "A search node cannot own primary shards. Decider inactive."
            );
        }

        for (RoutingNode routingNode : indexTier) {
            assertDecisionMatches(
                "Assigning a replica shard to a index tier node should succeed",
                indexBalanceAllocationDecider.canAllocate(searchTierShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "An index node cannot own search shards. Decider inactive."
            );
        }

        verifyCanAllocate();
    }

    private void verifyCanAllocate() {
        for (RoutingNode routingNode : indexTier) {
            assertDecisionMatches(
                "Assigning an additional primary shard to an index node has capacity should succeed",
                indexBalanceAllocationDecider.canAllocate(indexTierShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Node index shard allocation is under the threshold."
            );
        }

        for (RoutingNode routingNode : searchTier) {
            assertDecisionMatches(
                "Assigning an additional replica shard to an search node has capacity should succeed",
                indexBalanceAllocationDecider.canAllocate(searchTierShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Node index shard allocation is under the threshold."
            );
        }
    }

    public void testCanAllocateExceedThreshold() {
        setup(Settings.EMPTY, () -> Settings.EMPTY);

        int ideal = numberOfPrimaryShards / 2;
        int current = numberOfPrimaryShards / 2;

        assertDecisionMatches(
            "Assigning an additional primary shard to an index node at capacity should fail",
            indexBalanceAllocationDecider.canAllocate(indexTierShardRouting, routingIndexNodeOne, routingAllocation),
            Decision.Type.NOT_PREFERRED,
            "There are [2] eligible nodes in the [index] tier for assignment of ["
                + numberOfPrimaryShards
                + "] shards in index [[IndexBalanceAllocationDeciderIndex]]. Ideally no more than ["
                + ideal
                + "] shard would be assigned per node (the index balance excess shards setting is [0]). This node is already assigned ["
                + current
                + "] shards of the index."
        );

        int total = numberOfPrimaryShards * replicationFactor;
        ideal = numberOfPrimaryShards * replicationFactor / 2;
        current = numberOfPrimaryShards * replicationFactor / 2;

        assertDecisionMatches(
            "Assigning an additional replica shard to an replica node at capacity should fail",
            indexBalanceAllocationDecider.canAllocate(searchTierShardRouting, routingSearchNodeOne, routingAllocation),
            Decision.Type.NOT_PREFERRED,
            "There are [2] eligible nodes in the [search] tier for assignment of ["
                + total
                + "] shards in index [[IndexBalanceAllocationDeciderIndex]]. Ideally no more than ["
                + ideal
                + "] shard would be assigned per node (the index balance excess shards setting is [0]). This node is already assigned ["
                + current
                + "] shards of the index."
        );
    }

    public void testCanAllocateHasDiscoveryNodeFilters() {
        Settings clusterSettings = addRandomFilterSetting(Settings.EMPTY);
        if (randomBoolean()) {
            clusterSettings = allowExcessShards(clusterSettings);
        }
        setup(clusterSettings, () -> Settings.EMPTY);

        for (RoutingNode routingNode : indexTier) {
            assertDecisionMatches(
                "Having DiscoveryNodeFilters disables this decider",
                indexBalanceAllocationDecider.canAllocate(indexTierShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Decider is disabled."
            );
        }

        for (RoutingNode routingNode : searchTier) {
            assertDecisionMatches(
                "Having DiscoveryNodeFilters disables this decider",
                indexBalanceAllocationDecider.canAllocate(searchTierShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Decider is disabled."
            );
        }
    }

    public void testCanAllocateHasIndexRoutingFilters() {
        setup(Settings.EMPTY, this::addRandomIndexRoutingFilters);

        for (RoutingNode routingNode : indexTier) {
            assertDecisionMatches(
                "Having DiscoveryNodeFilters disables this decider",
                indexBalanceAllocationDecider.canAllocate(indexTierShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Decider is disabled for index level allocation filters."
            );
        }

        for (RoutingNode routingNode : searchTier) {
            assertDecisionMatches(
                "Having DiscoveryNodeFilters disables this decider",
                indexBalanceAllocationDecider.canAllocate(searchTierShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Decider is disabled for index level allocation filters."
            );
        }
    }

    public void testCanRemainReturnsYesWhenIndexRoutingFiltersArePresent() {
        setup(Settings.EMPTY, this::addRandomIndexRoutingFilters);

        assertDecisionMatches(
            "Having index routing filters disables this decider",
            indexBalanceAllocationDecider.canRemain(
                indexMetadata,
                getRandomShardRouting(indexNodeOne),
                routingIndexNodeOne,
                routingAllocation
            ),
            Decision.Type.YES,
            "Decider is disabled for index level allocation filters."
        );

        assertDecisionMatches(
            "Having index routing filters disables this decider",
            indexBalanceAllocationDecider.canRemain(
                indexMetadata,
                getRandomShardRouting(searchNodeOne),
                routingSearchNodeOne,
                routingAllocation
            ),
            Decision.Type.YES,
            "Decider is disabled for index level allocation filters."
        );
    }

    public void testCanRemainReturnsYesWhenClusterRoutingFiltersArePresent() {
        Settings clusterSettings = addRandomFilterSetting(Settings.EMPTY);
        if (randomBoolean()) {
            clusterSettings = allowExcessShards(clusterSettings);
        }
        setup(clusterSettings, () -> Settings.EMPTY);

        assertDecisionMatches(
            "Having cluster routing filters disables this decider",
            indexBalanceAllocationDecider.canRemain(
                indexMetadata,
                getRandomShardRouting(indexNodeOne),
                routingIndexNodeOne,
                routingAllocation
            ),
            Decision.Type.YES,
            "Decider is disabled."
        );

        assertDecisionMatches(
            "Having cluster routing filters disables this decider",
            indexBalanceAllocationDecider.canRemain(
                indexMetadata,
                getRandomShardRouting(searchNodeOne),
                routingSearchNodeOne,
                routingAllocation
            ),
            Decision.Type.YES,
            "Decider is disabled."
        );
    }

    public void testCanRemainWhenAllocationIsAtThreshold() {
        setup(Settings.EMPTY, () -> Settings.EMPTY);

        assertDecisionMatches(
            "No shards need to be moved when we're at the threshold",
            indexBalanceAllocationDecider.canRemain(
                indexMetadata,
                getRandomShardRouting(indexNodeOne),
                routingIndexNodeOne,
                routingAllocation
            ),
            Decision.Type.YES,
            "Node index shard allocation is under the threshold."
        );

        assertDecisionMatches(
            "No shards need to be moved when we're at the threshold",
            indexBalanceAllocationDecider.canRemain(
                indexMetadata,
                getRandomShardRouting(searchNodeOne),
                routingSearchNodeOne,
                routingAllocation
            ),
            Decision.Type.YES,
            "Node index shard allocation is under the threshold."
        );
    }

    public void testCanRemainWhenAllocationIsOverOrUnderThreshold() {
        setup(Settings.EMPTY, () -> Settings.EMPTY, 1);

        // Index node 1 will be under threshold, index node 2 will be over
        moveAShard(indexNodeOne, indexNodeTwo);

        // canRemain = YES when under the threshold (index tier)
        {
            assertDecisionMatches(
                "Shard is allowed to remain because node is under threshold.",
                indexBalanceAllocationDecider.canRemain(
                    indexMetadata,
                    getRandomShardRouting(indexNodeOne),
                    routingIndexNodeOne,
                    routingAllocation
                ),
                Decision.Type.YES,
                "Node index shard allocation is under the threshold."
            );
        }

        // canRemain = NOT_PREFERRED when over the threshold (index tier)
        {
            int idealShards = numberOfPrimaryShards / 2;
            int currentShards = numberOfPrimaryShards / 2 + 1;
            assertDecisionMatches(
                "Shard cannot remain because node is over threshold.",
                indexBalanceAllocationDecider.canRemain(
                    indexMetadata,
                    getRandomShardRouting(indexNodeTwo),
                    routingIndexNodeTwo,
                    routingAllocation
                ),
                Decision.Type.NOT_PREFERRED,
                "There are [2] eligible nodes in the [index] tier for assignment of ["
                    + numberOfPrimaryShards
                    + "] shards in index [[IndexBalanceAllocationDeciderIndex]]. Ideally no more than ["
                    + idealShards
                    + "] shard would be assigned per node (the index balance excess shards setting is [0]). This node is already assigned ["
                    + currentShards
                    + "] shards of the index."
            );
        }

        // Search node 1 will be under the threshold, search node 2 will be over
        moveAShard(searchNodeOne, searchNodeTwo);

        // canRemain = YES when under the threshold (search tier)
        {
            assertDecisionMatches(
                "Shard is allowed to remain because node is under threshold.",
                indexBalanceAllocationDecider.canRemain(
                    indexMetadata,
                    getRandomShardRouting(searchNodeOne),
                    routingSearchNodeOne,
                    routingAllocation
                ),
                Decision.Type.YES,
                "Node index shard allocation is under the threshold."
            );
        }

        // canRemain = NOT_PREFERRED when over the threshold (search tier)
        {
            int idealShards = numberOfPrimaryShards / 2;
            int currentShards = numberOfPrimaryShards / 2 + 1;
            assertDecisionMatches(
                "Shard cannot remain because node is over threshold.",
                indexBalanceAllocationDecider.canRemain(
                    indexMetadata,
                    getRandomShardRouting(searchNodeTwo),
                    routingSearchNodeTwo,
                    routingAllocation
                ),
                Decision.Type.NOT_PREFERRED,
                "There are [2] eligible nodes in the [search] tier for assignment of ["
                    + numberOfPrimaryShards
                    + "] shards in index [[IndexBalanceAllocationDeciderIndex]]. Ideally no more than ["
                    + idealShards
                    + "] shard would be assigned per node (the index balance excess shards setting is [0]). This node is already assigned ["
                    + currentShards
                    + "] shards of the index."
            );
        }
    }

    public void testCanRemainReturnsYesForSplitSourcesWhenIndexIsBeingResharded() {
        setup(Settings.EMPTY, () -> Settings.EMPTY, 1);

        // Both index node 2 and search node 2 will be over the threshold
        moveAShard(indexNodeOne, indexNodeTwo);
        moveAShard(searchNodeOne, searchNodeTwo);

        // The test reads the current index as the result of a 2x split
        final var reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(numberOfPrimaryShards / 2, 2);
        final var reshardingIndexMetadata = IndexMetadata.builder(indexMetadata).reshardingMetadata(reshardingMetadata).build();
        final var projectMetadata = clusterState.metadata().projectFor(indexMetadata.getIndex());
        clusterState = ClusterState.builder(clusterState)
            .putProjectMetadata(ProjectMetadata.builder(projectMetadata).put(reshardingIndexMetadata, true))
            .build();
        refreshDerivedState();

        final var overThresholdExplanation = "There are [2] eligible nodes in the [*] tier for assignment of [*] shards in index *";
        int splitSources = 0;
        int splitTargets = 0;
        for (ShardRouting shardRouting : clusterState.getRoutingNodes().node(indexNodeTwo.getId()).copyShards()) {
            final var decision = indexBalanceAllocationDecider.canRemain(
                reshardingIndexMetadata,
                shardRouting,
                routingIndexNodeTwo,
                routingAllocation
            );
            if (IndexReshardingMetadata.isSplitSource(shardRouting.shardId(), reshardingMetadata)) {
                splitSources++;
                assertDecisionMatches(
                    "Split sources are allowed to remain on an index node over the threshold",
                    decision,
                    Decision.Type.YES,
                    "Decider is disabled for split sources whose targets have not reached HANDOFF yet."
                );
            } else {
                splitTargets++;
                assertDecisionMatches(
                    "Split targets are not preferred to remain on an index node over the threshold",
                    decision,
                    Decision.Type.NOT_PREFERRED,
                    overThresholdExplanation
                );
            }
        }
        assertThat(splitSources, greaterThan(0));
        assertThat(splitTargets, greaterThan(0));

        // Once their targets have reached HANDOFF, split sources are no longer allowed to remain over the threshold
        final var handoffIndexMetadata = IndexMetadata.builder(indexMetadata)
            .reshardingMetadata(
                transitionSplitTargets(
                    reshardingMetadata,
                    indexMetadata.getIndex(),
                    randomFrom(
                        IndexReshardingState.Split.TargetShardState.HANDOFF,
                        IndexReshardingState.Split.TargetShardState.SPLIT,
                        IndexReshardingState.Split.TargetShardState.DONE
                    )
                )
            )
            .build();
        for (ShardRouting shardRouting : clusterState.getRoutingNodes().node(indexNodeTwo.getId()).copyShards()) {
            assertDecisionMatches(
                "Shards of an index whose split targets have reached HANDOFF are not preferred to remain over the threshold",
                indexBalanceAllocationDecider.canRemain(handoffIndexMetadata, shardRouting, routingIndexNodeTwo, routingAllocation),
                Decision.Type.NOT_PREFERRED,
                overThresholdExplanation
            );
        }

        for (ShardRouting shardRouting : clusterState.getRoutingNodes().node(searchNodeTwo.getId()).copyShards()) {
            assertDecisionMatches(
                "Search shards of an index being resharded are not preferred to remain on a search node over the threshold",
                indexBalanceAllocationDecider.canRemain(reshardingIndexMetadata, shardRouting, routingSearchNodeTwo, routingAllocation),
                Decision.Type.NOT_PREFERRED,
                overThresholdExplanation
            );
        }

        // canAllocate still accounts for the resharding index, so that split targets are spread across nodes
        assertDecisionMatches(
            "Assigning an additional primary shard to an index node over the threshold should fail",
            indexBalanceAllocationDecider.canAllocate(indexTierShardRouting, routingIndexNodeTwo, routingAllocation),
            Decision.Type.NOT_PREFERRED,
            "There are [2] eligible nodes in the [index] tier for assignment of [" + numberOfPrimaryShards + "] shards in index *"
        );
    }

    public void testRerouteDoesNotMoveSplitSourcesForIndexBalance() {
        final var settings = Settings.builder()
            .put("stateless.enabled", "true")
            .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), "true")
            // Stateless disables index tier rebalancing in the desired balance simulation
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
            .build();
        final var allocationService = createAllocationService(settings);
        final var projectId = ProjectId.DEFAULT;
        final int sourceShardCount = 4;

        // Start a 4-shard index on two index nodes, two shards on each
        final var initialIndexMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), sourceShardCount, 0))
            // Leave room in the routing shards for the split
            .setRoutingNumShards(sourceShardCount * 2)
            .build();
        final var index = initialIndexMetadata.getIndex();
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("index-node-1", Set.of(DiscoveryNodeRole.INDEX_ROLE)))
                    .add(newNode("index-node-2", Set.of(DiscoveryNodeRole.INDEX_ROLE)))
            )
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId).put(initialIndexMetadata, false)))
            .routingTable(
                GlobalRoutingTable.builder()
                    .put(projectId, RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(initialIndexMetadata))
                    .build()
            )
            .build();
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocationService);
        assertThat(clusterState.getRoutingNodes().node("index-node-1").numberOfOwningShardsForIndex(index), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("index-node-2").numberOfOwningShardsForIndex(index), equalTo(2));

        // Split the index to 8 shards
        final var reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(sourceShardCount, 2);
        final var split = reshardingMetadata.getSplit();
        final var splitIndexMetadata = IndexMetadata.builder(clusterState.metadata().getProject(projectId).index(index))
            .reshardingMetadata(reshardingMetadata)
            .reshardAddShards(reshardingMetadata.shardCountAfter())
            .build();
        final var indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        final var sourceIndexRoutingTable = clusterState.routingTable(projectId).index(index);
        for (int shard = 0; shard < sourceShardCount; shard++) {
            indexRoutingTableBuilder.addIndexShard(new IndexShardRoutingTable.Builder(sourceIndexRoutingTable.shard(shard)));
        }
        for (int shard = sourceShardCount; shard < reshardingMetadata.shardCountAfter(); shard++) {
            indexRoutingTableBuilder.addShard(
                ShardRouting.newUnassigned(
                    new ShardId(index, shard),
                    true,
                    new RecoverySource.ReshardSplitRecoverySource(new ShardId(index, split.sourceShard(shard))),
                    new UnassignedInfo(UnassignedInfo.Reason.RESHARD_ADDED, null),
                    ShardRouting.Role.DEFAULT,
                    RecoveryPriority.UNASSIGNED_PERFORMANCE_SCALING
                )
            );
        }
        clusterState = ClusterState.builder(clusterState)
            .putProjectMetadata(ProjectMetadata.builder(clusterState.metadata().getProject(projectId)).put(splitIndexMetadata, true))
            .putRoutingTable(projectId, RoutingTable.builder(clusterState.routingTable(projectId)).add(indexRoutingTableBuilder).build())
            .build();

        // The targets are spread across both nodes, which brings each of them to the threshold of 4 shards
        clusterState = allocationService.reroute(clusterState, "split", ActionListener.noop());
        assertThat(RoutingNodesHelper.shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), hasSize(4));
        assertThat(clusterState.getRoutingNodes().node("index-node-1").numberOfOwningShardsForIndex(index), equalTo(4));
        assertThat(clusterState.getRoutingNodes().node("index-node-2").numberOfOwningShardsForIndex(index), equalTo(4));

        // A third index node joins while the targets are still recovering, so the threshold drops to 3 shards per node
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("index-node-3", Set.of(DiscoveryNodeRole.INDEX_ROLE))))
            .build();
        clusterState = allocationService.reroute(clusterState, "node joined", ActionListener.noop());
        assertThat(
            "split sources must not be moved for index balance while the split is in progress",
            RoutingNodesHelper.shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING),
            empty()
        );

        // The targets reach HANDOFF before they are started, and the reroute that follows their start rebalances the
        // index across the three nodes, while the index is still being resharded
        final var handoffIndexMetadata = IndexMetadata.builder(clusterState.metadata().getProject(projectId).index(index))
            .reshardingMetadata(transitionSplitTargets(reshardingMetadata, index, IndexReshardingState.Split.TargetShardState.HANDOFF))
            .build();
        clusterState = ClusterState.builder(clusterState)
            .putProjectMetadata(ProjectMetadata.builder(clusterState.metadata().getProject(projectId)).put(handoffIndexMetadata, true))
            .build();
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocationService);
        assertNotNull(clusterState.metadata().getProject(projectId).index(index).getReshardingMetadata());
        for (String nodeId : List.of("index-node-1", "index-node-2", "index-node-3")) {
            assertThat(clusterState.getRoutingNodes().node(nodeId).numberOfOwningShardsForIndex(index), lessThanOrEqualTo(3));
        }
    }

    private static IndexReshardingMetadata transitionSplitTargets(
        IndexReshardingMetadata reshardingMetadata,
        Index index,
        IndexReshardingState.Split.TargetShardState targetShardState
    ) {
        var transitioned = reshardingMetadata;
        for (var state : IndexReshardingState.Split.TargetShardState.values()) {
            if (state != IndexReshardingState.Split.TargetShardState.CLONE && state.compareTo(targetShardState) <= 0) {
                for (int shard = reshardingMetadata.shardCountBefore(); shard < reshardingMetadata.shardCountAfter(); shard++) {
                    transitioned = transitioned.transitionSplitTargetToNewState(new ShardId(index, shard), state);
                }
            }
        }
        return transitioned;
    }

    /**
     * Move a random shard from one node to another
     *
     * @param fromNode The node to move a shard from
     * @param toNode The node to move the shard to
     */
    private void moveAShard(DiscoveryNode fromNode, DiscoveryNode toNode) {
        final var fromRoutingNode = clusterState.getRoutingNodes().node(fromNode.getId());
        final var toRoutingNode = clusterState.getRoutingNodes().node(toNode.getId());
        final var shardToMove = randomFrom(fromRoutingNode.copyShards());
        final var mutableRoutingNodes = clusterState.getRoutingNodes().mutableCopy();
        mutableRoutingNodes.relocateShard(
            shardToMove,
            toRoutingNode.nodeId(),
            randomNonNegativeLong(),
            "test",
            RoutingChangesObserver.NOOP,
            RecoveryPriority.RELOCATION_CAN_REMAIN_NO
        );
        final var updatedGlobalRoutingTable = clusterState.globalRoutingTable().rebuild(mutableRoutingNodes, clusterState.metadata());
        this.clusterState = ClusterState.builder(clusterState).routingTable(updatedGlobalRoutingTable).build();
        refreshDerivedState();
    }

    private ShardRouting getRandomShardRouting(DiscoveryNode node) {
        return randomFrom(clusterState.getRoutingNodes().node(node.getId()).copyShards());
    }

    private Settings addRandomFilterSetting(Settings settings) {
        String setting = randomFrom(
            CLUSTER_ROUTING_REQUIRE_GROUP_PREFIX,
            CLUSTER_ROUTING_INCLUDE_GROUP_PREFIX,
            CLUSTER_ROUTING_EXCLUDE_GROUP_PREFIX
        );
        String attribute = randomFrom("_value", "name");
        String name = randomFrom("indexNodeOne", "indexNodeTwo", "searchNodeOne", "searchNodeTwo");
        String ip = randomFrom("192.168.0.1", "192.168.0.2", "192.168.7.1", "10.17.0.1");
        return Settings.builder().put(settings).put(setting + "." + attribute, attribute.equals("name") ? name : ip).build();
    }

    private Settings allowExcessShards(Settings settings) {
        int excessShards = randomIntBetween(1, 5);

        return Settings.builder()
            .put(settings)
            .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_EXCESS_SHARDS.getKey(), excessShards)
            .build();
    }

    private Settings addRandomIndexRoutingFilters() {
        String setting = randomFrom(
            INDEX_ROUTING_REQUIRE_GROUP_PREFIX,
            INDEX_ROUTING_INCLUDE_GROUP_PREFIX,
            INDEX_ROUTING_EXCLUDE_GROUP_PREFIX
        );
        String attribute = randomFrom("_ip", "_host", "_id");
        String ip = randomFrom("192.168.0.1", "192.168.0.2", "192.168.7.1", "10.17.0.1");
        String id = randomFrom(indexNodeOne.getId(), indexNodeTwo.getId(), searchNodeOne.getId(), searchNodeTwo.getId());
        String hostName = randomFrom(
            indexNodeOne.getHostName(),
            indexNodeTwo.getHostName(),
            searchNodeOne.getHostName(),
            searchNodeTwo.getHostName()
        );

        String value = switch (attribute) {
            case "_ip" -> ip;
            case "_host" -> hostName;
            case "_id" -> id;
            default -> throw new IllegalStateException("Unexpected value: " + attribute);
        };
        return Settings.builder().put(setting + "." + attribute, value).build();
    }

}
