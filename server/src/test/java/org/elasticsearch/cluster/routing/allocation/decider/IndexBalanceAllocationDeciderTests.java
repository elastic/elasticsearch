/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.IndexBalanceConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private List<RoutingNode> searchIier;

    private void setup(Settings clusterSettings, Supplier<Settings> indexSettings) {
        final String indexName = "IndexBalanceAllocationDeciderIndex";
        final Map<DiscoveryNode, List<ShardRouting>> nodeToShardRoutings = new HashMap<>();

        Settings.Builder builder = Settings.builder()
            .put(clusterSettings)
            .put("stateless.enabled", "true")
            .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), "true");

        numberOfPrimaryShards = randomIntBetween(2, 10) * 2;
        replicationFactor = 2;

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
                DiscoveryNode searchNode = j % 2 == 0 ? searchNodeOne : searchNodeTwo;
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

        routingIndexNodeOne = RoutingNodesHelper.routingNode(
            indexNodeOne.getId(),
            indexNodeOne,
            nodeToShardRoutings.get(indexNodeOne).toArray(new ShardRouting[0])
        );
        routingIndexNodeTwo = RoutingNodesHelper.routingNode(
            indexNodeTwo.getId(),
            indexNodeTwo,
            nodeToShardRoutings.get(indexNodeTwo).toArray(new ShardRouting[0])
        );
        routingSearchNodeOne = RoutingNodesHelper.routingNode(
            searchNodeOne.getId(),
            searchNodeOne,
            nodeToShardRoutings.get(searchNodeOne).toArray(new ShardRouting[0])
        );
        routingSearchNodeTwo = RoutingNodesHelper.routingNode(
            searchNodeTwo.getId(),
            searchNodeTwo,
            nodeToShardRoutings.get(searchNodeTwo).toArray(new ShardRouting[0])
        );

        ClusterInfo clusterInfo = ClusterInfo.builder().build();
        routingAllocation = new RoutingAllocation(null, clusterState.getRoutingNodes(), clusterState, clusterInfo, null, System.nanoTime());
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        indexBalanceAllocationDecider = new IndexBalanceAllocationDecider(builder.build(), createBuiltInClusterSettings(builder.build()));

        indexTierShardRouting = TestShardRouting.newShardRouting(
            new ShardId(indexMetadata.getIndex(), 1),
            randomFrom(indexNodeOne, indexNodeTwo).getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );

        searchTierShardRouting = TestShardRouting.newShardRouting(
            new ShardId(indexMetadata.getIndex(), 1),
            randomFrom(searchNodeOne, searchNodeTwo).getId(),
            null,
            false,
            ShardRoutingState.STARTED
        );

        indexTier = List.of(routingIndexNodeOne, routingIndexNodeTwo);
        searchIier = List.of(routingSearchNodeOne, routingSearchNodeTwo);
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

        for (RoutingNode routingNode : searchIier) {
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

        for (RoutingNode routingNode : searchIier) {
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

        for (RoutingNode routingNode : searchIier) {
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

        for (RoutingNode routingNode : searchIier) {
            assertDecisionMatches(
                "Having DiscoveryNodeFilters disables this decider",
                indexBalanceAllocationDecider.canAllocate(searchTierShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Decider is disabled for index level allocation filters."
            );
        }
    }

    public Settings addRandomFilterSetting(Settings settings) {
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

    public Settings allowExcessShards(Settings settings) {
        int excessShards = randomIntBetween(1, 5);

        return Settings.builder()
            .put(settings)
            .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_EXCESS_SHARDS.getKey(), excessShards)
            .build();
    }

    public Settings addRandomIndexRoutingFilters() {
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
