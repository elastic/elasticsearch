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
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
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

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;

public class IndexBalanceAllocationDeciderTests extends ESAllocationTestCase {

    private ProjectId projectId;
    private DiscoveryNode indexNodeOne;
    private DiscoveryNode indexNodeTwo;
    private DiscoveryNode searchNodeOne;
    private DiscoveryNode searchNodeTwo;
    private DiscoveryNode masterNode;

    private RoutingNode routingIndexNodeOne;
    private RoutingNode routingIndexNodeTwo;
    private RoutingNode routingSearchNodeOne;
    private RoutingNode routingSearchNodeTwo;
    private RoutingNode routingMasterNode;

    private List<DiscoveryNode> allNodes;

    private int numberOfPrimaryShards;
    private int replicationFactor;

    private ShardId[] shardIds;
    private final String indexName = "IndexBalanceAllocationDeciderIndex";
    private final Map<DiscoveryNode, List<ShardRouting>> nodeToShardRoutings = new HashMap<>();
    private ClusterState clusterState;
    private IndexMetadata indexMetadata;
    private ShardRouting masterPrimaryShardRouting;

    private void setup(boolean allowMaster) {
        numberOfPrimaryShards = allowMaster ? 11 : 10;
        replicationFactor = 2;

        indexNodeOne = DiscoveryNodeUtils.builder("indexNodeOne").roles(Collections.singleton(DiscoveryNodeRole.INDEX_ROLE)).build();
        indexNodeTwo = DiscoveryNodeUtils.builder("indexNodeTwo").roles(Collections.singleton(DiscoveryNodeRole.INDEX_ROLE)).build();
        searchNodeOne = DiscoveryNodeUtils.builder("searchNodeOne").roles(Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE)).build();
        searchNodeTwo = DiscoveryNodeUtils.builder("searchNodeTwo").roles(Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE)).build();
        masterNode = DiscoveryNodeUtils.builder("masterNode").roles(Collections.singleton(DiscoveryNodeRole.MASTER_ROLE)).build();
        allNodes = List.of(indexNodeOne, indexNodeTwo, searchNodeOne, searchNodeTwo, masterNode);

        DiscoveryNodes.Builder discoveryNodeBuilder = DiscoveryNodes.builder();
        for (DiscoveryNode node : allNodes) {
            discoveryNodeBuilder.add(node);
        }

        projectId = ProjectId.fromId("test-IndexBalanceAllocationDecider");
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test-IndexBalanceAllocationDecider"));

        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                indexSettings(IndexVersion.current(), numberOfPrimaryShards, replicationFactor).put(
                    SETTING_CREATION_DATE,
                    System.currentTimeMillis()
                )
            )
            .timestampRange(IndexLongFieldRange.UNKNOWN)
            .eventIngestedRange(IndexLongFieldRange.UNKNOWN)
            .build();
        projectBuilder.put(indexMetadata, false);
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();

        shardIds = new ShardId[numberOfPrimaryShards];
        int shardCount = allowMaster ? numberOfPrimaryShards - 1 : numberOfPrimaryShards;
        for (int i = 0; i < shardCount; i++) {
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

        if (allowMaster) {
            ShardId lastPrimaryShardId = new ShardId(indexMetadata.getIndex(), numberOfPrimaryShards - 1);
            masterPrimaryShardRouting = TestShardRouting.newShardRouting(
                lastPrimaryShardId,
                masterNode.getId(),
                null,
                true,
                ShardRoutingState.STARTED
            );
            IndexShardRoutingTable.Builder indexShardRoutingBuilderMasterNode = IndexShardRoutingTable.builder(lastPrimaryShardId);
            indexShardRoutingBuilderMasterNode.addShard(masterPrimaryShardRouting);
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilderMasterNode);
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

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
        if (allowMaster) {
            routingMasterNode = RoutingNodesHelper.routingNode(masterNode.getId(), masterNode, masterPrimaryShardRouting);
        } else {
            routingMasterNode = RoutingNodesHelper.routingNode(masterNode.getId(), masterNode);
        }
    }

    public void testCanAllocate_under_threshold() {
        setup(true);

        Settings settings = Settings.builder()
            .put("stateless.enabled", "true")
            .put("cluster.routing.allocation.index_balance_decider.enabled", "true")
            .put("cluster.routing.allocation.index_balance_decider.load_skew_tolerance", 1.0d)
            .build();

        IndexBalanceAllocationDecider indexBalanceAllocationDecider = new IndexBalanceAllocationDecider(
            settings,
            createBuiltInClusterSettings(settings)
        );
        ClusterInfo clusterInfo = ClusterInfo.builder().build();

        var routingAllocation = new RoutingAllocation(
            null,
            RoutingNodes.immutable(clusterState.globalRoutingTable(), clusterState.nodes()),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        ShardRouting newIndexShardRouting = TestShardRouting.newShardRouting(
            new ShardId("newIndex", "uuid", 1),
            indexNodeTwo.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );

        for (RoutingNode routingNode : List.of(routingIndexNodeOne, routingIndexNodeTwo, routingSearchNodeOne, routingSearchNodeTwo)) {
            assertDecisionMatches(
                "Assigning a new index to a node should succeed",
                indexBalanceAllocationDecider.canAllocate(newIndexShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Node does not currently host this index."
            );
        }

        ShardRouting primaryIndexShardRouting = TestShardRouting.newShardRouting(
            new ShardId(indexMetadata.getIndex(), 1),
            indexNodeTwo.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );

        ShardRouting replicaIndexShardRouting = TestShardRouting.newShardRouting(
            new ShardId(indexMetadata.getIndex(), 1),
            searchNodeTwo.getId(),
            null,
            false,
            ShardRoutingState.STARTED
        );

        assertDecisionMatches(
            "Assigning a shard to a node that is not index or search node should succeed",
            indexBalanceAllocationDecider.canAllocate(primaryIndexShardRouting, routingMasterNode, routingAllocation),
            Decision.Type.YES,
            "Node has neither index nor search roles, outside purview."
        );

        for (RoutingNode routingNode : List.of(routingSearchNodeOne, routingSearchNodeTwo)) {
            assertDecisionMatches(
                "Assigning a new primary shard to a search node should succeed",
                indexBalanceAllocationDecider.canAllocate(primaryIndexShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Decider allows primaries move to search nodes."
            );
        }

        for (RoutingNode routingNode : List.of(routingSearchNodeOne, routingSearchNodeTwo)) {
            assertDecisionMatches(
                "Assigning a primary shard to a search node should succeed",
                indexBalanceAllocationDecider.canAllocate(primaryIndexShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Decider allows primaries move to search nodes."
            );
        }

        for (RoutingNode routingNode : List.of(routingIndexNodeOne, routingIndexNodeTwo)) {
            assertDecisionMatches(
                "Assigning a replica shard to a search node should succeed",
                indexBalanceAllocationDecider.canAllocate(replicaIndexShardRouting, routingNode, routingAllocation),
                Decision.Type.YES,
                "Decider allows replicas move to index nodes."
            );
        }

        assertDecisionMatches(
            "Assigning an additional primary shard to an index node has capacity should succeed",
            indexBalanceAllocationDecider.canAllocate(primaryIndexShardRouting, routingIndexNodeOne, routingAllocation),
            Decision.Type.YES,
            "Node index shard allocation is under the threshold."
        );
    }

    public void testCanAllocate_exceed_threshold() {
        setup(false);

        Settings settings = Settings.builder()
            .put("stateless.enabled", "true")
            .put("cluster.routing.allocation.index_balance_decider.enabled", "true")
            .put("cluster.routing.allocation.index_balance_decider.load_skew_tolerance", 1.0d)
            .build();

        IndexBalanceAllocationDecider indexBalanceAllocationDecider = new IndexBalanceAllocationDecider(
            settings,
            createBuiltInClusterSettings(settings)
        );
        ClusterInfo clusterInfo = ClusterInfo.builder().build();

        var routingAllocation = new RoutingAllocation(
            null,
            RoutingNodes.immutable(clusterState.globalRoutingTable(), clusterState.nodes()),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );
        routingAllocation.setDebugMode(RoutingAllocation.DebugMode.ON);

        ShardRouting primaryIndexShardRouting = TestShardRouting.newShardRouting(
            new ShardId(indexMetadata.getIndex(), 1),
            indexNodeTwo.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );

        ShardRouting replicaIndexShardRouting = TestShardRouting.newShardRouting(
            new ShardId(indexMetadata.getIndex(), 1),
            searchNodeTwo.getId(),
            null,
            false,
            ShardRoutingState.STARTED
        );

        assertDecisionMatches(
            "Assigning an additional primary shard to an index node at capacity should fail",
            indexBalanceAllocationDecider.canAllocate(primaryIndexShardRouting, routingIndexNodeOne, routingAllocation),
            Decision.Type.NOT_PREFERRED,
            "For index [[IndexBalanceAllocationDeciderIndex]] with [10] primary shards, Node [indexNodeOne] is "
                + "expected to hold [5] primary shards for index [[IndexBalanceAllocationDeciderIndex]], based on the total of [2]\n"
                + "nodes available. The configured load skew tolerance is [1.00], which yields an allocation threshold of\n"
                + "Math.ceil([5] × [1.00]) = [5] primary shards. Currently, node [indexNodeOne] is assigned [5] primary shards of index "
                + "[[IndexBalanceAllocationDeciderIndex]]. Therefore,\n"
                + "assigning additional primary shards is not preferred.\n"
        );

        assertDecisionMatches(
            "Assigning an additional replica shard to an replica node at capacity should fail",
            indexBalanceAllocationDecider.canAllocate(replicaIndexShardRouting, routingSearchNodeOne, routingAllocation),
            Decision.Type.NOT_PREFERRED,
            "For index [[IndexBalanceAllocationDeciderIndex]] with [20] replicas, Node [searchNodeOne] is "
                + "expected to hold [10] replicas for index [[IndexBalanceAllocationDeciderIndex]], based on the total of [2]\n"
                + "nodes available. The configured load skew tolerance is [1.00], which yields an allocation threshold of\n"
                + "Math.ceil([10] × [1.00]) = [10] replicas. Currently, node [searchNodeOne] is assigned [10] replicas of index "
                + "[[IndexBalanceAllocationDeciderIndex]]. Therefore,\n"
                + "assigning additional replicas is not preferred.\n"
        );
    }

}
