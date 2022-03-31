/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable.Builder;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * Helper methods for generating cluster states
 */
public class ClusterStateCreationUtils {
    /**
     * Creates cluster state with and index that has one shard and #(replicaStates) replicas
     *
     * @param index              name of the index
     * @param activePrimaryLocal if active primary should coincide with the local node in the cluster state
     * @param primaryState       state of primary
     * @param replicaStates      states of the replicas. length of this array determines also the number of replicas
     */
    public static ClusterState state(
        String index,
        boolean activePrimaryLocal,
        ShardRoutingState primaryState,
        ShardRoutingState... replicaStates
    ) {
        final int numberOfReplicas = replicaStates.length;

        int numberOfNodes = numberOfReplicas + 1;
        if (primaryState == ShardRoutingState.RELOCATING) {
            numberOfNodes++;
        }
        for (ShardRoutingState state : replicaStates) {
            if (state == ShardRoutingState.RELOCATING) {
                numberOfNodes++;
            }
        }
        numberOfNodes = Math.max(2, numberOfNodes); // we need a non-local master to test shard failures
        final ShardId shardId = new ShardId(index, "_na_", 0);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        Set<String> unassignedNodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes + 1; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.add(node);
            unassignedNodes.add(node.getId());
        }
        discoBuilder.localNodeId(newNode(0).getId());
        discoBuilder.masterNodeId(newNode(1).getId()); // we need a non-local master to test shard failures
        final int primaryTerm = 1 + randomInt(200);
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .primaryTerm(0, primaryTerm)
            .timestampRange(
                primaryState == ShardRoutingState.STARTED || primaryState == ShardRoutingState.RELOCATING
                    ? IndexLongFieldRange.UNKNOWN
                    : IndexLongFieldRange.NO_SHARDS
            )
            .build();

        IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);

        String primaryNode = null;
        String relocatingNode = null;
        UnassignedInfo unassignedInfo = null;
        if (primaryState != ShardRoutingState.UNASSIGNED) {
            if (activePrimaryLocal) {
                primaryNode = newNode(0).getId();
                unassignedNodes.remove(primaryNode);
            } else {
                Set<String> unassignedNodesExecludingPrimary = new HashSet<>(unassignedNodes);
                unassignedNodesExecludingPrimary.remove(newNode(0).getId());
                primaryNode = selectAndRemove(unassignedNodesExecludingPrimary);
                unassignedNodes.remove(primaryNode);
            }
            if (primaryState == ShardRoutingState.RELOCATING) {
                relocatingNode = selectAndRemove(unassignedNodes);
            } else if (primaryState == ShardRoutingState.INITIALIZING) {
                unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null);
            }
        } else {
            unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null);
        }
        indexShardRoutingBuilder.addShard(
            TestShardRouting.newShardRouting(index, 0, primaryNode, relocatingNode, true, primaryState, unassignedInfo)
        );

        for (ShardRoutingState replicaState : replicaStates) {
            String replicaNode = null;
            relocatingNode = null;
            unassignedInfo = null;
            if (replicaState != ShardRoutingState.UNASSIGNED) {
                assert primaryNode != null : "a replica is assigned but the primary isn't";
                replicaNode = selectAndRemove(unassignedNodes);
                if (replicaState == ShardRoutingState.RELOCATING) {
                    relocatingNode = selectAndRemove(unassignedNodes);
                }
            } else {
                unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null);
            }
            indexShardRoutingBuilder.addShard(
                TestShardRouting.newShardRouting(index, shardId.id(), replicaNode, relocatingNode, false, replicaState, unassignedInfo)
            );
        }
        final IndexShardRoutingTable indexShardRoutingTable = indexShardRoutingBuilder.build();

        IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder(indexMetadata);
        indexMetadataBuilder.putInSyncAllocationIds(
            0,
            indexShardRoutingTable.activeShards()
                .stream()
                .map(ShardRouting::allocationId)
                .map(AllocationId::getId)
                .collect(Collectors.toSet())
        );

        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        state.metadata(Metadata.builder().put(indexMetadataBuilder.build(), false).generateClusterUuidIfNeeded());
        state.routingTable(
            RoutingTable.builder().add(IndexRoutingTable.builder(indexMetadata.getIndex()).addIndexShard(indexShardRoutingTable)).build()
        );
        return state.build();
    }

    /**
     * Creates cluster state with an index that has #(numberOfPrimaries) primary shards in the started state and no replicas.
     * The cluster state contains #(numberOfNodes) nodes and assigns primaries to those nodes.
     */
    public static ClusterState state(String index, final int numberOfNodes, final int numberOfPrimaries) {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        Set<String> nodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.add(node);
            nodes.add(node.getId());
        }
        discoBuilder.localNodeId(newNode(0).getId());
        discoBuilder.masterNodeId(randomFrom(nodes));
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, numberOfPrimaries)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .build();

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int i = 0; i < numberOfPrimaries; i++) {
            ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingBuilder.addShard(
                TestShardRouting.newShardRouting(shardId, randomFrom(nodes), true, ShardRoutingState.STARTED)
            );
            indexRoutingTable.addIndexShard(indexShardRoutingBuilder.build());
        }

        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        state.metadata(Metadata.builder().put(indexMetadata, false).generateClusterUuidIfNeeded());
        state.routingTable(RoutingTable.builder().add(indexRoutingTable).build());
        return state.build();
    }

    /**
     * Creates cluster state with the given indices, each index containing #(numberOfPrimaries)
     * started primary shards and no replicas.  The cluster state contains #(numberOfNodes) nodes
     * and assigns primaries to those nodes.
     */
    public static ClusterState state(final int numberOfNodes, final String[] indices, final int numberOfPrimaries) {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        Set<String> nodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.add(node);
            nodes.add(node.getId());
        }
        discoBuilder.localNodeId(newNode(0).getId());
        discoBuilder.masterNodeId(newNode(0).getId());
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        List<String> nodesList = new ArrayList<>(nodes);
        int currentNodeToAssign = 0;
        for (String index : indices) {
            IndexMetadata indexMetadata = IndexMetadata.builder(index)
                .settings(
                    Settings.builder()
                        .put(SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(SETTING_NUMBER_OF_SHARDS, numberOfPrimaries)
                        .put(SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(SETTING_CREATION_DATE, System.currentTimeMillis())
                )
                .build();

            IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (int i = 0; i < numberOfPrimaries; i++) {
                ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
                indexShardRoutingBuilder.addShard(
                    TestShardRouting.newShardRouting(shardId, nodesList.get(currentNodeToAssign++), true, ShardRoutingState.STARTED)
                );
                if (currentNodeToAssign == nodesList.size()) {
                    currentNodeToAssign = 0;
                }
                indexRoutingTable.addIndexShard(indexShardRoutingBuilder.build());
            }

            metadata.put(indexMetadata, false);
            routingTable.add(indexRoutingTable);
        }
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        state.metadata(metadata.generateClusterUuidIfNeeded().build());
        state.routingTable(routingTable.build());
        return state.build();
    }

    /**
     * Creates cluster state with several shards and one replica and all shards STARTED.
     */
    public static ClusterState stateWithAssignedPrimariesAndOneReplica(String index, int numberOfShards) {

        int numberOfNodes = 2; // we need a non-local master to test shard failures
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes + 1; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.add(node);
        }
        discoBuilder.localNodeId(newNode(0).getId());
        discoBuilder.masterNodeId(newNode(1).getId()); // we need a non-local master to test shard failures
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            )
            .build();
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        state.metadata(Metadata.builder().put(indexMetadata, false).generateClusterUuidIfNeeded());
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int i = 0; i < numberOfShards; i++) {
            final ShardId shardId = new ShardId(index, "_na_", i);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
            indexShardRoutingBuilder.addShard(
                TestShardRouting.newShardRouting(index, i, newNode(0).getId(), null, true, ShardRoutingState.STARTED)
            );
            indexShardRoutingBuilder.addShard(
                TestShardRouting.newShardRouting(index, i, newNode(1).getId(), null, false, ShardRoutingState.STARTED)
            );
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
        }
        state.routingTable(RoutingTable.builder().add(indexRoutingTableBuilder.build()).build());
        return state.build();
    }

    /**
     * Creates cluster state with several indexes, shards and replicas and all shards STARTED.
     */
    public static ClusterState stateWithAssignedPrimariesAndReplicas(String[] indices, int numberOfShards, int numberOfReplicas) {

        int numberOfDataNodes = numberOfReplicas + 1;
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfDataNodes + 1; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.add(node);
            if (i == 0) {
                discoBuilder.localNodeId(node.getId());
            } else if (i == numberOfDataNodes) {
                discoBuilder.masterNodeId(node.getId());
            }
        }
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        Builder routingTableBuilder = RoutingTable.builder();

        Metadata.Builder metadataBuilder = Metadata.builder();

        for (String index : indices) {
            IndexMetadata indexMetadata = IndexMetadata.builder(index)
                .settings(
                    Settings.builder()
                        .put(SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                        .put(SETTING_CREATION_DATE, System.currentTimeMillis())
                )
                .timestampRange(IndexLongFieldRange.UNKNOWN)
                .build();
            metadataBuilder.put(indexMetadata, false).generateClusterUuidIfNeeded();
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (int i = 0; i < numberOfShards; i++) {
                final ShardId shardId = new ShardId(index, "_na_", i);
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
                indexShardRoutingBuilder.addShard(
                    TestShardRouting.newShardRouting(index, i, newNode(0).getId(), null, true, ShardRoutingState.STARTED)
                );
                for (int replica = 0; replica < numberOfReplicas; replica++) {
                    indexShardRoutingBuilder.addShard(
                        TestShardRouting.newShardRouting(index, i, newNode(replica + 1).getId(), null, false, ShardRoutingState.STARTED)
                    );
                }
                indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
            }
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        state.metadata(metadataBuilder);
        state.routingTable(routingTableBuilder.build());
        return state.build();
    }

    /**
     * Creates cluster state with and index that has one shard and as many replicas as numberOfReplicas.
     * Primary will be STARTED in cluster state but replicas will be one of UNASSIGNED, INITIALIZING, STARTED or RELOCATING.
     *
     * @param index              name of the index
     * @param activePrimaryLocal if active primary should coincide with the local node in the cluster state
     * @param numberOfReplicas   number of replicas
     */
    public static ClusterState stateWithActivePrimary(String index, boolean activePrimaryLocal, int numberOfReplicas) {
        int assignedReplicas = randomIntBetween(0, numberOfReplicas);
        return stateWithActivePrimary(index, activePrimaryLocal, assignedReplicas, numberOfReplicas - assignedReplicas);
    }

    /**
     * Creates cluster state with and index that has one shard and as many replicas as numberOfReplicas.
     * Primary will be STARTED in cluster state. Some (unassignedReplicas) will be UNASSIGNED and
     * some (assignedReplicas) will be one of INITIALIZING, STARTED or RELOCATING.
     *
     * @param index              name of the index
     * @param activePrimaryLocal if active primary should coincide with the local node in the cluster state
     * @param assignedReplicas   number of replicas that should have INITIALIZING, STARTED or RELOCATING state
     * @param unassignedReplicas number of replicas that should be unassigned
     */
    public static ClusterState stateWithActivePrimary(
        String index,
        boolean activePrimaryLocal,
        int assignedReplicas,
        int unassignedReplicas
    ) {
        ShardRoutingState[] replicaStates = new ShardRoutingState[assignedReplicas + unassignedReplicas];
        // no point in randomizing - node assignment later on does it too.
        for (int i = 0; i < assignedReplicas; i++) {
            replicaStates[i] = randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED, ShardRoutingState.RELOCATING);
        }
        for (int i = assignedReplicas; i < replicaStates.length; i++) {
            replicaStates[i] = ShardRoutingState.UNASSIGNED;
        }
        return state(index, activePrimaryLocal, randomFrom(ShardRoutingState.STARTED, ShardRoutingState.RELOCATING), replicaStates);
    }

    /**
     * Creates a cluster state with no index
     */
    public static ClusterState stateWithNoShard() {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        final DiscoveryNode localNode = newNode(0);
        discoBuilder.add(localNode);
        discoBuilder.localNodeId(localNode.getId());
        final DiscoveryNode masterNode = newNode(1);
        discoBuilder.add(masterNode);
        discoBuilder.masterNodeId(masterNode.getId());
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        state.metadata(Metadata.builder().generateClusterUuidIfNeeded());
        state.routingTable(RoutingTable.builder().build());
        return state.build();
    }

    /**
     * Creates a cluster state where local node and master node can be specified
     *
     * @param localNode  node in allNodes that is the local node
     * @param masterNode node in allNodes that is the master node. Can be null if no master exists
     * @param allNodes   all nodes in the cluster
     * @return cluster state
     */
    public static ClusterState state(DiscoveryNode localNode, DiscoveryNode masterNode, DiscoveryNode... allNodes) {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        for (DiscoveryNode node : allNodes) {
            discoBuilder.add(node);
        }
        if (masterNode != null) {
            discoBuilder.masterNodeId(masterNode.getId());
            discoBuilder.add(masterNode);
        }
        discoBuilder.localNodeId(localNode.getId());

        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        state.metadata(Metadata.builder().generateClusterUuidIfNeeded());
        return state.build();
    }

    private static DiscoveryNode newNode(int nodeId) {
        return new DiscoveryNode(
            "node_" + nodeId,
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(DiscoveryNodeRole.roles()),
            Version.CURRENT
        );
    }

    private static String selectAndRemove(Set<String> strings) {
        String selection = randomFrom(strings.toArray(new String[strings.size()]));
        strings.remove(selection);
        return selection;
    }
}
