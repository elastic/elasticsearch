/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.selection.HealthNodeTaskParams;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.test.ESTestCase.indexSettings;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
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
        return state(Metadata.DEFAULT_PROJECT_ID, index, activePrimaryLocal, primaryState, replicaStates);
    }

    /**
     * Creates cluster state with an index that has one shard and #(replicaStates) replicas
     *
     * @param projectId          project to create index in
     * @param index              name of the index
     * @param activePrimaryLocal if active primary should coincide with the local node in the cluster state
     * @param primaryState       state of primary
     * @param replicaStates      states of the replicas. length of this array determines also the number of replicas
     */
    public static ClusterState state(
        ProjectId projectId,
        String index,
        boolean activePrimaryLocal,
        ShardRoutingState primaryState,
        ShardRoutingState... replicaStates
    ) {
        return state(
            projectId,
            index,
            activePrimaryLocal,
            primaryState,
            Arrays.stream(replicaStates).map(shardRoutingState -> new Tuple<>(shardRoutingState, ShardRouting.Role.DEFAULT)).toList()
        );
    }

    /**
     * Creates cluster state with and index that has one shard and #(replicaStates) replicas with given roles
     *
     * @param index              name of the index
     * @param activePrimaryLocal if active primary should coincide with the local node in the cluster state
     * @param primaryState       state of primary
     * @param replicaStates      states and roles of the replicas. length of this collection determines also the number of replicas
     */
    public static ClusterState state(
        String index,
        boolean activePrimaryLocal,
        ShardRoutingState primaryState,
        List<Tuple<ShardRoutingState, ShardRouting.Role>> replicaStates
    ) {
        return state(index, activePrimaryLocal, primaryState, ShardRouting.Role.DEFAULT, replicaStates);
    }

    /**
     * Creates cluster state with an index that has one shard and #(replicaStates) replicas with given roles
     *
     * @param projectId          project to create index in
     * @param index              name of the index
     * @param activePrimaryLocal if active primary should coincide with the local node in the cluster state
     * @param primaryState       state of primary
     * @param replicaStates      states and roles of the replicas. length of this collection determines also the number of replicas
     */
    public static ClusterState state(
        ProjectId projectId,
        String index,
        boolean activePrimaryLocal,
        ShardRoutingState primaryState,
        List<Tuple<ShardRoutingState, ShardRouting.Role>> replicaStates
    ) {
        return state(projectId, index, activePrimaryLocal, primaryState, ShardRouting.Role.DEFAULT, replicaStates);
    }

    /**
     * Creates cluster state with an index that has one shard and #(replicaStates) replicas with given roles
     *
     * @param index              name of the index
     * @param activePrimaryLocal if active primary should coincide with the local node in the cluster state
     * @param primaryState       state of primary
     * @param primaryRole        role of primary
     * @param replicaStates      states and roles of the replicas. length of this collection determines also the number of replicas
     */
    public static ClusterState state(
        String index,
        boolean activePrimaryLocal,
        ShardRoutingState primaryState,
        ShardRouting.Role primaryRole,
        List<Tuple<ShardRoutingState, ShardRouting.Role>> replicaStates
    ) {
        return state(Metadata.DEFAULT_PROJECT_ID, index, activePrimaryLocal, primaryState, primaryRole, replicaStates);
    }

    /**
     * Creates cluster state with an index that has one shard and #(replicaStates) replicas with given roles
     *
     * @param projectId          project to create index in
     * @param index              name of the index
     * @param activePrimaryLocal if active primary should coincide with the local node in the cluster state
     * @param primaryState       state of primary
     * @param primaryRole        role of primary
     * @param replicaStates      states and roles of the replicas. length of this collection determines also the number of replicas
     */
    public static ClusterState state(
        ProjectId projectId,
        String index,
        boolean activePrimaryLocal,
        ShardRoutingState primaryState,
        ShardRouting.Role primaryRole,
        List<Tuple<ShardRoutingState, ShardRouting.Role>> replicaStates
    ) {
        assert primaryState == ShardRoutingState.STARTED
            || primaryState == ShardRoutingState.RELOCATING
            || replicaStates.stream().allMatch(s -> s.v1() == ShardRoutingState.UNASSIGNED)
            : "invalid shard states ["
                + primaryState
                + "] vs ["
                + Arrays.toString(replicaStates.stream().map(Tuple::v1).toArray(String[]::new))
                + "]";

        final int numberOfReplicas = replicaStates.size();

        int numberOfNodes = numberOfReplicas + 1;
        if (primaryState == ShardRoutingState.RELOCATING) {
            numberOfNodes++;
        }
        for (var state : replicaStates) {
            if (state.v1() == ShardRoutingState.RELOCATING) {
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
        IndexLongFieldRange timeFieldRange = primaryState == ShardRoutingState.STARTED || primaryState == ShardRoutingState.RELOCATING
            ? IndexLongFieldRange.UNKNOWN
            : IndexLongFieldRange.NO_SHARDS;

        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(indexSettings(IndexVersion.current(), 1, numberOfReplicas).put(SETTING_CREATION_DATE, System.currentTimeMillis()))
            .primaryTerm(0, primaryTerm)
            .timestampRange(timeFieldRange)
            .eventIngestedRange(timeFieldRange)
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
            shardRoutingBuilder(index, 0, primaryNode, true, primaryState).withRelocatingNodeId(relocatingNode)
                .withUnassignedInfo(unassignedInfo)
                .withRole(primaryRole)
                .build()
        );

        for (var replicaState : replicaStates) {
            String replicaNode = null;
            relocatingNode = null;
            unassignedInfo = null;
            if (replicaState.v1() != ShardRoutingState.UNASSIGNED) {
                assert primaryNode != null : "a replica is assigned but the primary isn't";
                replicaNode = selectAndRemove(unassignedNodes);
                if (replicaState.v1() == ShardRoutingState.RELOCATING) {
                    relocatingNode = selectAndRemove(unassignedNodes);
                }
            } else {
                unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null);
            }
            indexShardRoutingBuilder.addShard(
                shardRoutingBuilder(index, shardId.id(), replicaNode, false, replicaState.v1()).withRelocatingNodeId(relocatingNode)
                    .withUnassignedInfo(unassignedInfo)
                    .withRole(replicaState.v2())
                    .build()
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
        state.metadata(
            Metadata.builder()
                .put(ProjectMetadata.builder(projectId).put(indexMetadataBuilder.build(), false))
                .generateClusterUuidIfNeeded()
        );
        state.routingTable(
            GlobalRoutingTable.builder()
                .put(
                    projectId,
                    RoutingTable.builder().add(IndexRoutingTable.builder(indexMetadata.getIndex()).addIndexShard(indexShardRoutingBuilder))
                )
                .build()
        );
        return state.build();
    }

    /**
     * Creates cluster state with an index that has #(numberOfPrimaries) primary shards in the started state and no replicas.
     * The cluster state contains #(numberOfNodes) nodes and assigns primaries to those nodes.
     */
    public static ClusterState state(String index, int numberOfNodes, int numberOfPrimaries) {
        return state(Metadata.DEFAULT_PROJECT_ID, index, numberOfNodes, numberOfPrimaries);
    }

    /**
     * Creates cluster state with an index that has #(numberOfPrimaries) primary shards in the started state and no replicas.
     * The cluster state contains #(numberOfNodes) nodes and assigns primaries to those nodes.
     */
    public static ClusterState state(ProjectId projectId, String index, int numberOfNodes, int numberOfPrimaries) {
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
            .settings(indexSettings(IndexVersion.current(), numberOfPrimaries, 0).put(SETTING_CREATION_DATE, System.currentTimeMillis()))
            .build();

        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int i = 0; i < numberOfPrimaries; i++) {
            ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = IndexShardRoutingTable.builder(shardId);
            indexShardRoutingBuilder.addShard(
                TestShardRouting.newShardRouting(shardId, randomFrom(nodes), true, ShardRoutingState.STARTED)
            );
            indexRoutingTable.addIndexShard(indexShardRoutingBuilder);
        }

        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        state.metadata(Metadata.builder().put(ProjectMetadata.builder(projectId).put(indexMetadata, false)).generateClusterUuidIfNeeded());
        state.routingTable(GlobalRoutingTable.builder().put(projectId, RoutingTable.builder().add(indexRoutingTable)).build());
        return state.build();
    }

    /**
     * Creates cluster state with the given indices, each index containing #(numberOfPrimaries)
     * started primary shards and no replicas.  The cluster state contains #(numberOfNodes) nodes
     * and assigns primaries to those nodes.
     */
    public static ClusterState state(int numberOfNodes, String[] indices, int numberOfPrimaries) {
        return state(Metadata.DEFAULT_PROJECT_ID, numberOfNodes, indices, numberOfPrimaries);
    }

    /**
     * Creates cluster state with the given indices, each index containing #(numberOfPrimaries)
     * started primary shards and no replicas.  The cluster state contains #(numberOfNodes) nodes
     * and assigns primaries to those nodes.
     */
    public static ClusterState state(ProjectId projectId, int numberOfNodes, String[] indices, int numberOfPrimaries) {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        Set<String> nodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.add(node);
            nodes.add(node.getId());
        }
        discoBuilder.localNodeId(newNode(0).getId());
        discoBuilder.masterNodeId(newNode(0).getId());
        ProjectMetadata.Builder projectMetadata = ProjectMetadata.builder(projectId);
        RoutingTable.Builder routingTable = RoutingTable.builder();
        List<String> nodesList = new ArrayList<>(nodes);
        int currentNodeToAssign = 0;
        for (String index : indices) {
            IndexMetadata indexMetadata = IndexMetadata.builder(index)
                .settings(
                    indexSettings(IndexVersion.current(), numberOfPrimaries, 0).put(SETTING_CREATION_DATE, System.currentTimeMillis())
                )
                .eventIngestedRange(IndexLongFieldRange.UNKNOWN)
                .build();

            IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (int i = 0; i < numberOfPrimaries; i++) {
                ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = IndexShardRoutingTable.builder(shardId);
                indexShardRoutingBuilder.addShard(
                    TestShardRouting.newShardRouting(shardId, nodesList.get(currentNodeToAssign++), true, ShardRoutingState.STARTED)
                );
                if (currentNodeToAssign == nodesList.size()) {
                    currentNodeToAssign = 0;
                }
                indexRoutingTable.addIndexShard(indexShardRoutingBuilder);
            }

            projectMetadata.put(indexMetadata, false);
            routingTable.add(indexRoutingTable);
        }
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        state.metadata(Metadata.builder().put(projectMetadata).generateClusterUuidIfNeeded().build());
        state.routingTable(GlobalRoutingTable.builder().put(projectId, routingTable).build());
        return state.build();
    }

    /**
     * Creates cluster state with several shards and one replica and all shards STARTED.
     */
    public static ClusterState stateWithAssignedPrimariesAndOneReplica(String index, int numberOfShards) {
        return stateWithAssignedPrimariesAndOneReplica(Metadata.DEFAULT_PROJECT_ID, index, numberOfShards);
    }

    /**
     * Creates cluster state with several shards and one replica and all shards STARTED.
     */
    public static ClusterState stateWithAssignedPrimariesAndOneReplica(ProjectId projectId, String index, int numberOfShards) {

        int numberOfNodes = 2; // we need a non-local master to test shard failures
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes + 1; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.add(node);
        }
        discoBuilder.localNodeId(newNode(0).getId());
        discoBuilder.masterNodeId(newNode(1).getId()); // we need a non-local master to test shard failures
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
            .settings(indexSettings(IndexVersion.current(), numberOfShards, 1).put(SETTING_CREATION_DATE, System.currentTimeMillis()))
            .build();
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.nodes(discoBuilder);
        state.metadata(Metadata.builder().put(ProjectMetadata.builder(projectId).put(indexMetadata, false)).generateClusterUuidIfNeeded());
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int i = 0; i < numberOfShards; i++) {
            final ShardId shardId = new ShardId(index, "_na_", i);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = IndexShardRoutingTable.builder(shardId);
            indexShardRoutingBuilder.addShard(
                TestShardRouting.newShardRouting(index, i, newNode(0).getId(), null, true, ShardRoutingState.STARTED)
            );
            indexShardRoutingBuilder.addShard(
                TestShardRouting.newShardRouting(index, i, newNode(1).getId(), null, false, ShardRoutingState.STARTED)
            );
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder);
        }
        state.routingTable(
            GlobalRoutingTable.builder().put(projectId, RoutingTable.builder().add(indexRoutingTableBuilder.build())).build()
        );
        return state.build();
    }

    /**
     * Creates cluster state with several indexes, shards and replicas and all shards STARTED.
     */
    public static ClusterState stateWithAssignedPrimariesAndReplicas(String[] indices, int numberOfShards, int numberOfReplicas) {
        return stateWithAssignedPrimariesAndReplicas(Metadata.DEFAULT_PROJECT_ID, indices, numberOfShards, numberOfReplicas);
    }

    /**
     * Creates cluster state with several indexes, shards and replicas and all shards STARTED.
     */
    public static ClusterState stateWithAssignedPrimariesAndReplicas(
        ProjectId projectId,
        String[] indices,
        int numberOfShards,
        int numberOfReplicas
    ) {
        return stateWithAssignedPrimariesAndReplicas(
            projectId,
            indices,
            numberOfShards,
            Collections.nCopies(numberOfReplicas, ShardRouting.Role.DEFAULT)
        );
    }

    /**
     * Creates cluster state with several indexes, shards and replicas (with given roles) and all shards STARTED.
     */
    public static ClusterState stateWithAssignedPrimariesAndReplicas(
        String[] indices,
        int numberOfShards,
        List<ShardRouting.Role> replicaRoles
    ) {
        return stateWithAssignedPrimariesAndReplicas(Metadata.DEFAULT_PROJECT_ID, indices, numberOfShards, replicaRoles);
    }

    /**
     * Creates cluster state with several indexes, shards and replicas (with given roles) and all shards STARTED.
     */
    public static ClusterState stateWithAssignedPrimariesAndReplicas(
        ProjectId projectId,
        String[] indices,
        int numberOfShards,
        List<ShardRouting.Role> replicaRoles
    ) {
        int numberOfDataNodes = replicaRoles.size() + 1;
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
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        Metadata.Builder metadataBuilder = Metadata.builder();
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        for (String index : indices) {
            IndexMetadata indexMetadata = IndexMetadata.builder(index)
                .settings(
                    indexSettings(IndexVersion.current(), numberOfShards, replicaRoles.size()).put(
                        SETTING_CREATION_DATE,
                        System.currentTimeMillis()
                    )
                )
                .timestampRange(IndexLongFieldRange.UNKNOWN)
                .eventIngestedRange(IndexLongFieldRange.UNKNOWN)
                .build();
            projectBuilder.put(indexMetadata, false);
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (int i = 0; i < numberOfShards; i++) {
                final ShardId shardId = new ShardId(index, "_na_", i);
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = IndexShardRoutingTable.builder(shardId);
                indexShardRoutingBuilder.addShard(
                    TestShardRouting.newShardRouting(index, i, newNode(0).getId(), null, true, ShardRoutingState.STARTED)
                );
                for (int replica = 0; replica < replicaRoles.size(); replica++) {
                    indexShardRoutingBuilder.addShard(
                        shardRoutingBuilder(index, i, newNode(replica + 1).getId(), false, ShardRoutingState.STARTED).withRole(
                            replicaRoles.get(replica)
                        ).build()
                    );
                }
                indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder);
            }
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }

        metadataBuilder.put(projectBuilder).generateClusterUuidIfNeeded();

        state.metadata(metadataBuilder);
        state.routingTable(GlobalRoutingTable.builder().put(projectId, routingTableBuilder).build());
        return state.build();
    }

    public static Tuple<ProjectMetadata.Builder, RoutingTable.Builder> projectWithAssignedPrimariesAndReplicas(
        ProjectId projectId,
        String[] indices,
        int numberOfShards,
        int numberOfReplicas,
        DiscoveryNodes nodes

    ) {
        return projectWithAssignedPrimariesAndReplicas(
            projectId,
            indices,
            numberOfShards,
            Collections.nCopies(numberOfReplicas, ShardRouting.Role.DEFAULT),
            nodes
        );
    }

    private static Tuple<ProjectMetadata.Builder, RoutingTable.Builder> projectWithAssignedPrimariesAndReplicas(
        ProjectId projectId,
        String[] indices,
        int numberOfShards,
        List<ShardRouting.Role> replicaRoles,
        DiscoveryNodes nodes
    ) {
        final ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        final RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        final Iterator<DiscoveryNode> nodeIterator = Iterators.cycling(nodes);
        for (String index : indices) {
            final String uuid = UUIDs.base64UUID();
            IndexMetadata indexMetadata = IndexMetadata.builder(index)
                .settings(
                    indexSettings(IndexVersion.current(), numberOfShards, replicaRoles.size()).put(
                        SETTING_CREATION_DATE,
                        System.currentTimeMillis()
                    ).put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                )
                .timestampRange(IndexLongFieldRange.UNKNOWN)
                .eventIngestedRange(IndexLongFieldRange.UNKNOWN)
                .build();
            projectBuilder.put(indexMetadata, false);
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (int i = 0; i < numberOfShards; i++) {
                final ShardId shardId = new ShardId(index, uuid, i);
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = IndexShardRoutingTable.builder(shardId);
                indexShardRoutingBuilder.addShard(
                    TestShardRouting.newShardRouting(shardId, nodeIterator.next().getId(), null, true, ShardRoutingState.STARTED)
                );
                for (int replica = 0; replica < replicaRoles.size(); replica++) {
                    indexShardRoutingBuilder.addShard(
                        shardRoutingBuilder(shardId, nodeIterator.next().getId(), false, ShardRoutingState.STARTED).withRole(
                            replicaRoles.get(replica)
                        ).build()
                    );
                }
                indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder);
            }
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        return new Tuple<>(projectBuilder, routingTableBuilder);
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
        return stateWithActivePrimary(Metadata.DEFAULT_PROJECT_ID, index, activePrimaryLocal, numberOfReplicas);
    }

    /**
     * Creates cluster state with and index that has one shard and as many replicas as numberOfReplicas.
     * Primary will be STARTED in cluster state but replicas will be one of UNASSIGNED, INITIALIZING, STARTED or RELOCATING.
     *
     * @param projectId          project to create index in
     * @param index              name of the index
     * @param activePrimaryLocal if active primary should coincide with the local node in the cluster state
     * @param numberOfReplicas   number of replicas
     */
    public static ClusterState stateWithActivePrimary(ProjectId projectId, String index, boolean activePrimaryLocal, int numberOfReplicas) {
        int assignedReplicas = randomIntBetween(0, numberOfReplicas);
        return stateWithActivePrimary(projectId, index, activePrimaryLocal, assignedReplicas, numberOfReplicas - assignedReplicas);
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
        return stateWithActivePrimary(Metadata.DEFAULT_PROJECT_ID, index, activePrimaryLocal, assignedReplicas, unassignedReplicas);
    }

    /**
     * Creates cluster state with and index that has one shard and as many replicas as numberOfReplicas.
     * Primary will be STARTED in cluster state. Some (unassignedReplicas) will be UNASSIGNED and
     * some (assignedReplicas) will be one of INITIALIZING, STARTED or RELOCATING.
     *
     * @param projectId          project to create index in
     * @param index              name of the index
     * @param activePrimaryLocal if active primary should coincide with the local node in the cluster state
     * @param assignedReplicas   number of replicas that should have INITIALIZING, STARTED or RELOCATING state
     * @param unassignedReplicas number of replicas that should be unassigned
     */
    public static ClusterState stateWithActivePrimary(
        ProjectId projectId,
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
        return state(
            projectId,
            index,
            activePrimaryLocal,
            randomFrom(ShardRoutingState.STARTED, ShardRoutingState.RELOCATING),
            replicaStates
        );
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
        state.routingTable(GlobalRoutingTable.builder().build());
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
        return state(localNode, masterNode, null, allNodes);
    }

    /**
     * Creates a cluster state where local node and master node can be specified
     *
     * @param localNode  node in allNodes that is the local node
     * @param masterNode node in allNodes that is the master node. Can be null if no master exists
     * @param allNodes   all nodes in the cluster
     * @param transportVersion  the transport version used by the cluster
     * @return cluster state
     */
    public static ClusterState state(
        DiscoveryNode localNode,
        DiscoveryNode masterNode,
        DiscoveryNode[] allNodes,
        TransportVersion transportVersion
    ) {
        return state(localNode, masterNode, null, allNodes, transportVersion);
    }

    /**
     * Creates a cluster state where local node, master and health node can be specified
     *
     * @param localNode  node in allNodes that is the local node
     * @param masterNode node in allNodes that is the master node. Can be null if no master exists
     * @param healthNode node in allNodes that is the health node. Can be null if no health node exists
     * @param allNodes   all nodes in the cluster
     * @return cluster state
     */
    public static ClusterState state(
        DiscoveryNode localNode,
        DiscoveryNode masterNode,
        DiscoveryNode healthNode,
        DiscoveryNode... allNodes
    ) {
        return state(localNode, masterNode, healthNode, allNodes, TransportVersion.current());
    }

    /**
     * Creates a cluster state where local node, master and health node can be specified
     *
     * @param localNode  node in allNodes that is the local node
     * @param masterNode node in allNodes that is the master node. Can be null if no master exists
     * @param healthNode node in allNodes that is the health node. Can be null if no health node exists
     * @param allNodes   all nodes in the cluster
     * @param transportVersion  the transport version used by the cluster
     * @return cluster state
     */
    public static ClusterState state(
        DiscoveryNode localNode,
        DiscoveryNode masterNode,
        DiscoveryNode healthNode,
        DiscoveryNode[] allNodes,
        TransportVersion transportVersion
    ) {
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
        for (DiscoveryNode node : allNodes) {
            state.putCompatibilityVersions(node.getId(), transportVersion, SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS);
        }

        Metadata.Builder metadataBuilder = Metadata.builder().generateClusterUuidIfNeeded();
        if (healthNode != null) {
            addHealthNode(metadataBuilder, healthNode);
        }
        state.metadata(metadataBuilder);
        return state.build();
    }

    private static DiscoveryNode newNode(int nodeId) {
        return DiscoveryNodeUtils.create("node_" + nodeId);
    }

    private static String selectAndRemove(Set<String> strings) {
        String selection = randomFrom(strings.toArray(new String[strings.size()]));
        strings.remove(selection);
        return selection;
    }

    private static Metadata.Builder addHealthNode(Metadata.Builder metadataBuilder, DiscoveryNode healthNode) {
        ClusterPersistentTasksCustomMetadata.Builder tasks = ClusterPersistentTasksCustomMetadata.builder();
        PersistentTasksCustomMetadata.Assignment assignment = new PersistentTasksCustomMetadata.Assignment(
            healthNode.getId(),
            randomAlphaOfLength(10)
        );
        tasks.addTask(HealthNode.TASK_NAME, HealthNode.TASK_NAME, HealthNodeTaskParams.INSTANCE, assignment);
        return metadataBuilder.putCustom(ClusterPersistentTasksCustomMetadata.TYPE, tasks.build());
    }
}
