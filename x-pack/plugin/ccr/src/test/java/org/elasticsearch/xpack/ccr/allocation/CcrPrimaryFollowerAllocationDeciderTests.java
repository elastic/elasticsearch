/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.allocation;

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
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;

public class CcrPrimaryFollowerAllocationDeciderTests extends ESAllocationTestCase {

    private ProjectId projectId;
    private int extraProjectCount;

    @Before
    public void setupProject() {
        extraProjectCount = randomIntBetween(0, 3);
        if (extraProjectCount == 0) {
            projectId = Metadata.DEFAULT_PROJECT_ID;
        } else {
            projectId = randomUniqueProjectId();
        }
    }

    public void testRegularIndex() {
        String index = "test-index";
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(index)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1);
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            final Set<DiscoveryNodeRole> roles = new HashSet<>();
            roles.add(DiscoveryNodeRole.DATA_ROLE);
            if (randomBoolean()) {
                roles.add(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
            }
            nodes.add(newNode("node" + i, roles));
        }
        DiscoveryNodes.Builder discoveryNodes = DiscoveryNodes.builder();
        nodes.forEach(discoveryNodes::add);
        final Metadata metadata = createMetadata(indexMetadata);
        final GlobalRoutingTable routingTable = GlobalRoutingTableTestHelper.buildRoutingTable(metadata, (builder, indexMetdata) -> {
            if (randomBoolean()) {
                builder.addAsNew(indexMetdata);
            } else if (randomBoolean()) {
                builder.addAsRecovery(indexMetdata);
            } else if (randomBoolean()) {
                builder.addAsNewRestore(indexMetdata, newSnapshotRecoverySource(), new HashSet<>());
            } else {
                builder.addAsRestore(indexMetdata, newSnapshotRecoverySource());
            }
        });
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.EMPTY_NODES)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        for (int i = 0; i < clusterState.routingTable(projectId).index(index).size(); i++) {
            IndexShardRoutingTable shardRouting = clusterState.routingTable(projectId).index(index).shard(i);
            assertThat(shardRouting.size(), equalTo(2));
            assertThat(shardRouting.primaryShard().state(), equalTo(UNASSIGNED));
            Decision decision = executeAllocation(clusterState, shardRouting.primaryShard(), randomFrom(nodes));
            assertThat(decision.type(), equalTo(Decision.Type.YES));
            assertThat(decision.getExplanation(), equalTo("shard is not a follower and is not under the purview of this decider"));
            for (ShardRouting replica : shardRouting.replicaShards()) {
                assertThat(replica.state(), equalTo(UNASSIGNED));
                decision = executeAllocation(clusterState, replica, randomFrom(nodes));
                assertThat(decision.type(), equalTo(Decision.Type.YES));
                assertThat(decision.getExplanation(), equalTo("shard is not a follower and is not under the purview of this decider"));
            }
        }
    }

    private Metadata createMetadata(IndexMetadata.Builder indexMetadata) {
        final ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).put(indexMetadata).build();
        final Metadata.Builder metadataBuilder = Metadata.builder();
        metadataBuilder.put(projectMetadata);
        for (int i = 0; i < extraProjectCount; i++) {
            metadataBuilder.put(ProjectMetadata.builder(randomUniqueProjectId()).build());
        }
        return metadataBuilder.build();
    }

    public void testAlreadyBootstrappedFollowerIndex() {
        String index = "test-index";
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(index)
            .settings(settings(IndexVersion.current()).put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(1);
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            final Set<DiscoveryNodeRole> roles = new HashSet<>();
            roles.add(DiscoveryNodeRole.DATA_ROLE);
            if (randomBoolean()) {
                roles.add(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
            }
            nodes.add(newNode("node" + i, roles));
        }
        DiscoveryNodes.Builder discoveryNodes = DiscoveryNodes.builder();
        nodes.forEach(discoveryNodes::add);
        final Metadata metadata = createMetadata(indexMetadata);
        final GlobalRoutingTable routingTable = GlobalRoutingTableTestHelper.buildRoutingTable(
            metadata,
            RoutingTable.Builder::addAsRecovery
        );
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        for (int i = 0; i < clusterState.routingTable(projectId).index(index).size(); i++) {
            IndexShardRoutingTable shardRouting = clusterState.routingTable(projectId).index(index).shard(i);
            assertThat(shardRouting.size(), equalTo(2));
            assertThat(shardRouting.primaryShard().state(), equalTo(UNASSIGNED));
            Decision decision = executeAllocation(clusterState, shardRouting.primaryShard(), randomFrom(nodes));
            assertThat(decision.type(), equalTo(Decision.Type.YES));
            assertThat(
                decision.getExplanation(),
                equalTo("shard is a primary follower but was bootstrapped already; hence is not under the purview of this decider")
            );
            for (ShardRouting replica : shardRouting.replicaShards()) {
                assertThat(replica.state(), equalTo(UNASSIGNED));
                decision = executeAllocation(clusterState, replica, randomFrom(nodes));
                assertThat(decision.type(), equalTo(Decision.Type.YES));
                assertThat(decision.getExplanation(), equalTo("shard is a replica follower and is not under the purview of this decider"));
            }
        }
    }

    public void testBootstrappingFollowerIndex() {
        String index = "test-index";
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(index)
            .settings(settings(IndexVersion.current()).put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true))
            .numberOfShards(1)
            .numberOfReplicas(1);
        DiscoveryNode dataOnlyNode = newNode("d1", Set.of(DiscoveryNodeRole.DATA_ROLE));
        DiscoveryNode dataAndRemoteNode = newNode("dr1", Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(dataOnlyNode).add(dataAndRemoteNode).build();
        final Metadata metadata = createMetadata(indexMetadata);
        final GlobalRoutingTable routingTable = GlobalRoutingTableTestHelper.buildRoutingTable(
            metadata,
            (builder, idx) -> builder.addAsNewRestore(idx, newSnapshotRecoverySource(), new HashSet<>())
        );
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        for (int i = 0; i < clusterState.routingTable(projectId).index(index).size(); i++) {
            IndexShardRoutingTable shardRouting = clusterState.routingTable(projectId).index(index).shard(i);
            assertThat(shardRouting.size(), equalTo(2));
            assertThat(shardRouting.primaryShard().state(), equalTo(UNASSIGNED));
            Decision noDecision = executeAllocation(clusterState, shardRouting.primaryShard(), dataOnlyNode);
            assertThat(noDecision.type(), equalTo(Decision.Type.NO));
            assertThat(
                noDecision.getExplanation(),
                equalTo("shard is a primary follower and being bootstrapped, but node does not have the remote_cluster_client role")
            );
            Decision yesDecision = executeAllocation(clusterState, shardRouting.primaryShard(), dataAndRemoteNode);
            assertThat(yesDecision.type(), equalTo(Decision.Type.YES));
            assertThat(yesDecision.getExplanation(), equalTo("shard is a primary follower and node has the remote_cluster_client role"));
            for (ShardRouting replica : shardRouting.replicaShards()) {
                assertThat(replica.state(), equalTo(UNASSIGNED));
                yesDecision = executeAllocation(clusterState, replica, randomFrom(dataOnlyNode, dataAndRemoteNode));
                assertThat(yesDecision.type(), equalTo(Decision.Type.YES));
                assertThat(
                    yesDecision.getExplanation(),
                    equalTo("shard is a replica follower and is not under the purview of this decider")
                );
            }
        }
    }

    static Decision executeAllocation(ClusterState clusterState, ShardRouting shardRouting, DiscoveryNode node) {
        final AllocationDecider decider = new CcrPrimaryFollowerAllocationDecider();
        final RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            RoutingNodes.immutable(clusterState.globalRoutingTable(), clusterState.nodes()),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
        routingAllocation.debugDecision(true);
        return decider.canAllocate(shardRouting, RoutingNodesHelper.routingNode(node.getId(), node), routingAllocation);
    }

    static RecoverySource.SnapshotRecoverySource newSnapshotRecoverySource() {
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("name", "_uuid"));
        return new RecoverySource.SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            snapshot,
            IndexVersion.current(),
            new IndexId("test", UUIDs.randomBase64UUID(random()))
        );
    }
}
