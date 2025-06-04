/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class RetryFailedAllocationTests extends ESAllocationTestCase {

    private MockAllocationService strategy;
    private ProjectId projectId;
    private ClusterState clusterState;
    private final String INDEX_NAME = "index";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        projectId = randomProjectIdOrDefault();
        ProjectMetadata project = ProjectMetadata.builder(projectId)
            .put(IndexMetadata.builder(INDEX_NAME).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(project.index(INDEX_NAME))
            .build();
        clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(project)
            .putRoutingTable(projectId, routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        strategy = createAllocationService(Settings.EMPTY);
    }

    private ShardRouting getPrimary() {
        return clusterState.routingTable(projectId).index(INDEX_NAME).shard(0).primaryShard();
    }

    private ShardRouting getReplica() {
        return clusterState.routingTable(projectId).index(INDEX_NAME).shard(0).replicaShards().get(0);
    }

    public void testRetryFailedResetForAllocationCommands() {
        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        clusterState = strategy.reroute(clusterState, "initial allocation", ActionListener.noop());
        clusterState = startShardsAndReroute(strategy, clusterState, getPrimary());

        // Exhaust all replica allocation attempts with shard failures
        for (int i = 0; i < retries; i++) {
            List<FailedShard> failedShards = Collections.singletonList(
                new FailedShard(getReplica(), "failing-shard::attempt-" + i, new ElasticsearchException("simulated"), randomBoolean())
            );
            clusterState = strategy.applyFailedShards(clusterState, failedShards, List.of());
            clusterState = strategy.reroute(clusterState, "allocation retry attempt-" + i, ActionListener.noop());
        }
        assertThat("replica should not be assigned", getReplica().state(), equalTo(ShardRoutingState.UNASSIGNED));
        assertThat("reroute should be a no-op", strategy.reroute(clusterState, "test", ActionListener.noop()), sameInstance(clusterState));

        // Now allocate replica with retry_failed flag set
        AllocationService.CommandsResult result = strategy.reroute(
            clusterState,
            new AllocationCommands(
                new AllocateReplicaAllocationCommand(
                    INDEX_NAME,
                    0,
                    getPrimary().currentNodeId().equals("node1") ? "node2" : "node1",
                    projectId
                )
            ),
            false,
            true,
            false,
            ActionListener.noop()
        );
        clusterState = result.clusterState();

        assertEquals(ShardRoutingState.INITIALIZING, getReplica().state());
        clusterState = startShardsAndReroute(strategy, clusterState, getReplica());
        assertEquals(ShardRoutingState.STARTED, getReplica().state());
        assertFalse(clusterState.getRoutingNodes().hasUnassignedShards());
    }
}
