/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class RetryFailedAllocationTests extends ESAllocationTestCase {

    private MockAllocationService strategy;
    private ClusterState clusterState;
    private final String INDEX_NAME = "index";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Metadata metadata = Metadata.builder().put(IndexMetadata.builder(INDEX_NAME)
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1)).build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index(INDEX_NAME)).build();
        clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        strategy = createAllocationService(Settings.EMPTY);
    }

    private ShardRouting getPrimary() {
        return clusterState.getRoutingTable().index(INDEX_NAME).shard(0).primaryShard();
    }

    private ShardRouting getReplica() {
        return clusterState.getRoutingTable().index(INDEX_NAME).shard(0).replicaShards().get(0);
    }

    public void testRetryFailedResetForAllocationCommands() {
        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        clusterState = strategy.reroute(clusterState, "initial allocation");
        clusterState = startShardsAndReroute(strategy, clusterState, getPrimary());

        // Exhaust all replica allocation attempts with shard failures
        for (int i = 0; i < retries; i++) {
            List<FailedShard> failedShards = Collections.singletonList(
                new FailedShard(getReplica(), "failing-shard::attempt-" + i,
                    new ElasticsearchException("simulated"), randomBoolean()));
            clusterState = strategy.applyFailedShards(clusterState, failedShards);
            clusterState = strategy.reroute(clusterState, "allocation retry attempt-" + i);
        }
        assertThat("replica should not be assigned", getReplica().state(), equalTo(ShardRoutingState.UNASSIGNED));
        assertThat("reroute should be a no-op", strategy.reroute(clusterState, "test"), sameInstance(clusterState));

        // Now allocate replica with retry_failed flag set
        AllocationService.CommandsResult result = strategy.reroute(clusterState,
            new AllocationCommands(new AllocateReplicaAllocationCommand(INDEX_NAME, 0,
                getPrimary().currentNodeId().equals("node1") ? "node2" : "node1")),
            false, true);
        clusterState = result.getClusterState();

        assertEquals(ShardRoutingState.INITIALIZING, getReplica().state());
        clusterState = startShardsAndReroute(strategy, clusterState, getReplica());
        assertEquals(ShardRoutingState.STARTED, getReplica().state());
        assertFalse(clusterState.getRoutingNodes().hasUnassignedShards());
    }
}
