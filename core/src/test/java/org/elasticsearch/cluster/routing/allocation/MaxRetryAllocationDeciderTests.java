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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESAllocationTestCase;
import org.elasticsearch.test.gateway.NoopGatewayAllocator;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;

public class MaxRetryAllocationDeciderTests extends ESAllocationTestCase {

    private AllocationService strategy;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        strategy = new AllocationService(Settings.builder().build(), new AllocationDeciders(Settings.EMPTY,
            Collections.singleton(new MaxRetryAllocationDecider(Settings.EMPTY))),
            NoopGatewayAllocator.INSTANCE, new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);
    }

    private ClusterState createInitialClusterState() {
        MetaData.Builder metaBuilder = MetaData.builder();
        metaBuilder.put(IndexMetaData.builder("idx").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        MetaData metaData = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metaData.index("idx"));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metaData(metaData).routingTable(routingTable).build();
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute", false).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertEquals(prevRoutingTable.index("idx").shards().size(), 1);
        assertEquals(prevRoutingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);

        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
        return clusterState;
    }

    public void testSingleRetryOnIgnore() {
        ClusterState clusterState = createInitialClusterState();
        RoutingTable routingTable = clusterState.routingTable();
        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        // now fail it N-1 times
        for (int i = 0; i < retries-1; i++) {
            List<FailedRerouteAllocation.FailedShard> failedShards = Collections.singletonList(
                new FailedRerouteAllocation.FailedShard(routingTable.index("idx").shard(0).shards().get(0), "boom" + i,
                    new UnsupportedOperationException()));
            RoutingAllocation.Result result = strategy.applyFailedShards(clusterState, failedShards);
            assertTrue(result.changed());
            routingTable = result.routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            assertEquals(routingTable.index("idx").shards().size(), 1);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), i+1);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), "boom" + i);
        }
        // now we go and check that we are actually stick to unassigned on the next failure
        List<FailedRerouteAllocation.FailedShard> failedShards = Collections.singletonList(
            new FailedRerouteAllocation.FailedShard(routingTable.index("idx").shard(0).shards().get(0), "boom",
                new UnsupportedOperationException()));
        RoutingAllocation.Result result = strategy.applyFailedShards(clusterState, failedShards);
        assertTrue(result.changed());
        routingTable = result.routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), retries);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), "boom");

        result = strategy.reroute(clusterState, new AllocationCommands(), false, true); // manual reroute should retry once
        assertTrue(result.changed());
        routingTable = result.routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), retries);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), "boom");

        // now we go and check that we are actually stick to unassigned on the next failure ie. no retry
        failedShards = Collections.singletonList(
            new FailedRerouteAllocation.FailedShard(routingTable.index("idx").shard(0).shards().get(0), "boom",
                new UnsupportedOperationException()));
        result = strategy.applyFailedShards(clusterState, failedShards);
        assertTrue(result.changed());
        routingTable = result.routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), retries+1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), "boom");

    }

    public void testFailedAllocation() {
        ClusterState clusterState = createInitialClusterState();
        RoutingTable routingTable = clusterState.routingTable();
        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        // now fail it N-1 times
        for (int i = 0; i < retries-1; i++) {
            List<FailedRerouteAllocation.FailedShard> failedShards = Collections.singletonList(
                new FailedRerouteAllocation.FailedShard(routingTable.index("idx").shard(0).shards().get(0), "boom" + i,
                    new UnsupportedOperationException()));
            RoutingAllocation.Result result = strategy.applyFailedShards(clusterState, failedShards);
            assertTrue(result.changed());
            routingTable = result.routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            assertEquals(routingTable.index("idx").shards().size(), 1);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), i+1);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), "boom" + i);
        }
        // now we go and check that we are actually stick to unassigned on the next failure
        {
            List<FailedRerouteAllocation.FailedShard> failedShards = Collections.singletonList(
                new FailedRerouteAllocation.FailedShard(routingTable.index("idx").shard(0).shards().get(0), "boom",
                    new UnsupportedOperationException()));
            RoutingAllocation.Result result = strategy.applyFailedShards(clusterState, failedShards);
            assertTrue(result.changed());
            routingTable = result.routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            assertEquals(routingTable.index("idx").shards().size(), 1);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), retries);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), "boom");
        }

        // change the settings and ensure we can do another round of allocation for that index.
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable)
            .metaData(MetaData.builder(clusterState.metaData())
                .put(IndexMetaData.builder(clusterState.metaData().index("idx")).settings(
                    Settings.builder().put(clusterState.metaData().index("idx").getSettings()).put("index.allocation.max_retries",
                        retries+1).build()
                ).build(), true).build()).build();
        RoutingAllocation.Result result = strategy.reroute(clusterState, "settings changed", false);
        assertTrue(result.changed());
        routingTable = result.routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // good we are initializing and we are maintaining failure information
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), retries);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), "boom");

        // now we start the shard
        routingTable = strategy.applyStartedShards(clusterState, Collections.singletonList(routingTable.index("idx")
            .shard(0).shards().get(0))).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        // all counters have been reset to 0 ie. no unassigned info
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertNull(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo());
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), STARTED);

        // now fail again and see if it has a new counter
        List<FailedRerouteAllocation.FailedShard> failedShards = Collections.singletonList(
            new FailedRerouteAllocation.FailedShard(routingTable.index("idx").shard(0).shards().get(0), "ZOOOMG",
                new UnsupportedOperationException()));
        result = strategy.applyFailedShards(clusterState, failedShards);
        assertTrue(result.changed());
        routingTable = result.routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), "ZOOOMG");
    }
}
