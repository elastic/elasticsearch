/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class MaxRetryAllocationDeciderTests extends ESAllocationTestCase {

    private AllocationService strategy;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        strategy = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
    }

    private ClusterState createInitialClusterState() {
        Metadata.Builder metaBuilder = Metadata.builder();
        metaBuilder.put(IndexMetadata.builder("idx").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metadata.index("idx"));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
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
        for (int i = 0; i < retries - 1; i++) {
            List<FailedShard> failedShards = Collections.singletonList(
                new FailedShard(
                    routingTable.index("idx").shard(0).shards().get(0),
                    "boom" + i,
                    new UnsupportedOperationException(),
                    randomBoolean()
                )
            );
            ClusterState newState = strategy.applyFailedShards(clusterState, failedShards);
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            routingTable = newState.routingTable();
            assertEquals(routingTable.index("idx").shards().size(), 1);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), i + 1);
            assertThat(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), containsString("boom" + i));
        }
        // now we go and check that we are actually stick to unassigned on the next failure
        List<FailedShard> failedShards = Collections.singletonList(
            new FailedShard(
                routingTable.index("idx").shard(0).shards().get(0),
                "boom",
                new UnsupportedOperationException(),
                randomBoolean()
            )
        );
        ClusterState newState = strategy.applyFailedShards(clusterState, failedShards);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingTable = newState.routingTable();
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), retries);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);
        assertThat(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), containsString("boom"));

        // manual resetting of retry count
        newState = strategy.reroute(clusterState, new AllocationCommands(), false, true).getClusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingTable = newState.routingTable();

        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(0, routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations());
        assertEquals(INITIALIZING, routingTable.index("idx").shard(0).shards().get(0).state());
        assertThat(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), containsString("boom"));

        // again fail it N-1 times
        for (int i = 0; i < retries - 1; i++) {
            failedShards = Collections.singletonList(
                new FailedShard(
                    routingTable.index("idx").shard(0).shards().get(0),
                    "boom",
                    new UnsupportedOperationException(),
                    randomBoolean()
                )
            );

            newState = strategy.applyFailedShards(clusterState, failedShards);
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            routingTable = newState.routingTable();
            assertEquals(routingTable.index("idx").shards().size(), 1);
            assertEquals(i + 1, routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations());
            assertEquals(INITIALIZING, routingTable.index("idx").shard(0).shards().get(0).state());
            assertThat(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), containsString("boom"));
        }

        // now we go and check that we are actually stick to unassigned on the next failure
        failedShards = Collections.singletonList(
            new FailedShard(
                routingTable.index("idx").shard(0).shards().get(0),
                "boom",
                new UnsupportedOperationException(),
                randomBoolean()
            )
        );
        newState = strategy.applyFailedShards(clusterState, failedShards);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingTable = newState.routingTable();
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(retries, routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations());
        assertEquals(UNASSIGNED, routingTable.index("idx").shard(0).shards().get(0).state());
        assertThat(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getMessage(), containsString("boom"));
    }

    public void testFailedAllocation() {
        ClusterState clusterState = createInitialClusterState();
        RoutingTable routingTable = clusterState.routingTable();
        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        // now fail it N-1 times
        for (int i = 0; i < retries - 1; i++) {
            List<FailedShard> failedShards = Collections.singletonList(
                new FailedShard(
                    routingTable.index("idx").shard(0).shards().get(0),
                    "boom" + i,
                    new UnsupportedOperationException(),
                    randomBoolean()
                )
            );
            ClusterState newState = strategy.applyFailedShards(clusterState, failedShards);
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            routingTable = newState.routingTable();
            assertEquals(routingTable.index("idx").shards().size(), 1);
            ShardRouting unassignedPrimary = routingTable.index("idx").shard(0).shards().get(0);
            assertEquals(unassignedPrimary.state(), INITIALIZING);
            assertEquals(unassignedPrimary.unassignedInfo().getNumFailedAllocations(), i + 1);
            assertThat(unassignedPrimary.unassignedInfo().getMessage(), containsString("boom" + i));
            // MaxRetryAllocationDecider#canForceAllocatePrimary should return YES decisions because canAllocate returns YES here
            assertEquals(
                Decision.Type.YES,
                new MaxRetryAllocationDecider().canForceAllocatePrimary(unassignedPrimary, null, newRoutingAllocation(clusterState)).type()
            );
        }
        // now we go and check that we are actually stick to unassigned on the next failure
        {
            List<FailedShard> failedShards = Collections.singletonList(
                new FailedShard(
                    routingTable.index("idx").shard(0).shards().get(0),
                    "boom",
                    new UnsupportedOperationException(),
                    randomBoolean()
                )
            );
            ClusterState newState = strategy.applyFailedShards(clusterState, failedShards);
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            routingTable = newState.routingTable();
            assertEquals(routingTable.index("idx").shards().size(), 1);
            ShardRouting unassignedPrimary = routingTable.index("idx").shard(0).shards().get(0);
            assertEquals(unassignedPrimary.unassignedInfo().getNumFailedAllocations(), retries);
            assertEquals(unassignedPrimary.state(), UNASSIGNED);
            assertThat(unassignedPrimary.unassignedInfo().getMessage(), containsString("boom"));
            // MaxRetryAllocationDecider#canForceAllocatePrimary should return a NO decision because canAllocate returns NO here
            assertEquals(
                Decision.Type.NO,
                new MaxRetryAllocationDecider().canForceAllocatePrimary(unassignedPrimary, null, newRoutingAllocation(clusterState)).type()
            );
        }

        // change the settings and ensure we can do another round of allocation for that index.
        clusterState = ClusterState.builder(clusterState)
            .routingTable(routingTable)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .put(
                        IndexMetadata.builder(clusterState.metadata().index("idx"))
                            .settings(
                                Settings.builder()
                                    .put(clusterState.metadata().index("idx").getSettings())
                                    .put("index.allocation.max_retries", retries + 1)
                                    .build()
                            )
                            .build(),
                        true
                    )
                    .build()
            )
            .build();
        ClusterState newState = strategy.reroute(clusterState, "settings changed");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingTable = newState.routingTable();
        // good we are initializing and we are maintaining failure information
        assertEquals(routingTable.index("idx").shards().size(), 1);
        ShardRouting unassignedPrimary = routingTable.index("idx").shard(0).shards().get(0);
        assertEquals(unassignedPrimary.unassignedInfo().getNumFailedAllocations(), retries);
        assertEquals(unassignedPrimary.state(), INITIALIZING);
        assertThat(unassignedPrimary.unassignedInfo().getMessage(), containsString("boom"));
        // bumped up the max retry count, so canForceAllocatePrimary should return a YES decision
        assertEquals(
            Decision.Type.YES,
            new MaxRetryAllocationDecider().canForceAllocatePrimary(
                routingTable.index("idx").shard(0).shards().get(0),
                null,
                newRoutingAllocation(clusterState)
            ).type()
        );

        // now we start the shard
        clusterState = startShardsAndReroute(strategy, clusterState, routingTable.index("idx").shard(0).shards().get(0));
        routingTable = clusterState.routingTable();

        // all counters have been reset to 0 ie. no unassigned info
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertNull(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo());
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), STARTED);

        // now fail again and see if it has a new counter
        List<FailedShard> failedShards = Collections.singletonList(
            new FailedShard(
                routingTable.index("idx").shard(0).shards().get(0),
                "ZOOOMG",
                new UnsupportedOperationException(),
                randomBoolean()
            )
        );
        newState = strategy.applyFailedShards(clusterState, failedShards);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingTable = newState.routingTable();
        assertEquals(routingTable.index("idx").shards().size(), 1);
        unassignedPrimary = routingTable.index("idx").shard(0).shards().get(0);
        assertEquals(unassignedPrimary.unassignedInfo().getNumFailedAllocations(), 1);
        assertEquals(unassignedPrimary.state(), UNASSIGNED);
        assertThat(unassignedPrimary.unassignedInfo().getMessage(), containsString("ZOOOMG"));
        // Counter reset, so MaxRetryAllocationDecider#canForceAllocatePrimary should return a YES decision
        assertEquals(
            Decision.Type.YES,
            new MaxRetryAllocationDecider().canForceAllocatePrimary(unassignedPrimary, null, newRoutingAllocation(clusterState)).type()
        );
    }

    private RoutingAllocation newRoutingAllocation(ClusterState clusterState) {
        final RoutingAllocation routingAllocation = new RoutingAllocation(null, null, clusterState, null, null, 0);
        if (randomBoolean()) {
            routingAllocation.setDebugMode(randomFrom(RoutingAllocation.DebugMode.values()));
        }
        return routingAllocation;
    }

}
