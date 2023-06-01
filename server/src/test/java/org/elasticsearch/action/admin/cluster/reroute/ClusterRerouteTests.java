/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ClusterRerouteTests extends ESAllocationTestCase {

    public void testSerializeRequest() throws IOException {
        ClusterRerouteRequest req = new ClusterRerouteRequest();
        req.setRetryFailed(randomBoolean());
        req.dryRun(randomBoolean());
        req.explain(randomBoolean());
        req.add(new AllocateEmptyPrimaryAllocationCommand("foo", 1, "bar", randomBoolean()));
        req.timeout(TimeValue.timeValueMillis(randomIntBetween(0, 100)));
        BytesStreamOutput out = new BytesStreamOutput();
        req.writeTo(out);
        BytesReference bytes = out.bytes();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(NetworkModule.getNamedWriteables());
        StreamInput wrap = new NamedWriteableAwareStreamInput(bytes.streamInput(), namedWriteableRegistry);
        ClusterRerouteRequest deserializedReq = new ClusterRerouteRequest(wrap);

        assertEquals(req.isRetryFailed(), deserializedReq.isRetryFailed());
        assertEquals(req.dryRun(), deserializedReq.dryRun());
        assertEquals(req.explain(), deserializedReq.explain());
        assertEquals(req.timeout(), deserializedReq.timeout());
        assertEquals(1, deserializedReq.getCommands().commands().size()); // allocation commands have their own tests
        assertEquals(req.getCommands().commands().size(), deserializedReq.getCommands().commands().size());
    }

    public void testClusterStateUpdateTaskInDryRun() {
        AllocationService allocationService = new AllocationService(
            new AllocationDeciders(List.of(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        ClusterState clusterState = createInitialClusterState(allocationService);

        var responseActionListener = ActionListener.<ClusterRerouteResponse>wrap(ignore -> {}, exception -> {
            throw new AssertionError("Should not fail in test", exception);
        });

        var request = new ClusterRerouteRequest().dryRun(true);
        var task = new TransportClusterRerouteAction.ClusterRerouteResponseAckedClusterStateUpdateTask(
            logger,
            allocationService,
            new ThreadContext(Settings.EMPTY),
            request,
            responseActionListener
        );

        ClusterState execute = task.execute(clusterState);
        assertSame(execute, clusterState); // dry-run should keep the current cluster state
        task.onAllNodesAcked();
    }

    public void testClusterStateUpdateTask() {
        AllocationService allocationService = new AllocationService(
            new AllocationDeciders(List.of(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        ClusterState clusterState = createInitialClusterState(allocationService);

        var req = new ClusterRerouteRequest().dryRun(false);
        var task = new TransportClusterRerouteAction.ClusterRerouteResponseAckedClusterStateUpdateTask(
            logger,
            allocationService,
            new ThreadContext(Settings.EMPTY),
            req,
            ActionListener.noop()
        );

        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        // now fail it N-1 times
        for (int i = 0; i < retries; i++) {
            // execute task
            ClusterState newState = task.execute(clusterState);
            assertNotSame(newState, clusterState);
            assertStateAndFailedAllocations(newState.routingTable().index("idx"), INITIALIZING, i);
            clusterState = newState;

            // apply failed shards
            newState = allocationService.applyFailedShards(
                clusterState,
                List.of(
                    new FailedShard(
                        clusterState.routingTable().index("idx").shard(0).shard(0),
                        "boom" + i,
                        new RuntimeException("test-failure"),
                        randomBoolean()
                    )
                ),
                List.of()
            );
            assertThat(newState, not(equalTo(clusterState)));
            assertStateAndFailedAllocations(newState.routingTable().index("idx"), i == retries - 1 ? UNASSIGNED : INITIALIZING, i + 1);
            clusterState = newState;
        }

        // without retry_failed we won't allocate that shard
        ClusterState newState = task.execute(clusterState);
        assertNotSame(newState, clusterState);
        assertStateAndFailedAllocations(clusterState.routingTable().index("idx"), UNASSIGNED, retries);

        req.setRetryFailed(true); // now we manually retry and get the shard back into initializing
        newState = task.execute(clusterState);
        assertNotSame(newState, clusterState); // dry-run=false
        assertStateAndFailedAllocations(newState.routingTable().index("idx"), INITIALIZING, 0);
    }

    private void assertStateAndFailedAllocations(IndexRoutingTable indexRoutingTable, ShardRoutingState state, int failedAllocations) {
        assertThat(indexRoutingTable.size(), equalTo(1));
        assertThat(indexRoutingTable.shard(0).shard(0).state(), equalTo(state));
        assertThat(indexRoutingTable.shard(0).shard(0).unassignedInfo().getNumFailedAllocations(), equalTo(failedAllocations));
    }

    private ClusterState createInitialClusterState(AllocationService service) {
        Metadata.Builder metaBuilder = Metadata.builder();
        metaBuilder.put(IndexMetadata.builder("idx").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        routingTableBuilder.addAsNew(metadata.index("idx"));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = service.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertEquals(prevRoutingTable.index("idx").size(), 1);
        assertEquals(prevRoutingTable.index("idx").shard(0).shard(0).state(), UNASSIGNED);

        assertEquals(routingTable.index("idx").size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shard(0).state(), INITIALIZING);
        return clusterState;
    }
}
