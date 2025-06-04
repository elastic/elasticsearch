/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceComputer;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceInput;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportDeleteDesiredBalanceActionTests extends ESAllocationTestCase {

    public void testReturnsErrorIfAllocatorIsNotDesiredBalanced() throws Exception {
        var listener = new PlainActionFuture<ActionResponse.Empty>();

        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);

        new TransportDeleteDesiredBalanceAction(
            transportService,
            mock(ClusterService.class),
            threadPool,
            mock(ActionFilters.class),
            mock(AllocationService.class),
            mock(ShardsAllocator.class)
        ).masterOperation(mock(Task.class), new DesiredBalanceRequest(TEST_REQUEST_TIMEOUT), ClusterState.EMPTY_STATE, listener);

        var exception = expectThrows(ResourceNotFoundException.class, listener);
        assertThat(exception.getMessage(), equalTo("Desired balance allocator is not in use, no desired balance found"));
    }

    public void testDeleteDesiredBalance() throws Exception {

        var threadPool = new TestThreadPool(getTestName());

        var shardId = new ShardId("test-index", UUIDs.randomBase64UUID(), 0);
        var index = createIndex(shardId.getIndexName());
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("master")).localNodeId("master").masterNodeId("master").build())
            .metadata(Metadata.builder().put(index, false).build())
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(index).build())
            .build();

        var clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool);

        var settings = Settings.EMPTY;
        var clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);

        var delegate = new BalancedShardsAllocator();
        var computer = new DesiredBalanceComputer(clusterSettings, threadPool, delegate) {

            final AtomicReference<DesiredBalance> lastComputationInput = new AtomicReference<>();

            @Override
            public DesiredBalance compute(
                DesiredBalance previousDesiredBalance,
                DesiredBalanceInput desiredBalanceInput,
                Queue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves,
                Predicate<DesiredBalanceInput> isFresh
            ) {
                lastComputationInput.set(previousDesiredBalance);
                return super.compute(previousDesiredBalance, desiredBalanceInput, pendingDesiredBalanceMoves, isFresh);
            }
        };
        var allocator = new DesiredBalanceShardsAllocator(
            delegate,
            threadPool,
            clusterService,
            computer,
            (state, action) -> state,
            TelemetryProvider.NOOP,
            EMPTY_NODE_ALLOCATION_STATS
        );
        var allocationService = new MockAllocationService(
            randomAllocationDeciders(settings, clusterSettings),
            new TestGatewayAllocator(),
            allocator,
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );

        safeAwait((ActionListener<Void> listener) -> allocationService.reroute(clusterState, "inital-allocate", listener));

        var balanceBeforeReset = allocator.getDesiredBalance();
        assertThat(balanceBeforeReset.lastConvergedIndex(), greaterThan(DesiredBalance.BECOME_MASTER_INITIAL.lastConvergedIndex()));
        assertThat(balanceBeforeReset.assignments(), not(anEmptyMap()));

        var listener = new PlainActionFuture<ActionResponse.Empty>();

        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);

        var action = new TransportDeleteDesiredBalanceAction(
            transportService,
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            allocationService,
            allocator
        );

        action.masterOperation(mock(Task.class), new DesiredBalanceRequest(TEST_REQUEST_TIMEOUT), clusterState, listener);

        try {
            assertThat(listener.get(), notNullValue());
            // resetting desired balance should trigger new computation with empty assignments
            assertThat(computer.lastComputationInput.get(), equalTo(new DesiredBalance(balanceBeforeReset.lastConvergedIndex(), Map.of())));
        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }

    private static IndexMetadata createIndex(String name) {
        return IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
    }
}
