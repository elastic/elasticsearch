/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceStats;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetDesiredBalanceActionTests extends ESTestCase {

    private final DesiredBalanceShardsAllocator desiredBalanceShardsAllocator = mock(DesiredBalanceShardsAllocator.class);
    private final ClusterState clusterState = mock(ClusterState.class);
    private final Metadata metadata = mock(Metadata.class);
    private final TransportGetDesiredBalanceAction transportGetDesiredBalanceAction = new TransportGetDesiredBalanceAction(
        mock(TransportService.class),
        mock(ClusterService.class),
        mock(ThreadPool.class),
        mock(ActionFilters.class),
        mock(IndexNameExpressionResolver.class),
        desiredBalanceShardsAllocator
    );
    @SuppressWarnings("unchecked")
    private final ActionListener<DesiredBalanceResponse> listener = mock(ActionListener.class);

    @Before
    public void setUpMocks() throws Exception {
        when(clusterState.metadata()).thenReturn(metadata);
    }

    public void testReturnsErrorIfAllocatorIsNotDesiredBalanced() throws Exception {
        when(metadata.settings()).thenReturn(Settings.builder().put("cluster.routing.allocation.type", "balanced").build());

        new TransportGetDesiredBalanceAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            mock(ShardsAllocator.class)
        ).masterOperation(mock(Task.class), mock(DesiredBalanceRequest.class), clusterState, listener);

        ArgumentCaptor<ResourceNotFoundException> exceptionArgumentCaptor = ArgumentCaptor.forClass(ResourceNotFoundException.class);
        verify(listener).onFailure(exceptionArgumentCaptor.capture());

        final var exception = exceptionArgumentCaptor.getValue();
        assertEquals("Desired balance allocator is not in use, no desired balance found", exception.getMessage());
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
    }

    public void testReturnsErrorIfDesiredBalanceIsNotAvailable() throws Exception {
        when(metadata.settings()).thenReturn(Settings.builder().put("cluster.routing.allocation.type", "desired_balance").build());

        transportGetDesiredBalanceAction.masterOperation(mock(Task.class), mock(DesiredBalanceRequest.class), clusterState, listener);

        ArgumentCaptor<ResourceNotFoundException> exceptionArgumentCaptor = ArgumentCaptor.forClass(ResourceNotFoundException.class);
        verify(listener).onFailure(exceptionArgumentCaptor.capture());

        assertEquals("Desired balance is not computed yet", exceptionArgumentCaptor.getValue().getMessage());
    }

    public void testGetDesiredBalance() throws Exception {
        Set<String> nodeIds = randomUnique(() -> randomAlphaOfLength(8), randomIntBetween(1, 32));
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < randomInt(8); i++) {
            Index index = new Index(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
            for (int j = 0; j < randomIntBetween(1, 16); j++) {
                String nodeId = randomFrom(nodeIds);
                switch (randomInt(3)) {
                    case 0 -> indexRoutingTableBuilder.addShard(
                        TestShardRouting.newShardRouting(new ShardId(index, j), nodeId, true, ShardRoutingState.STARTED)
                    );
                    case 1 -> {
                        indexRoutingTableBuilder.addShard(
                            TestShardRouting.newShardRouting(new ShardId(index, j), nodeId, true, ShardRoutingState.STARTED)
                        );
                        if (nodeIds.size() > 1) {
                            indexRoutingTableBuilder.addShard(
                                TestShardRouting.newShardRouting(
                                    new ShardId(index, j),
                                    randomValueOtherThan(nodeId, () -> randomFrom(nodeIds)),
                                    false,
                                    ShardRoutingState.STARTED
                                )
                            );
                        }
                        break;
                    }
                    case 2 -> indexRoutingTableBuilder.addShard(
                        TestShardRouting.newShardRouting(new ShardId(index, j), null, false, ShardRoutingState.UNASSIGNED)
                    );
                    case 3 -> {
                        ShardRouting shard = TestShardRouting.newShardRouting(
                            new ShardId(index, j),
                            nodeId,
                            true,
                            ShardRoutingState.STARTED
                        );
                        if (nodeIds.size() > 1) {
                            shard = TestShardRouting.relocate(
                                shard,
                                randomValueOtherThan(nodeId, () -> randomFrom(nodeIds)),
                                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
                            );
                        }
                        indexRoutingTableBuilder.addShard(shard);
                    }
                }
            }
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        RoutingTable routingTable = routingTableBuilder.build();
        when(clusterState.routingTable()).thenReturn(routingTable);

        List<ShardId> shardIds = routingTable.allShards().stream().map(ShardRouting::shardId).toList();
        Map<String, Set<ShardId>> indexShards = shardIds.stream()
            .collect(Collectors.groupingBy(e -> e.getIndex().getName(), Collectors.toSet()));
        Map<ShardId, ShardAssignment> shardAssignments = new HashMap<>();
        if (shardIds.size() > 0) {
            for (int i = 0; i < randomInt(8); i++) {
                int total = randomIntBetween(1, 1024);
                Set<String> shardNodeIds = randomUnique(() -> randomFrom(nodeIds), randomInt(8));
                shardAssignments.put(
                    randomFrom(shardIds),
                    new ShardAssignment(shardNodeIds, total, total - shardNodeIds.size(), randomInt(1024))
                );
            }
        }

        when(desiredBalanceShardsAllocator.getDesiredBalance()).thenReturn(new DesiredBalance(randomInt(1024), shardAssignments));
        DesiredBalanceStats desiredBalanceStats = new DesiredBalanceStats(
            randomInt(Integer.MAX_VALUE),
            randomBoolean(),
            randomInt(Integer.MAX_VALUE),
            randomInt(Integer.MAX_VALUE),
            randomInt(Integer.MAX_VALUE),
            randomInt(Integer.MAX_VALUE),
            randomInt(Integer.MAX_VALUE),
            randomInt(Integer.MAX_VALUE)
        );
        when(desiredBalanceShardsAllocator.getStats()).thenReturn(desiredBalanceStats);
        when(metadata.settings()).thenReturn(Settings.builder().put("cluster.routing.allocation.type", "desired_balance").build());

        transportGetDesiredBalanceAction.masterOperation(mock(Task.class), mock(DesiredBalanceRequest.class), clusterState, listener);

        ArgumentCaptor<DesiredBalanceResponse> desiredBalanceResponseCaptor = ArgumentCaptor.forClass(DesiredBalanceResponse.class);
        verify(listener).onResponse(desiredBalanceResponseCaptor.capture());
        DesiredBalanceResponse desiredBalanceResponse = desiredBalanceResponseCaptor.getValue();
        assertEquals(desiredBalanceStats, desiredBalanceResponse.getStats());
        assertEquals(indexShards.keySet(), desiredBalanceResponse.getRoutingTable().keySet());
        for (var e : desiredBalanceResponse.getRoutingTable().entrySet()) {
            String index = e.getKey();
            Map<Integer, DesiredBalanceResponse.DesiredShards> shardsMap = e.getValue();
            assertEquals(indexShards.get(index).stream().map(ShardId::id).collect(Collectors.toSet()), shardsMap.keySet());
            for (var shardDesiredBalance : shardsMap.entrySet()) {
                DesiredBalanceResponse.DesiredShards desiredShard = shardDesiredBalance.getValue();
                int shardId = shardDesiredBalance.getKey();
                IndexShardRoutingTable indexShardRoutingTable = routingTable.shardRoutingTable(index, shardId);
                for (int idx = 0; idx < indexShardRoutingTable.size(); idx++) {
                    ShardRouting shard = indexShardRoutingTable.shard(idx);
                    DesiredBalanceResponse.ShardView shardView = desiredShard.current().get(idx);
                    assertEquals(shard.state(), shardView.state());
                    assertEquals(shard.primary(), shardView.primary());
                    assertEquals(shard.currentNodeId(), shardView.node());
                    assertEquals(shard.relocatingNodeId(), shardView.relocatingNode());
                    assertEquals(shard.index().getName(), shardView.index());
                    assertEquals(shard.shardId().id(), shardView.shardId());
                    assertEquals(shard.allocationId(), shardView.allocationId());
                    Set<String> desiredNodeIds = Optional.ofNullable(shardAssignments.get(shard.shardId()))
                        .map(ShardAssignment::nodeIds)
                        .orElse(Set.of());
                    assertEquals(
                        shard.currentNodeId() != null && desiredNodeIds.contains(shard.currentNodeId()),
                        shardView.nodeIsDesired()
                    );
                    assertEquals(
                        shard.relocatingNodeId() != null && desiredNodeIds.contains(shard.relocatingNodeId()),
                        shardView.relocatingNodeIsDesired()
                    );
                }
                Optional<ShardAssignment> shardAssignment = Optional.ofNullable(shardAssignments.get(indexShardRoutingTable.shardId()));
                if (shardAssignment.isPresent()) {
                    assertEquals(shardAssignment.get().nodeIds(), desiredShard.desired().nodeIds());
                    assertEquals(shardAssignment.get().total(), desiredShard.desired().total());
                    assertEquals(shardAssignment.get().unassigned(), desiredShard.desired().unassigned());
                    assertEquals(shardAssignment.get().ignored(), desiredShard.desired().ignored());
                } else {
                    assertNull(desiredShard.desired());
                }
            }
        }
    }
}
