/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceStats;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.ClusterModule.BALANCED_ALLOCATOR;
import static org.elasticsearch.cluster.ClusterModule.DESIRED_BALANCE_ALLOCATOR;
import static org.elasticsearch.cluster.ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetDesiredBalanceActionTests extends ESAllocationTestCase {

    private final DesiredBalanceShardsAllocator desiredBalanceShardsAllocator = mock(DesiredBalanceShardsAllocator.class);
    private final ClusterInfoService clusterInfoService = mock(ClusterInfoService.class);
    private final TransportGetDesiredBalanceAction transportGetDesiredBalanceAction = new TransportGetDesiredBalanceAction(
        mock(TransportService.class),
        mock(ClusterService.class),
        mock(ThreadPool.class),
        mock(ActionFilters.class),
        mock(IndexNameExpressionResolver.class),
        desiredBalanceShardsAllocator,
        clusterInfoService,
        TEST_WRITE_LOAD_FORECASTER
    );
    @SuppressWarnings("unchecked")
    private final ActionListener<DesiredBalanceResponse> listener = mock(ActionListener.class);

    public void testReturnsErrorIfAllocatorIsNotDesiredBalanced() throws Exception {
        var clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadataWithConfiguredAllocator(BALANCED_ALLOCATOR)).build();

        new TransportGetDesiredBalanceAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            mock(ShardsAllocator.class),
            mock(ClusterInfoService.class),
            mock(WriteLoadForecaster.class)
        ).masterOperation(mock(Task.class), mock(DesiredBalanceRequest.class), clusterState, listener);

        ArgumentCaptor<ResourceNotFoundException> exceptionArgumentCaptor = ArgumentCaptor.forClass(ResourceNotFoundException.class);
        verify(listener).onFailure(exceptionArgumentCaptor.capture());

        final var exception = exceptionArgumentCaptor.getValue();
        assertEquals("Desired balance allocator is not in use, no desired balance found", exception.getMessage());
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
    }

    public void testReturnsErrorIfDesiredBalanceIsNotAvailable() throws Exception {
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadataWithConfiguredAllocator(DESIRED_BALANCE_ALLOCATOR))
            .build();

        transportGetDesiredBalanceAction.masterOperation(mock(Task.class), mock(DesiredBalanceRequest.class), clusterState, listener);

        ArgumentCaptor<ResourceNotFoundException> exceptionArgumentCaptor = ArgumentCaptor.forClass(ResourceNotFoundException.class);
        verify(listener).onFailure(exceptionArgumentCaptor.capture());

        assertEquals("Desired balance is not computed yet", exceptionArgumentCaptor.getValue().getMessage());
    }

    public void testGetDesiredBalance() throws Exception {
        Set<String> nodeIds = randomUnique(() -> randomAlphaOfLength(8), randomIntBetween(1, 32));
        DiscoveryNodes.Builder discoveryNodes = DiscoveryNodes.builder();
        for (String nodeId : nodeIds) {
            discoveryNodes.add(newNode(nodeId, Set.of(DiscoveryNodeRole.DATA_ROLE)));
        }
        Metadata.Builder metadataBuilder = metadataWithConfiguredAllocator(DESIRED_BALANCE_ALLOCATOR);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < randomInt(8); i++) {
            String indexName = randomAlphaOfLength(8);
            Settings.Builder settings = indexSettings(Version.CURRENT, 1, 0);
            if (randomBoolean()) {
                settings.put(DataTier.TIER_PREFERENCE_SETTING.getKey(), randomFrom("data_hot", "data_warm", "data_cold"));
            }
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName).settings(settings);
            if (randomBoolean()) {
                indexMetadataBuilder.indexWriteLoadForecast(randomDoubleBetween(0.0, 8.0, true));
            }
            if (randomBoolean()) {
                indexMetadataBuilder.shardSizeInBytesForecast(randomLongBetween(0, 1024));
            }
            IndexMetadata indexMetadata = indexMetadataBuilder.build();
            Index index = indexMetadata.getIndex();
            metadataBuilder.put(indexMetadata, false);
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
                    }
                    case 2 -> indexRoutingTableBuilder.addShard(
                        TestShardRouting.newShardRouting(new ShardId(index, j), null, true, ShardRoutingState.UNASSIGNED)
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

        List<ShardId> shardIds = routingTable.allShards().map(ShardRouting::shardId).toList();
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
            randomInt(Integer.MAX_VALUE),
            randomInt(Integer.MAX_VALUE)
        );
        when(desiredBalanceShardsAllocator.getStats()).thenReturn(desiredBalanceStats);
        ClusterInfo clusterInfo = ClusterInfo.EMPTY;
        when(clusterInfoService.getClusterInfo()).thenReturn(clusterInfo);

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadataBuilder.build())
            .nodes(discoveryNodes.build())
            .routingTable(routingTable)
            .build();

        transportGetDesiredBalanceAction.masterOperation(mock(Task.class), mock(DesiredBalanceRequest.class), clusterState, listener);

        ArgumentCaptor<DesiredBalanceResponse> desiredBalanceResponseCaptor = ArgumentCaptor.forClass(DesiredBalanceResponse.class);
        verify(listener).onResponse(desiredBalanceResponseCaptor.capture());
        DesiredBalanceResponse desiredBalanceResponse = desiredBalanceResponseCaptor.getValue();
        assertThat(desiredBalanceResponse.getStats(), equalTo(desiredBalanceStats));
        assertThat(desiredBalanceResponse.getClusterBalanceStats(), notNullValue());
        assertThat(desiredBalanceResponse.getClusterInfo(), equalTo(clusterInfo));
        assertEquals(indexShards.keySet(), desiredBalanceResponse.getRoutingTable().keySet());
        for (var e : desiredBalanceResponse.getRoutingTable().entrySet()) {
            String index = e.getKey();
            Map<Integer, DesiredBalanceResponse.DesiredShards> shardsMap = e.getValue();
            assertEquals(indexShards.get(index).stream().map(ShardId::id).collect(Collectors.toSet()), shardsMap.keySet());
            for (var shardDesiredBalance : shardsMap.entrySet()) {
                DesiredBalanceResponse.DesiredShards desiredShard = shardDesiredBalance.getValue();
                int shardId = shardDesiredBalance.getKey();
                IndexMetadata indexMetadata = clusterState.metadata().index(index);
                IndexShardRoutingTable indexShardRoutingTable = clusterState.getRoutingTable().shardRoutingTable(index, shardId);
                for (int idx = 0; idx < indexShardRoutingTable.size(); idx++) {
                    ShardRouting shard = indexShardRoutingTable.shard(idx);
                    DesiredBalanceResponse.ShardView shardView = desiredShard.current().get(idx);
                    assertEquals(shard.state(), shardView.state());
                    assertEquals(shard.primary(), shardView.primary());
                    assertEquals(shard.currentNodeId(), shardView.node());
                    assertEquals(shard.relocatingNodeId(), shardView.relocatingNode());
                    assertEquals(shard.index().getName(), shardView.index());
                    assertEquals(shard.shardId().id(), shardView.shardId());
                    var forecastedWriteLoad = TEST_WRITE_LOAD_FORECASTER.getForecastedWriteLoad(indexMetadata);
                    assertEquals(forecastedWriteLoad.isPresent() ? forecastedWriteLoad.getAsDouble() : null, shardView.forecastWriteLoad());
                    var forecastedShardSizeInBytes = indexMetadata.getForecastedShardSizeInBytes();
                    assertEquals(
                        forecastedShardSizeInBytes.isPresent() ? forecastedShardSizeInBytes.getAsLong() : null,
                        shardView.forecastShardSizeInBytes()
                    );
                    Set<String> desiredNodeIds = Optional.ofNullable(shardAssignments.get(shard.shardId()))
                        .map(ShardAssignment::nodeIds)
                        .orElse(Set.of());
                    assertEquals(
                        shard.currentNodeId() != null && desiredNodeIds.contains(shard.currentNodeId()),
                        shardView.nodeIsDesired()
                    );
                    assertEquals(
                        shard.relocatingNodeId() != null ? desiredNodeIds.contains(shard.relocatingNodeId()) : null,
                        shardView.relocatingNodeIsDesired()
                    );
                    assertEquals(indexMetadata.getTierPreference(), shardView.tierPreference());
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

    private static Metadata.Builder metadataWithConfiguredAllocator(String allocator) {
        return Metadata.builder().persistentSettings(Settings.builder().put(SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), allocator).build());
    }
}
