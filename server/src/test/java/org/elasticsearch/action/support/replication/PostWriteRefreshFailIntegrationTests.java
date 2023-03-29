/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.UnpromotableShardRefreshRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class PostWriteRefreshFailIntegrationTests extends IndexShardTestCase {

    private final String errorMessage = "Unable to update to refresh an unpromotable shards";
    private final CapturingTransport transport = new CapturingTransport();

    private TransportService transportService;
    private ClusterService clusterService;
    private AllocationService allocationService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        transportService.registerRequestHandler(
            TransportUnpromotableShardRefreshAction.NAME,
            ThreadPool.Names.SAME,
            UnpromotableShardRefreshRequest::new,
            (request, channel, task) -> channel.sendResponse(new IllegalStateException(errorMessage))
        );
        allocationService = new AllocationService(
            new AllocationDeciders(List.of(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
    }

    @Override
    public void tearDown() throws Exception {
        transportService.close();
        clusterService.stop();
        super.tearDown();
    }

    public void testFailShardIfUnpromotableRequestFails() throws IOException {
        allowShardFailures();

        clusterService.addListener(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {}
        });

        IndexShard shard = spy(newShard(true));
        recoverShardFromStore(shard);

        ReplicationGroup realReplicationGroup = shard.getReplicationGroup();
        Engine.IndexResult result = indexDoc(shard, "_doc", "0");

        ShardStateAction shardStateAction = new ShardStateAction(clusterService, transportService, allocationService, null, threadPool);
        PostWriteRefresh postWriteRefresh = new PostWriteRefresh(transportService, shardStateAction);

        ReplicationGroup mockReplicationGroup = mock(ReplicationGroup.class);
        when(shard.getReplicationGroup()).thenReturn(mockReplicationGroup).thenReturn(realReplicationGroup);

        IndexShardRoutingTable routingTable = mock(IndexShardRoutingTable.class);
        ShardRouting unpromotableShard = ShardRouting.newUnassigned(
            shard.shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "message"),
            ShardRouting.Role.SEARCH_ONLY
        );
        when(routingTable.unpromotableShards()).thenReturn(List.of(unpromotableShard));
        when(mockReplicationGroup.getRoutingTable()).thenReturn(routingTable);

        initializeClusterState(shard, unpromotableShard);

        try {
            PlainActionFuture<Boolean> f = PlainActionFuture.newFuture();
            postWriteRefresh.refreshShard(WriteRequest.RefreshPolicy.IMMEDIATE, shard, result.getTranslogLocation(), f);
            var illegalStateException = expectThrows(IllegalStateException.class, f::actionGet);
            assertEquals(errorMessage, illegalStateException.getMessage());
        } finally {
            IOUtils.close(() -> shard.close("test", false), shard.store());
        }
    }

    private void initializeClusterState(IndexShard shard, ShardRouting unpromotableShard) {
        PlainActionFuture.<Void, RuntimeException>get(
            future -> clusterService.getClusterApplierService().onNewClusterState("apply-custer-state", () -> {
                ClusterState currentState = clusterService.state();
                String indexName = shard.routingEntry().getIndexName();
                var metadataBuilder = Metadata.builder(currentState.metadata())
                    .put(
                        IndexMetadata.builder(indexName)
                            .settings(
                                Settings.builder()
                                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(SETTING_NUMBER_OF_REPLICAS, 1)
                                    .put(SETTING_CREATION_DATE, System.currentTimeMillis())
                            )
                            .primaryTerm(shard.routingEntry().shardId().getId(), shard.getOperationPrimaryTerm())
                            .build(),
                        true
                    )
                    .generateClusterUuidIfNeeded();
                return ClusterState.builder(currentState)
                    .routingTable(
                        RoutingTable.builder(currentState.routingTable())
                            .add(
                                IndexRoutingTable.builder(
                                    TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
                                    metadataBuilder.get(indexName).getIndex()
                                ).addShard(shard.routingEntry()).addShard(unpromotableShard).build()
                            )
                            .build()
                    )
                    .metadata(metadataBuilder)
                    .build();
            }, future),
            10,
            TimeUnit.SECONDS
        );
    }

}
