/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.UnpromotableShardRefreshRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class PostWriteRefreshFailTests extends IndexShardTestCase {

    private final String errorMessage = "Unable to update to refresh an unpromotable shards";

    private TransportService transportService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, TransportVersion.CURRENT, threadPool);
        transportService.start();
        transportService.acceptIncomingRequests();
        transportService.registerRequestHandler(
            TransportUnpromotableShardRefreshAction.NAME,
            ThreadPool.Names.SAME,
            UnpromotableShardRefreshRequest::new,
            (request, channel, task) -> channel.sendResponse(new IllegalStateException(errorMessage))
        );
    }

    @Override
    public void tearDown() throws Exception {
        transportService.close();
        super.tearDown();
    }

    public void testFailShardIfUnpromotableRequestFails() throws IOException {
        allowShardFailures();

        IndexShard shard = spy(newShard(true));
        recoverShardFromStore(shard);
        ReplicationGroup realReplicationGroup = shard.getReplicationGroup();
        Engine.IndexResult result = indexDoc(shard, "_doc", "0");
        ShardStateAction shardStateAction = mock(ShardStateAction.class);
        PostWriteRefresh postWriteRefresh = new PostWriteRefresh(transportService, shardStateAction);

        ReplicationGroup mockReplicationGroup = mock(ReplicationGroup.class);
        when(shard.getReplicationGroup()).thenReturn(mockReplicationGroup).thenReturn(realReplicationGroup);

        IndexShardRoutingTable routingTable = mock(IndexShardRoutingTable.class);
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shard.shardId(),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "message"),
            ShardRouting.Role.SEARCH_ONLY
        );
        when(routingTable.unpromotableShards()).thenReturn(List.of(shardRouting));
        when(mockReplicationGroup.getRoutingTable()).thenReturn(routingTable);

        Mockito.doAnswer(invocation -> {
            ActionListener<Void> argument = invocation.getArgument(6);
            argument.onFailure(new IllegalStateException(errorMessage));
            return null;
        })
            .when(shardStateAction)
            .remoteShardFailed(any(ShardId.class), anyString(), anyLong(), anyBoolean(), anyString(), any(Exception.class), any());

        try {
            PlainActionFuture<Boolean> f = PlainActionFuture.newFuture();
            postWriteRefresh.refreshShard(WriteRequest.RefreshPolicy.IMMEDIATE, shard, result.getTranslogLocation(), f);
            var illegalStateException = expectThrows(IllegalStateException.class, f::actionGet);
            assertEquals(errorMessage, illegalStateException.getMessage());
        } finally {
            IOUtils.close(() -> shard.close("test", false), shard.store());
        }
    }
}
