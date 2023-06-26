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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.UnpromotableShardRefreshRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.DocIdSeqNoAndSource;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class PostWriteRefreshTests extends IndexShardTestCase {

    private final TimeValue postWriteRefreshTimeout = TimeValue.timeValueSeconds(30);
    private final AtomicBoolean unpromotableRefreshRequestReceived = new AtomicBoolean(false);
    private TransportService transportService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        transportService = MockTransportService.createNewService(Settings.EMPTY, Version.CURRENT, TransportVersion.current(), threadPool);
        transportService.start();
        transportService.acceptIncomingRequests();
        transportService.registerRequestHandler(
            TransportUnpromotableShardRefreshAction.NAME,
            ThreadPool.Names.SAME,
            UnpromotableShardRefreshRequest::new,
            (request, channel, task) -> {
                unpromotableRefreshRequestReceived.set(true);
                channel.sendResponse(ActionResponse.Empty.INSTANCE);
            }
        );

    }

    @Override
    public void tearDown() throws Exception {
        transportService.close();
        super.tearDown();
    }

    public void testWaitUntilRefreshPrimaryShard() throws IOException {
        final IndexShard primary = newShard(true);
        recoverShardFromStore(primary);
        try {
            String id = "0";
            Engine.IndexResult result = indexDoc(primary, "_doc", id);
            PlainActionFuture<Boolean> f = PlainActionFuture.newFuture();
            PostWriteRefresh postWriteRefresh = new PostWriteRefresh(transportService);
            postWriteRefresh.refreshShard(
                WriteRequest.RefreshPolicy.WAIT_UNTIL,
                primary,
                result.getTranslogLocation(),
                f,
                postWriteRefreshTimeout
            );
            Releasable releasable = simulateScheduledRefresh(primary, false);
            f.actionGet();
            assertFalse(unpromotableRefreshRequestReceived.get());
            assertEngineContainsIdNoRefresh(primary, id);
            releasable.close();
        } finally {
            closeShards(primary, primary);
        }
    }

    public void testImmediateRefreshPrimaryShard() throws IOException {
        final IndexShard primary = newShard(true);
        recoverShardFromStore(primary);
        try {
            String id = "0";
            Engine.IndexResult result = indexDoc(primary, "_doc", id);
            PlainActionFuture<Boolean> f = PlainActionFuture.newFuture();
            PostWriteRefresh postWriteRefresh = new PostWriteRefresh(transportService);
            postWriteRefresh.refreshShard(
                WriteRequest.RefreshPolicy.IMMEDIATE,
                primary,
                result.getTranslogLocation(),
                f,
                postWriteRefreshTimeout
            );
            f.actionGet();
            assertFalse(unpromotableRefreshRequestReceived.get());
            assertEngineContainsIdNoRefresh(primary, id);
        } finally {
            closeShards(primary, primary);
        }
    }

    public void testPrimaryWithUnpromotables() throws IOException {
        final IndexShard primary = spy(newShard(true));
        recoverShardFromStore(primary);
        ReplicationGroup realReplicationGroup = primary.getReplicationGroup();
        try {
            String id = "0";
            Engine.IndexResult result = indexDoc(primary, "_doc", id);
            PlainActionFuture<Boolean> f = PlainActionFuture.newFuture();
            PostWriteRefresh postWriteRefresh = new PostWriteRefresh(transportService);

            ReplicationGroup replicationGroup = mock(ReplicationGroup.class);
            IndexShardRoutingTable routingTable = mock(IndexShardRoutingTable.class);
            ShardId shardId = primary.shardId();
            ShardRouting routing = ShardRouting.newUnassigned(
                shardId,
                true,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
                ShardRouting.Role.INDEX_ONLY
            );
            when(primary.routingEntry()).thenReturn(routing);
            when(primary.getReplicationGroup()).thenReturn(replicationGroup).thenReturn(realReplicationGroup);
            when(replicationGroup.getRoutingTable()).thenReturn(routingTable);
            ShardRouting shardRouting = ShardRouting.newUnassigned(
                shardId,
                false,
                RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "message"),
                ShardRouting.Role.SEARCH_ONLY
            );
            when(routingTable.unpromotableShards()).thenReturn(List.of(shardRouting));
            when(routingTable.shardId()).thenReturn(shardId);
            WriteRequest.RefreshPolicy policy = randomFrom(WriteRequest.RefreshPolicy.IMMEDIATE, WriteRequest.RefreshPolicy.WAIT_UNTIL);
            postWriteRefresh.refreshShard(policy, primary, result.getTranslogLocation(), f, postWriteRefreshTimeout);
            final Releasable releasable;
            if (policy == WriteRequest.RefreshPolicy.WAIT_UNTIL) {
                releasable = simulateScheduledRefresh(primary, true);
            } else {
                releasable = () -> {};
            }
            f.actionGet();
            assertTrue(unpromotableRefreshRequestReceived.get());
            assertEngineContainsIdNoRefresh(primary, id);
            releasable.close();
        } finally {
            closeShards(primary, primary);
        }
    }

    public void testWaitUntilRefreshReplicaShard() throws IOException {
        final IndexShard primary = newShard(true);
        recoverShardFromStore(primary);
        final IndexShard replica = newShard(false);
        recoverReplica(replica, primary, true);
        try {
            String id = "0";
            Engine.IndexResult result = indexDoc(replica, "_doc", id);
            PlainActionFuture<Boolean> f = PlainActionFuture.newFuture();
            PostWriteRefresh.refreshReplicaShard(WriteRequest.RefreshPolicy.WAIT_UNTIL, replica, result.getTranslogLocation(), f);
            Releasable releasable = simulateScheduledRefresh(replica, false);
            f.actionGet();
            assertEngineContainsIdNoRefresh(replica, id);
            releasable.close();
        } finally {
            closeShards(primary, replica);
        }
    }

    public void testImmediateRefreshReplicaShard() throws IOException {
        final IndexShard primary = newShard(true);
        recoverShardFromStore(primary);
        final IndexShard replica = newShard(false);
        recoverReplica(replica, primary, true);
        try {
            String id = "0";
            Engine.IndexResult result = indexDoc(replica, "_doc", id);
            PlainActionFuture<Boolean> f = PlainActionFuture.newFuture();
            PostWriteRefresh.refreshReplicaShard(WriteRequest.RefreshPolicy.IMMEDIATE, replica, result.getTranslogLocation(), f);
            f.actionGet();
            assertEngineContainsIdNoRefresh(replica, id);
        } finally {
            closeShards(primary, replica);
        }
    }

    public void testWaitForWithNullLocationCompletedImmediately() throws IOException {
        final IndexShard primary = spy(newShard(true));
        recoverShardFromStore(primary);
        ReplicationGroup realReplicationGroup = primary.getReplicationGroup();
        try {
            PlainActionFuture<Boolean> f = PlainActionFuture.newFuture();
            PostWriteRefresh postWriteRefresh = new PostWriteRefresh(transportService);

            ReplicationGroup replicationGroup = mock(ReplicationGroup.class);
            IndexShardRoutingTable routingTable = mock(IndexShardRoutingTable.class);
            when(primary.getReplicationGroup()).thenReturn(replicationGroup).thenReturn(realReplicationGroup);
            when(replicationGroup.getRoutingTable()).thenReturn(routingTable);
            ShardRouting shardRouting = ShardRouting.newUnassigned(
                primary.shardId(),
                false,
                RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "message"),
                ShardRouting.Role.SEARCH_ONLY
            );
            // Randomly test scenarios with and without unpromotables
            if (randomBoolean()) {
                when(routingTable.unpromotableShards()).thenReturn(Collections.emptyList());
            } else {
                when(routingTable.unpromotableShards()).thenReturn(List.of(shardRouting));
            }
            WriteRequest.RefreshPolicy policy = WriteRequest.RefreshPolicy.WAIT_UNTIL;
            postWriteRefresh.refreshShard(policy, primary, null, f, postWriteRefreshTimeout);
            f.actionGet();
        } finally {
            closeShards(primary, primary);
        }
    }

    private static void assertEngineContainsIdNoRefresh(IndexShard replica, String id) throws IOException {
        List<DocIdSeqNoAndSource> docIds = EngineTestCase.getDocIds(replica.getEngineOrNull(), false);
        Set<String> ids = docIds.stream().map(DocIdSeqNoAndSource::id).collect(Collectors.toSet());
        assertThat(ids, contains(id));
    }

    private static Releasable simulateScheduledRefresh(IndexShard shard, boolean needsFlush) {
        // Simulate periodic refresh
        Thread thread = new Thread(() -> {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(randomLongBetween(100, 500)));
            shard.refresh("test");
            if (needsFlush) {
                shard.flush(new FlushRequest());
            }
        });
        thread.start();
        return () -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

}
