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
package org.elasticsearch.indices.flush;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class SyncedFlushSingleNodeTests extends ESSingleNodeTestCase {

    public void testModificationPreventsFlushing() throws InterruptedException {
        createIndex("test");
        client().prepareIndex("test", "test", "1").setSource("{}", XContentType.JSON).get();
        IndexService test = getInstanceFromNode(IndicesService.class).indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);

        SyncedFlushService flushService = getInstanceFromNode(SyncedFlushService.class);
        final ShardId shardId = shard.shardId();
        final ClusterState state = getInstanceFromNode(ClusterService.class).state();
        final IndexShardRoutingTable shardRoutingTable = flushService.getShardRoutingTable(shardId, state);
        final List<ShardRouting> activeShards = shardRoutingTable.activeShards();
        assertEquals("exactly one active shard", 1, activeShards.size());
        Map<String, SyncedFlushService.PreSyncedFlushResponse> preSyncedResponses =
            SyncedFlushUtil.sendPreSyncRequests(flushService, activeShards, state, shardId);
        assertEquals("exactly one commit id", 1, preSyncedResponses.size());
        client().prepareIndex("test", "test", "2").setSource("{}", XContentType.JSON).get();
        String syncId = UUIDs.randomBase64UUID();
        SyncedFlushUtil.LatchedListener<ShardsSyncedFlushResult> listener = new SyncedFlushUtil.LatchedListener<>();
        flushService.sendSyncRequests(syncId, activeShards, state, preSyncedResponses, shardId, shardRoutingTable.size(), listener);
        listener.latch.await();
        assertNull(listener.error);
        ShardsSyncedFlushResult syncedFlushResult = listener.result;
        assertNotNull(syncedFlushResult);
        assertEquals(0, syncedFlushResult.successfulShards());
        assertEquals(1, syncedFlushResult.totalShards());
        assertEquals(syncId, syncedFlushResult.syncId());
        assertNotNull(syncedFlushResult.shardResponses().get(activeShards.get(0)));
        assertFalse(syncedFlushResult.shardResponses().get(activeShards.get(0)).success());
        assertEquals("pending operations", syncedFlushResult.shardResponses().get(activeShards.get(0)).failureReason());

        // pull another commit and make sure we can't sync-flush with the old one
        SyncedFlushUtil.sendPreSyncRequests(flushService, activeShards, state, shardId);
        listener = new SyncedFlushUtil.LatchedListener<>();
        flushService.sendSyncRequests(syncId, activeShards, state, preSyncedResponses, shardId, shardRoutingTable.size(), listener);
        listener.latch.await();
        assertNull(listener.error);
        syncedFlushResult = listener.result;
        assertNotNull(syncedFlushResult);
        assertEquals(0, syncedFlushResult.successfulShards());
        assertEquals(1, syncedFlushResult.totalShards());
        assertEquals(syncId, syncedFlushResult.syncId());
        assertNotNull(syncedFlushResult.shardResponses().get(activeShards.get(0)));
        assertFalse(syncedFlushResult.shardResponses().get(activeShards.get(0)).success());
        assertEquals("commit has changed", syncedFlushResult.shardResponses().get(activeShards.get(0)).failureReason());
    }

    public void testSingleShardSuccess() throws InterruptedException {
        createIndex("test");
        client().prepareIndex("test", "test", "1").setSource("{}", XContentType.JSON).get();
        IndexService test = getInstanceFromNode(IndicesService.class).indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);

        SyncedFlushService flushService = getInstanceFromNode(SyncedFlushService.class);
        final ShardId shardId = shard.shardId();
        SyncedFlushUtil.LatchedListener<ShardsSyncedFlushResult> listener = new SyncedFlushUtil.LatchedListener<>();
        flushService.attemptSyncedFlush(shardId, listener);
        listener.latch.await();
        assertNull(listener.error);
        ShardsSyncedFlushResult syncedFlushResult = listener.result;
        assertNotNull(syncedFlushResult);
        assertEquals(1, syncedFlushResult.successfulShards());
        assertEquals(1, syncedFlushResult.totalShards());
        SyncedFlushService.ShardSyncedFlushResponse response = syncedFlushResult.shardResponses().values().iterator().next();
        assertTrue(response.success());
    }

    public void testSyncFailsIfOperationIsInFlight() throws Exception {
        createIndex("test");
        client().prepareIndex("test", "test", "1").setSource("{}", XContentType.JSON).get();
        IndexService test = getInstanceFromNode(IndicesService.class).indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);

        // wait for the GCP sync spawned from the index request above to complete to avoid that request disturbing the check below
        assertBusy(() -> {
            assertEquals(0, shard.getLastSyncedGlobalCheckpoint());
            assertEquals(0, shard.getActiveOperationsCount());
        });

        SyncedFlushService flushService = getInstanceFromNode(SyncedFlushService.class);
        final ShardId shardId = shard.shardId();
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        shard.acquirePrimaryOperationPermit(fut, ThreadPool.Names.WRITE, "");
        try (Releasable operationLock = fut.get()) {
            SyncedFlushUtil.LatchedListener<ShardsSyncedFlushResult> listener = new SyncedFlushUtil.LatchedListener<>();
            flushService.attemptSyncedFlush(shardId, listener);
            listener.latch.await();
            assertNull(listener.error);
            ShardsSyncedFlushResult syncedFlushResult = listener.result;
            assertNotNull(syncedFlushResult);
            assertEquals(0, syncedFlushResult.successfulShards());
            assertNotEquals(0, syncedFlushResult.totalShards());
            assertEquals("[1] ongoing operations on primary", syncedFlushResult.failureReason());
        }
    }

    public void testSyncFailsOnIndexClosedOrMissing() throws InterruptedException {
        createIndex("test", Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .build());
        IndexService test = getInstanceFromNode(IndicesService.class).indexService(resolveIndex("test"));
        final IndexShard shard = test.getShardOrNull(0);
        assertNotNull(shard);
        final ShardId shardId = shard.shardId();

        final SyncedFlushService flushService = getInstanceFromNode(SyncedFlushService.class);

        SyncedFlushUtil.LatchedListener listener = new SyncedFlushUtil.LatchedListener();
        flushService.attemptSyncedFlush(new ShardId(shard.shardId().getIndex(), 1), listener);
        listener.latch.await();
        assertNotNull(listener.error);
        assertNull(listener.result);
        assertEquals(ShardNotFoundException.class, listener.error.getClass());
        assertEquals("no such shard", listener.error.getMessage());

        assertAcked(client().admin().indices().prepareClose("test"));
        listener = new SyncedFlushUtil.LatchedListener();
        flushService.attemptSyncedFlush(shardId, listener);
        listener.latch.await();
        assertNotNull(listener.error);
        assertNull(listener.result);
        assertEquals("closed", listener.error.getMessage());

        listener = new SyncedFlushUtil.LatchedListener();
        flushService.attemptSyncedFlush(new ShardId("index not found", "_na_", 0), listener);
        listener.latch.await();
        assertNotNull(listener.error);
        assertNull(listener.result);
        assertEquals("no such index [index not found]", listener.error.getMessage());
    }

    public void testFailAfterIntermediateCommit() throws InterruptedException {
        createIndex("test");
        client().prepareIndex("test", "test", "1").setSource("{}", XContentType.JSON).get();
        IndexService test = getInstanceFromNode(IndicesService.class).indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);

        SyncedFlushService flushService = getInstanceFromNode(SyncedFlushService.class);
        final ShardId shardId = shard.shardId();
        final ClusterState state = getInstanceFromNode(ClusterService.class).state();
        final IndexShardRoutingTable shardRoutingTable = flushService.getShardRoutingTable(shardId, state);
        final List<ShardRouting> activeShards = shardRoutingTable.activeShards();
        assertEquals("exactly one active shard", 1, activeShards.size());
        Map<String, SyncedFlushService.PreSyncedFlushResponse> preSyncedResponses =
            SyncedFlushUtil.sendPreSyncRequests(flushService, activeShards, state, shardId);
        assertEquals("exactly one commit id", 1, preSyncedResponses.size());
        if (randomBoolean()) {
            client().prepareIndex("test", "test", "2").setSource("{}", XContentType.JSON).get();
        }
        client().admin().indices().prepareFlush("test").setForce(true).get();
        String syncId = UUIDs.randomBase64UUID();
        final SyncedFlushUtil.LatchedListener<ShardsSyncedFlushResult> listener = new SyncedFlushUtil.LatchedListener<>();
        flushService.sendSyncRequests(syncId, activeShards, state, preSyncedResponses, shardId, shardRoutingTable.size(), listener);
        listener.latch.await();
        assertNull(listener.error);
        ShardsSyncedFlushResult syncedFlushResult = listener.result;
        assertNotNull(syncedFlushResult);
        assertEquals(0, syncedFlushResult.successfulShards());
        assertEquals(1, syncedFlushResult.totalShards());
        assertEquals(syncId, syncedFlushResult.syncId());
        assertNotNull(syncedFlushResult.shardResponses().get(activeShards.get(0)));
        assertFalse(syncedFlushResult.shardResponses().get(activeShards.get(0)).success());
        assertEquals("commit has changed", syncedFlushResult.shardResponses().get(activeShards.get(0)).failureReason());
    }

    public void testFailWhenCommitIsMissing() throws InterruptedException {
        createIndex("test");
        client().prepareIndex("test", "test", "1").setSource("{}", XContentType.JSON).get();
        IndexService test = getInstanceFromNode(IndicesService.class).indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);

        SyncedFlushService flushService = getInstanceFromNode(SyncedFlushService.class);
        final ShardId shardId = shard.shardId();
        final ClusterState state = getInstanceFromNode(ClusterService.class).state();
        final IndexShardRoutingTable shardRoutingTable = flushService.getShardRoutingTable(shardId, state);
        final List<ShardRouting> activeShards = shardRoutingTable.activeShards();
        assertEquals("exactly one active shard", 1, activeShards.size());
        Map<String, SyncedFlushService.PreSyncedFlushResponse> preSyncedResponses =
            SyncedFlushUtil.sendPreSyncRequests(flushService, activeShards, state, shardId);
        assertEquals("exactly one commit id", 1, preSyncedResponses.size());
        preSyncedResponses.clear(); // wipe it...
        String syncId = UUIDs.randomBase64UUID();
        SyncedFlushUtil.LatchedListener<ShardsSyncedFlushResult> listener = new SyncedFlushUtil.LatchedListener<>();
        flushService.sendSyncRequests(syncId, activeShards, state, preSyncedResponses, shardId, shardRoutingTable.size(), listener);
        listener.latch.await();
        assertNull(listener.error);
        ShardsSyncedFlushResult syncedFlushResult = listener.result;
        assertNotNull(syncedFlushResult);
        assertEquals(0, syncedFlushResult.successfulShards());
        assertEquals(1, syncedFlushResult.totalShards());
        assertEquals(syncId, syncedFlushResult.syncId());
        assertNotNull(syncedFlushResult.shardResponses().get(activeShards.get(0)));
        assertFalse(syncedFlushResult.shardResponses().get(activeShards.get(0)).success());
        assertEquals("no commit id from pre-sync flush", syncedFlushResult.shardResponses().get(activeShards.get(0)).failureReason());
    }


}
