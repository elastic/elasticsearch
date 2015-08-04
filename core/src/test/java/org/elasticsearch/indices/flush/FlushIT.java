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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

public class FlushIT extends ESIntegTestCase {

    @Test
    public void testWaitIfOngoing() throws InterruptedException {
        createIndex("test");
        ensureGreen("test");
        final int numIters = scaledRandomIntBetween(10, 30);
        for (int i = 0; i < numIters; i++) {
            for (int j = 0; j < 10; j++) {
                client().prepareIndex("test", "test").setSource("{}").get();
            }
            final CountDownLatch latch = new CountDownLatch(10);
            final CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
            for (int j = 0; j < 10; j++) {
                client().admin().indices().prepareFlush("test").setWaitIfOngoing(true).execute(new ActionListener<FlushResponse>() {
                    @Override
                    public void onResponse(FlushResponse flushResponse) {
                        try {
                            // dont' use assertAllSuccesssful it uses a randomized context that belongs to a different thread
                            assertThat("Unexpected ShardFailures: " + Arrays.toString(flushResponse.getShardFailures()), flushResponse.getFailedShards(), equalTo(0));
                            latch.countDown();
                        } catch (Throwable ex) {
                            onFailure(ex);
                        }

                    }

                    @Override
                    public void onFailure(Throwable e) {
                        errors.add(e);
                        latch.countDown();
                    }
                });
            }
            latch.await();
            assertThat(errors, emptyIterable());
        }
    }

    @TestLogging("indices:TRACE")
    public void testSyncedFlush() throws ExecutionException, InterruptedException, IOException {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("test").setSettings(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).get();
        ensureGreen();

        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }

        ShardsSyncedFlushResult result;
        if (randomBoolean()) {
            logger.info("--> sync flushing shard 0");
            result = SyncedFlushUtil.attemptSyncedFlush(internalCluster(), new ShardId("test", 0));
        } else {
            logger.info("--> sync flushing index [test]");
            IndicesSyncedFlushResult indicesResult = SyncedFlushUtil.attemptSyncedFlush(internalCluster(), "test");
            result = indicesResult.getShardsResultPerIndex().get("test").get(0);
        }
        assertFalse(result.failed());
        assertThat(result.totalShards(), equalTo(indexStats.getShards().length));
        assertThat(result.successfulShards(), equalTo(indexStats.getShards().length));

        indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        String syncId = result.syncId();
        for (ShardStats shardStats : indexStats.getShards()) {
            final String shardSyncId = shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID);
            assertThat(shardSyncId, equalTo(syncId));
        }

        // now, start new node and relocate a shard there and see if sync id still there
        String newNodeName = internalCluster().startNode();
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        ShardRouting shardRouting = clusterState.getRoutingTable().index("test").shard(0).iterator().next();
        String currentNodeName = clusterState.nodes().resolveNode(shardRouting.currentNodeId()).name();
        assertFalse(currentNodeName.equals(newNodeName));
        internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(new ShardId("test", 0), currentNodeName, newNodeName)).get();

        client().admin().cluster().prepareHealth()
                .setWaitForRelocatingShards(0)
                .get();
        indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }

        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).build()).get();
        ensureGreen("test");
        indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, internalCluster().numDataNodes() - 1).build()).get();
        ensureGreen("test");
        indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
    }

    @TestLogging("indices:TRACE")
    public void testSyncedFlushWithConcurrentIndexing() throws Exception {

        internalCluster().ensureAtLeastNumDataNodes(3);
        createIndex("test");

        client().admin().indices().prepareUpdateSettings("test").setSettings(
                Settings.builder().put("index.translog.disable_flush", true).put("index.refresh_interval", -1).put("index.number_of_replicas", internalCluster().numDataNodes() - 1))
                .get();
        ensureGreen();
        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicInteger numDocs = new AtomicInteger(0);
        Thread indexingThread = new Thread() {
            @Override
            public void run() {
                while (stop.get() == false) {
                    client().prepareIndex().setIndex("test").setType("doc").setSource("{}").get();
                    numDocs.incrementAndGet();
                }
            }
        };
        indexingThread.start();

        IndexStats indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        for (ShardStats shardStats : indexStats.getShards()) {
            assertNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
        }
        logger.info("--> trying sync flush");
        IndicesSyncedFlushResult syncedFlushResult = SyncedFlushUtil.attemptSyncedFlush(internalCluster(), "test");
        logger.info("--> sync flush done");
        stop.set(true);
        indexingThread.join();
        indexStats = client().admin().indices().prepareStats("test").get().getIndex("test");
        assertFlushResponseEqualsShardStats(indexStats.getShards(), syncedFlushResult.getShardsResultPerIndex().get("test"));
        refresh();
        assertThat(client().prepareCount().get().getCount(), equalTo((long) numDocs.get()));
        logger.info("indexed {} docs", client().prepareCount().get().getCount());
        logClusterState();
        internalCluster().fullRestart();
        ensureGreen();
        assertThat(client().prepareCount().get().getCount(), equalTo((long) numDocs.get()));
    }

    private void assertFlushResponseEqualsShardStats(ShardStats[] shardsStats, List<ShardsSyncedFlushResult> syncedFlushResults) {

        for (final ShardStats shardStats : shardsStats) {
            for (final ShardsSyncedFlushResult shardResult : syncedFlushResults) {
                if (shardStats.getShardRouting().getId() == shardResult.shardId().getId()) {
                    for (Map.Entry<ShardRouting, SyncedFlushService.SyncedFlushResponse> singleResponse : shardResult.shardResponses().entrySet()) {
                        if (singleResponse.getKey().currentNodeId().equals(shardStats.getShardRouting().currentNodeId())) {
                            if (singleResponse.getValue().success()) {
                                logger.info("{} sync flushed on node {}", singleResponse.getKey().shardId(), singleResponse.getKey().currentNodeId());
                                assertNotNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
                            } else {
                                logger.info("{} sync flush failed for on node {}", singleResponse.getKey().shardId(), singleResponse.getKey().currentNodeId());
                                assertNull(shardStats.getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID));
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testUnallocatedShardsDoesNotHang() throws InterruptedException {
        //  create an index but disallow allocation
        prepareCreate("test").setSettings(Settings.builder().put("index.routing.allocation.include._name", "nonexistent")).get();

        // this should not hang but instead immediately return with empty result set
        List<ShardsSyncedFlushResult> shardsResult = SyncedFlushUtil.attemptSyncedFlush(internalCluster(), "test").getShardsResultPerIndex().get("test");
        // just to make sure the test actually tests the right thing
        int numShards = client().admin().indices().prepareGetSettings("test").get().getIndexToSettings().get("test").getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, -1);
        assertThat(shardsResult.size(), equalTo(numShards));
        assertThat(shardsResult.get(0).failureReason(), equalTo("no active shards"));
    }

}
