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
package org.elasticsearch.indices.syncedflush;

import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SyncedFlushTests extends ElasticsearchIntegrationTest {

    static final String INDEX = "test";
    static final String TYPE = "test";

    @Test
    public void testCommitIdsReturnedCorrectly() throws InterruptedException, IOException, ExecutionException {
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(
                ImmutableSettings.builder().put("index.number_of_replicas", internalCluster().numDataNodes() - 1)
                        .put("index.number_of_shards", 1)
                        .put("index.translog.flush_threshold_period", "1m")));
        ensureGreen("test");
        for (int j = 0; j < 10; j++) {
            client().prepareIndex("test", "test").setSource("{}").get();
        }
        TransportPreSyncedFlushAction transportPreSyncedFlushAction = internalCluster().getInstance(TransportPreSyncedFlushAction.class);
        PreSyncedFlushResponse preSyncedFlushResponse = transportPreSyncedFlushAction.execute(new PreSyncedFlushRequest(new ShardId("test", 0))).get();
        assertThat(preSyncedFlushResponse.getFailedShards(), equalTo(0));
        assertThat(preSyncedFlushResponse.commitIds.size(), equalTo(internalCluster().numDataNodes()));
        // TODO: use stats api once it is in
        for (Map.Entry<String, byte[]> entry : preSyncedFlushResponse.commitIds.entrySet()) {
            String nodeName = getNodeNameFromNodeId(entry.getKey());
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
            IndexShard indexShard = indicesService.indexService("test").shard(0);
            Store store = indexShard.engine().config().getStore();
            SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
            assertArrayEquals(segmentInfos.getId(), entry.getValue());
        }
    }

    // check that failures on replicas show up in response
    public void testRepActionResponseWithFailure() throws InterruptedException, ExecutionException, IOException {
        createIndex(INDEX);
        ensureGreen(INDEX);
        int numberOfShards = getNumShards(INDEX).numPrimaries;
        // create cluster state observer here to capture the first state
        String observingNode = randomFrom(internalCluster().getNodeNames());
        final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(internalCluster().getInstance(ClusterService.class, observingNode), TimeValue.timeValueMillis(100), logger);
        // make a list of all the shards that should fail and their nodes and their nodes
        final AtomicReference<String> failure = new AtomicReference<>();
        final AtomicBoolean stop = new AtomicBoolean(false);

        client().prepareIndex(INDEX, TYPE).setSource("foo", "bar").get();
        TransportPreSyncedFlushAction transportPreSyncedFlushAction = internalCluster().getInstance(TransportPreSyncedFlushAction.class);
        ShardId shardId = new ShardId(INDEX, randomInt(numberOfShards - 1));
        PreSyncedFlushResponse preSyncedFlushResponse = transportPreSyncedFlushAction.execute(new PreSyncedFlushRequest(shardId)).get();
        String result = randomFrom(preSyncedFlushResponse.commitIds().keySet().toArray(new String[preSyncedFlushResponse.commitIds().size()]));
        byte[] wrongCommitId = preSyncedFlushResponse.commitIds().get(result);
        wrongCommitId[0] ^= 1;
        try {
            TransportSyncedFlushAction transportSyncedFlushAction = internalCluster().getInstance(TransportSyncedFlushAction.class);
            assertNotNull(preSyncedFlushResponse.commitIds().put(result, wrongCommitId));
            SyncedFlushResponse syncedFlushResponse = transportSyncedFlushAction.execute(new SyncedFlushRequest(shardId, "123", preSyncedFlushResponse.commitIds())).get();
            assertTrue(syncedFlushResponse.success());
            assertThat(syncedFlushResponse.getShardInfo().getFailed(), equalTo(1));
        } catch (ExecutionException e) {
            logger.info("", e);
            if (e.getCause() instanceof RemoteTransportException) {
                assertTrue(e.getCause().getCause() instanceof ElasticsearchIllegalStateException);
                assertThat(e.getCause().getCause().getMessage(), containsString("could not sync commit on primary"));
            } else if (e.getCause() instanceof ElasticsearchIllegalStateException) {
                assertThat(e.getCause().getMessage(), containsString("could not sync commit on primary"));
            }
        }
        ensureGreen("test");
        if (failure.get() != null) {
            logger.error("there was a shard failure", failure.get());
            fail();
        }
    }


    public void testReplicaDoesNotFailOnFailedSyncFlush() throws InterruptedException, ExecutionException, IOException {
        createIndex(INDEX);
        ensureGreen(INDEX);
        int numberOfShards = getNumShards(INDEX).numPrimaries;
        // create cluster state observer here to capture the first state
        String observingNode = randomFrom(internalCluster().getNodeNames());
        final ClusterStateObserver clusterStateObserver = new ClusterStateObserver(internalCluster().getInstance(ClusterService.class, observingNode), TimeValue.timeValueMillis(100), logger);
        // make a list of all the shards that should fail and their nodes and their nodes
        final AtomicReference<String> failure = new AtomicReference<>();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Thread observerThread = new Thread() {
            public void run() {
                logger.info("init latch");
                final AtomicReference<CountDownLatch> observerLatch = new AtomicReference<>();
                observerLatch.getAndSet(new CountDownLatch(0));
                while (failure.get() == null && stop.get() == false) {
                    try {
                        observerLatch.get().await();
                    } catch (InterruptedException e) {
                        fail();
                    }
                    if (failure.get() != null || stop.get() == true) {
                        return;
                    }
                    observerLatch.set(new CountDownLatch(1));
                    clusterStateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
                        @Override
                        public void onNewClusterState(ClusterState state) {
                            if (state.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size() != 0) {
                                failure.set("found " + state.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size() + "initializing shards");
                            }
                            observerLatch.get().countDown();
                        }

                        @Override
                        public void onClusterServiceClose() {
                            failure.set("we should never get here unless the test times out");
                            observerLatch.get().countDown();
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            if (clusterService().state().routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size() != 0) {
                                failure.set("found " + clusterService().state().routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size() + "initializing shards");
                            }
                            observerLatch.get().countDown();
                        }
                    }, new ClusterStateObserver.ValidationPredicate() {
                        @Override
                        protected boolean validate(ClusterState newState) {
                            return newState.routingTable().shardsWithState(ShardRoutingState.INITIALIZING).size() != 0;
                        }
                    });
                }
            }
        };

        observerThread.start();
        client().prepareIndex(INDEX, TYPE).setSource("foo", "bar").get();
        TransportPreSyncedFlushAction transportPreSyncedFlushAction = internalCluster().getInstance(TransportPreSyncedFlushAction.class);
        ShardId shardId = new ShardId(INDEX, randomInt(numberOfShards - 1));
        PreSyncedFlushResponse preSyncedFlushResponse = transportPreSyncedFlushAction.execute(new PreSyncedFlushRequest(shardId)).get();
        String result = randomFrom(preSyncedFlushResponse.commitIds().keySet().toArray(new String[preSyncedFlushResponse.commitIds().size()]));
        byte[] wrongCommitId = preSyncedFlushResponse.commitIds().get(result);
        wrongCommitId[0] ^= 1;
        try {
            TransportSyncedFlushAction transportSyncedFlushAction = internalCluster().getInstance(TransportSyncedFlushAction.class);
            assertNotNull(preSyncedFlushResponse.commitIds().put(result, wrongCommitId));
            SyncedFlushResponse syncedFlushResponse = transportSyncedFlushAction.execute(new SyncedFlushRequest(shardId, "123", preSyncedFlushResponse.commitIds())).get();
            assertTrue(syncedFlushResponse.success());
            assertThat(syncedFlushResponse.getShardInfo().getFailed(), equalTo(1));
        } catch (ExecutionException e) {
            logger.info("", e);
            if (e.getCause() instanceof RemoteTransportException) {
                assertTrue(e.getCause().getCause() instanceof ElasticsearchIllegalStateException);
                assertThat(e.getCause().getCause().getMessage(), containsString("could not sync commit on primary"));
            } else if (e.getCause() instanceof ElasticsearchIllegalStateException) {
                assertThat(e.getCause().getMessage(), containsString("could not sync commit on primary"));
            }
        }
        ensureGreen("test");
        stop.set(true);
        observerThread.join();
        if (failure.get() != null) {
            fail("there was a shard failure");
        }
    }

    //TODO: add test for in flight on replica, with mock transport similar to https://github.com/elastic/elasticsearch/pull/10610/files ?

    private String getNodeNameFromNodeId(String nodeId) {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().get();
        for (RoutingNode routingNode : clusterStateResponse.getState().getRoutingNodes()) {
            if (routingNode.nodeId().equals(nodeId)) {
                return routingNode.node().name();
            }
        }
        return null;
    }
}
