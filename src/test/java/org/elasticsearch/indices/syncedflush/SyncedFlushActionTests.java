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

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SyncedFlushActionTests extends ElasticsearchSingleNodeTest {
    final static public String INDEX = "test";
    final static public String TYPE = "test";

    @Test
    public void testPreSyncedFlushActionResponseFailure() throws ExecutionException, InterruptedException {
        createIndex(INDEX);
        ensureGreen(INDEX);
        int numShards = Integer.parseInt(getInstanceFromNode(ClusterService.class).state().metaData().index(INDEX).settings().get("index.number_of_shards"));
        client().prepareIndex(INDEX, TYPE).setSource("foo", "bar").get();
        TransportPreSyncedFlushAction transportPreSyncedFlushAction = getPreSyncedFlushAction();
        // try sync on a shard which is not there
        PreSyncedFlushRequest preSyncedFlushRequest = new PreSyncedFlushRequest(new ShardId(INDEX, numShards));
        try {
            transportPreSyncedFlushAction.execute(preSyncedFlushRequest).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ElasticsearchIllegalStateException);
        }
    }

    @Test
    public void testPreSyncedFlushActionResponse() throws ExecutionException, InterruptedException, IOException {
        createIndex(INDEX);
        ensureGreen(INDEX);
        client().prepareIndex(INDEX, TYPE).setSource("foo", "bar").get();
        TransportPreSyncedFlushAction transportPreSyncedFlushAction = getPreSyncedFlushAction();
        PreSyncedShardFlushRequest syncCommitRequest = new PreSyncedShardFlushRequest(getPrimaryShardRouting(), new PreSyncedFlushRequest(new ShardId(INDEX, 0)));
        PreSyncedShardFlushResponse syncCommitResponse = transportPreSyncedFlushAction.shardOperation(syncCommitRequest);
        assertArrayEquals(getLastWrittenCommitId(), syncCommitResponse.id());
    }

    @Test
    public void testSyncedFlushAction() throws ExecutionException, InterruptedException, IOException {
        createIndex(INDEX);
        ensureGreen(INDEX);
        client().prepareIndex(INDEX, TYPE).setSource("foo", "bar").get();
        client().admin().indices().prepareFlush(INDEX).get();
        TransportSyncedFlushAction transportSyncCommitAction = getSyncedFlushAction();
        String syncId = Strings.base64UUID();
        Map<String, byte[]> commitIds = new HashMap<>();
        commitIds.put(getPrimaryShardRouting().currentNodeId(), getLastWrittenCommitId());
        SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(new ShardId(INDEX, 0), syncId, commitIds);
        SyncedFlushResponse syncedFlushResponse = transportSyncCommitAction.execute(syncedFlushRequest).get();
        assertTrue(syncedFlushResponse.success());
        assertEquals(syncId, getLastWritenSyncId());
        // no see if fails if commit id is wrong
        byte[] invalid = getLastWrittenCommitId();
        invalid[0] = (byte) (invalid[0] ^ Byte.MAX_VALUE);
        commitIds.put(getPrimaryShardRouting().currentNodeId(), invalid);
        String newSyncId = syncId + syncId;
        syncedFlushRequest = new SyncedFlushRequest(new ShardId(INDEX, 0), newSyncId, commitIds);
        try {
            transportSyncCommitAction.execute(syncedFlushRequest).get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ElasticsearchIllegalStateException);
        }
        assertEquals(syncId, getLastWritenSyncId());
    }

    @Test
    public void testFailOnPrimaryIfOperationSneakedIn() throws ExecutionException, InterruptedException, IOException {
        createIndex(INDEX);
        ensureGreen(INDEX);
        client().prepareIndex(INDEX, TYPE).setSource("foo", "bar").get();
        client().admin().indices().prepareFlush(INDEX).get();
        TransportSyncedFlushAction transportSyncCommitAction = getSyncedFlushAction();
        String syncId = randomUnicodeOfLength(10);
        Map<String, byte[]> commitIds = new HashMap<>();
        commitIds.put(getPrimaryShardRouting().currentNodeId(), getLastWrittenCommitId());
        client().prepareIndex(INDEX, TYPE).setSource("foo", "bar").get();
        SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(new ShardId(INDEX, 0), syncId, commitIds);
        try {
            transportSyncCommitAction.execute(syncedFlushRequest).get();
            fail();
        } catch (ExecutionException e) {
            logger.info("got a ", e);
            assertTrue(e.getCause() instanceof ElasticsearchIllegalStateException);
        }
        assertNull(getLastWritenSyncId());
    }

    @Test
    @LuceneTestCase.AwaitsFix(bugUrl = "can only pass once https://github.com/elastic/elasticsearch/pull/10610 is in")
    public void testFailOnPrimaryIfOperationInFlight() throws ExecutionException, InterruptedException, IOException {
        createIndex(INDEX);
        ensureGreen(INDEX);
        client().admin().indices().prepareFlush(INDEX).get();
        TransportSyncedFlushAction transportSyncCommitAction = getSyncedFlushAction();
        String syncId = randomUnicodeOfLength(10);
        Map<String, byte[]> commitIds = new HashMap<>();
        byte[] commitId = getLastWrittenCommitId();
        commitIds.put(getPrimaryShardRouting().currentNodeId(), commitId);
        DelayedTransportIndexAction delayedTransportIndexAction = getDelayedTransportIndexAction();
        Future<IndexResponse> indexResponse = delayedTransportIndexAction.execute(new IndexRequest("test", "doc").source("{\"foo\":\"bar\"}"));

        SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(new ShardId(INDEX, 0), syncId, commitIds);
        try {
            transportSyncCommitAction.execute(syncedFlushRequest).get();
            fail("should not attempt sync if operation is in flight");
        } catch (ExecutionException e) {
            logger.info("got a ", e);
            assertTrue(e.getCause() instanceof ElasticsearchIllegalStateException);
        }
        assertNull(getLastWritenSyncId());
        delayedTransportIndexAction.beginIndexLatch.countDown();
        indexResponse.get();
    }

    public TransportSyncedFlushAction getSyncedFlushAction() {
        TransportService transportService = getInstanceFromNode(TransportService.class);
        transportService.removeHandler(TransportSyncedFlushAction.NAME);
        transportService.removeHandler(TransportSyncedFlushAction.NAME + TransportShardReplicationOperationAction.getReplicaOperationNameSuffix());
        return getInstanceFromNode(TransportSyncedFlushAction.class);
    }

    public TransportPreSyncedFlushAction getPreSyncedFlushAction() {
        TransportService transportService = getInstanceFromNode(TransportService.class);
        transportService.removeHandler(TransportPreSyncedFlushAction.NAME);
        transportService.removeHandler(TransportPreSyncedFlushAction.NAME + TransportPreSyncedFlushAction.getShardOperationNameSuffix());
        return getInstanceFromNode(TransportPreSyncedFlushAction.class);
    }

    public byte[] getLastWrittenCommitId() throws IOException {
        return Base64.decode(client().admin().indices().prepareStats(INDEX).get().getIndex(INDEX).getShards()[0].getCommitStats().getId());
    }

    public String getLastWritenSyncId() throws IOException {
        IndexStats indexStats =  client().admin().indices().prepareStats(INDEX).get().getIndex(INDEX);
        return indexStats.getShards()[0].getCommitStats().getUserData().get(Engine.SYNC_COMMIT_ID);
    }

    public ShardRouting getPrimaryShardRouting() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        return clusterService.state().routingTable().indicesRouting().get(INDEX).shard(0).primaryShard();
    }

    DelayedTransportIndexAction getDelayedTransportIndexAction() {
        TransportService transportService = getInstanceFromNode(TransportService.class);
        transportService.removeHandler(IndexAction.NAME);
        transportService.removeHandler(getInstanceFromNode(TransportIndexAction.class).getReplicaActionName());
        return new DelayedTransportIndexAction(
                getInstanceFromNode(Settings.class),
                getInstanceFromNode(TransportService.class),
                getInstanceFromNode(ClusterService.class),
                getInstanceFromNode(IndicesService.class),
                getInstanceFromNode(ThreadPool.class),
                getInstanceFromNode(ShardStateAction.class),
                getInstanceFromNode(TransportCreateIndexAction.class),
                getInstanceFromNode(MappingUpdatedAction.class),
                getInstanceFromNode(ActionFilters.class)
        );
    }

    // delays indexing until counter has been dec
    public static class DelayedTransportIndexAction extends TransportIndexAction {

        CountDownLatch beginIndexLatch = new CountDownLatch(1);

        public DelayedTransportIndexAction(Settings settings, TransportService transportService, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction, TransportCreateIndexAction createIndexAction, MappingUpdatedAction mappingUpdatedAction, ActionFilters actionFilters) {
            super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction, createIndexAction, mappingUpdatedAction, actionFilters);
        }

        @Override
        protected Tuple<IndexResponse, IndexRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
            beginIndexLatch.await();
            Tuple<IndexResponse, IndexRequest> response = super.shardOperationOnPrimary(clusterState, shardRequest);
            return response;
        }
    }

}
