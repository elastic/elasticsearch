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

package org.elasticsearch.index;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShadowIndexShard;
import org.elasticsearch.index.store.FsDirectoryService;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryTargetService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests for indices that use shadow replicas and a shared filesystem
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexWithShadowReplicasIT extends ESIntegTestCase {

    private Settings nodeSettings(Path dataPath) {
        return nodeSettings(dataPath.toString());
    }

    private Settings nodeSettings(String dataPath) {
        return Settings.builder()
                .put(NodeEnvironment.ADD_NODE_ID_TO_CUSTOM_PATH.getKey(), false)
                .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), dataPath)
                .put(FsDirectoryService.INDEX_LOCK_FACTOR_SETTING.getKey(), randomFrom("native", "simple"))
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(MockTransportService.TestPlugin.class);
    }

    public void testCannotCreateWithBadPath() throws Exception {
        Settings nodeSettings = nodeSettings("/badpath");
        internalCluster().startNodesAsync(1, nodeSettings).get();
        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, "/etc/foo")
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).build();
        try {
            assertAcked(prepareCreate("foo").setSettings(idxSettings));
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(),
                    e.getMessage().contains("custom path [/etc/foo] is not a sub-path of path.shared_data"));
        }
    }

    /**
     * Tests the case where we create an index without shadow replicas, snapshot it and then restore into
     * an index with shadow replicas enabled.
     */
    public void testRestoreToShadow() throws ExecutionException, InterruptedException {
        final Path dataPath = createTempDir();
        Settings nodeSettings = nodeSettings(dataPath);

        internalCluster().startNodesAsync(3, nodeSettings).get();
        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).build();
        assertAcked(prepareCreate("foo").setSettings(idxSettings));
        ensureGreen();
        final int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("foo", "doc", ""+i).setSource("foo", "bar").get();
        }
        assertNoFailures(client().admin().indices().prepareFlush().setForce(true).setWaitIfOngoing(true).execute().actionGet());

        assertAcked(client().admin().cluster().preparePutRepository("test-repo")
                .setType("fs").setSettings(Settings.builder()
                        .put("location", randomRepoPath())));
        CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("foo").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(createSnapshotResponse.getSnapshotInfo().totalShards()));
        assertThat(client().admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).state(), equalTo(SnapshotState.SUCCESS));

        Settings shadowSettings = Settings.builder()
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2).build();

        logger.info("--> restore the index into shadow replica index");
        RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap")
                .setIndexSettings(shadowSettings).setWaitForCompletion(true)
                .setRenamePattern("(.+)").setRenameReplacement("$1-copy")
                .execute().actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen();
        refresh();
        Index index = resolveIndex("foo-copy");
        for (IndicesService service : internalCluster().getDataNodeInstances(IndicesService.class)) {

            if (service.hasIndex(index)) {
                IndexShard shard = service.indexServiceSafe(index).getShardOrNull(0);
                if (shard.routingEntry().primary()) {
                    assertFalse(shard instanceof ShadowIndexShard);
                } else {
                    assertTrue(shard instanceof ShadowIndexShard);
                }
            }
        }
        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch("foo-copy").setQuery(matchAllQuery()).get();
        assertHitCount(resp, numDocs);

    }

    @TestLogging("gateway:TRACE")
    public void testIndexWithFewDocuments() throws Exception {
        final Path dataPath = createTempDir();
        Settings nodeSettings = nodeSettings(dataPath);

        internalCluster().startNodesAsync(3, nodeSettings).get();
        final String IDX = "test";

        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB))
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=text").get();
        ensureGreen(IDX);

        // So basically, the primary should fail and the replica will need to
        // replay the translog, this is what this tests
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();

        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(IDX).clear().setTranslog(true).get();
        assertEquals(2, indicesStatsResponse.getIndex(IDX).getPrimaries().getTranslog().estimatedNumberOfOperations());
        assertEquals(2, indicesStatsResponse.getIndex(IDX).getTotal().getTranslog().estimatedNumberOfOperations());
        Index index = resolveIndex(IDX);
        for (IndicesService service : internalCluster().getInstances(IndicesService.class)) {
            IndexService indexService = service.indexService(index);
            if (indexService != null) {
                IndexShard shard = indexService.getShard(0);
                TranslogStats translogStats = shard.translogStats();
                assertTrue(translogStats != null || shard instanceof ShadowIndexShard);
                if (translogStats != null) {
                    assertEquals(2, translogStats.estimatedNumberOfOperations());
                }
            }
        }

        // Check that we can get doc 1 and 2, because we are doing realtime
        // gets and getting from the primary
        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setRealtime(true).setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setRealtime(true).setFields("foo").get();
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        flushAndRefresh(IDX);
        client().prepareIndex(IDX, "doc", "3").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "4").setSource("foo", "bar").get();
        refresh();

        // Check that we can get doc 1 and 2 without realtime
        gResp1 = client().prepareGet(IDX, "doc", "1").setRealtime(false).setFields("foo").get();
        gResp2 = client().prepareGet(IDX, "doc", "2").setRealtime(false).setFields("foo").get();
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        logger.info("--> restarting all nodes");
        if (randomBoolean()) {
            logger.info("--> rolling restart");
            internalCluster().rollingRestart();
        } else {
            logger.info("--> full restart");
            internalCluster().fullRestart();
        }

        client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        ensureGreen(IDX);
        flushAndRefresh(IDX);

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 4);

        logger.info("--> deleting index");
        assertAcked(client().admin().indices().prepareDelete(IDX));
    }

    public void testReplicaToPrimaryPromotion() throws Exception {
        Path dataPath = createTempDir();
        Settings nodeSettings = nodeSettings(dataPath);

        String node1 = internalCluster().startNode(nodeSettings);
        String IDX = "test";

        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=text").get();
        ensureYellow(IDX);
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();

        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        // Node1 has the primary, now node2 has the replica
        String node2 = internalCluster().startNode(nodeSettings);
        ensureGreen(IDX);
        client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
        flushAndRefresh(IDX);

        logger.info("--> stopping node1 [{}]", node1);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1));
        ensureYellow(IDX);

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 2);

        gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.toString(), gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        client().prepareIndex(IDX, "doc", "1").setSource("foo", "foobar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "foobar").get();
        gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.toString(), gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("foobar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("foobar"));
    }

    public void testPrimaryRelocation() throws Exception {
        Path dataPath = createTempDir();
        Settings nodeSettings = nodeSettings(dataPath);

        String node1 = internalCluster().startNode(nodeSettings);
        String IDX = "test";

        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=text").get();
        ensureYellow(IDX);
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();

        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        // Node1 has the primary, now node2 has the replica
        String node2 = internalCluster().startNode(nodeSettings);
        ensureGreen(IDX);
        client().admin().cluster().prepareHealth().setWaitForNodes("2").get();
        flushAndRefresh(IDX);

        // now prevent primary from being allocated on node 1 move to node_3
        String node3 = internalCluster().startNode(nodeSettings);
        Settings build = Settings.builder().put("index.routing.allocation.exclude._name", node1).build();
        client().admin().indices().prepareUpdateSettings(IDX).setSettings(build).execute().actionGet();

        ensureGreen(IDX);
        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 2);

        gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.toString(), gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        client().prepareIndex(IDX, "doc", "3").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "4").setSource("foo", "bar").get();
        gResp1 = client().prepareGet(IDX, "doc", "3").setPreference("_primary").setFields("foo").get();
        gResp2 = client().prepareGet(IDX, "doc", "4").setPreference("_primary").setFields("foo").get();
        assertTrue(gResp1.isExists());
        assertTrue(gResp2.isExists());
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));
    }

    public void testPrimaryRelocationWithConcurrentIndexing() throws Throwable {
        Path dataPath = createTempDir();
        Settings nodeSettings = nodeSettings(dataPath);

        String node1 = internalCluster().startNode(nodeSettings);
        final String IDX = "test";

        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=text").get();
        ensureYellow(IDX);
        // Node1 has the primary, now node2 has the replica
        String node2 = internalCluster().startNode(nodeSettings);
        ensureGreen(IDX);
        flushAndRefresh(IDX);
        String node3 = internalCluster().startNode(nodeSettings);
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch started = new CountDownLatch(1);

        final int numPhase1Docs = scaledRandomIntBetween(25, 200);
        final int numPhase2Docs = scaledRandomIntBetween(25, 200);
        final CountDownLatch phase1finished = new CountDownLatch(1);
        final CountDownLatch phase2finished = new CountDownLatch(1);
        final CopyOnWriteArrayList<Throwable> exceptions = new CopyOnWriteArrayList<>();
        Thread thread = new Thread() {
            @Override
            public void run() {
                started.countDown();
                while (counter.get() < (numPhase1Docs + numPhase2Docs)) {
                    try {
                        final IndexResponse indexResponse = client().prepareIndex(IDX, "doc",
                                Integer.toString(counter.incrementAndGet())).setSource("foo", "bar").get();
                        assertTrue(indexResponse.isCreated());
                    } catch (Throwable t) {
                        exceptions.add(t);
                    }
                    final int docCount = counter.get();
                    if (docCount == numPhase1Docs) {
                        phase1finished.countDown();
                    }
                }
                logger.info("--> stopping indexing thread");
                phase2finished.countDown();
            }
        };
        thread.start();
        started.await();
        phase1finished.await(); // wait for a certain number of documents to be indexed
        logger.info("--> excluding {} from allocation", node1);
        // now prevent primary from being allocated on node 1 move to node_3
        Settings build = Settings.builder().put("index.routing.allocation.exclude._name", node1).build();
        client().admin().indices().prepareUpdateSettings(IDX).setSettings(build).execute().actionGet();
        // wait for more documents to be indexed post-recovery, also waits for
        // indexing thread to stop
        phase2finished.await();
        ExceptionsHelper.rethrowAndSuppress(exceptions);
        ensureGreen(IDX);
        thread.join();
        logger.info("--> performing query");
        flushAndRefresh();

        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, counter.get());
        assertHitCount(resp, numPhase1Docs + numPhase2Docs);
    }

    public void testPrimaryRelocationWhereRecoveryFails() throws Exception {
        Path dataPath = createTempDir();
        Settings nodeSettings = Settings.builder()
                .put("node.add_id_to_custom_path", false)
                .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), dataPath)
                .build();

        String node1 = internalCluster().startNode(nodeSettings);
        final String IDX = "test";

        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=text").get();
        ensureYellow(IDX);
        // Node1 has the primary, now node2 has the replica
        String node2 = internalCluster().startNode(nodeSettings);
        ensureGreen(IDX);
        flushAndRefresh(IDX);
        String node3 = internalCluster().startNode(nodeSettings);
        final AtomicInteger counter = new AtomicInteger(0);
        final CountDownLatch started = new CountDownLatch(1);

        final int numPhase1Docs = scaledRandomIntBetween(25, 200);
        final int numPhase2Docs = scaledRandomIntBetween(25, 200);
        final int numPhase3Docs = scaledRandomIntBetween(25, 200);
        final CountDownLatch phase1finished = new CountDownLatch(1);
        final CountDownLatch phase2finished = new CountDownLatch(1);
        final CountDownLatch phase3finished = new CountDownLatch(1);

        final AtomicBoolean keepFailing = new AtomicBoolean(true);

        MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(TransportService.class, node1));
        mockTransportService.addDelegate(internalCluster().getInstance(TransportService.class, node3),
                new MockTransportService.DelegateTransport(mockTransportService.original()) {

                    @Override
                    public void sendRequest(DiscoveryNode node, long requestId, String action,
                                            TransportRequest request, TransportRequestOptions options)
                            throws IOException, TransportException {
                        if (keepFailing.get() && action.equals(RecoveryTargetService.Actions.TRANSLOG_OPS)) {
                            logger.info("--> failing translog ops");
                            throw new ElasticsearchException("failing on purpose");
                        }
                        super.sendRequest(node, requestId, action, request, options);
                    }
                });

        Thread thread = new Thread() {
            @Override
            public void run() {
                started.countDown();
                while (counter.get() < (numPhase1Docs + numPhase2Docs + numPhase3Docs)) {
                    final IndexResponse indexResponse = client().prepareIndex(IDX, "doc",
                            Integer.toString(counter.incrementAndGet())).setSource("foo", "bar").get();
                    assertTrue(indexResponse.isCreated());
                    final int docCount = counter.get();
                    if (docCount == numPhase1Docs) {
                        phase1finished.countDown();
                    } else if (docCount == (numPhase1Docs + numPhase2Docs)) {
                        phase2finished.countDown();
                    }
                }
                logger.info("--> stopping indexing thread");
                phase3finished.countDown();
            }
        };
        thread.start();
        started.await();
        phase1finished.await(); // wait for a certain number of documents to be indexed
        logger.info("--> excluding {} from allocation", node1);
        // now prevent primary from being allocated on node 1 move to node_3
        Settings build = Settings.builder().put("index.routing.allocation.exclude._name", node1).build();
        client().admin().indices().prepareUpdateSettings(IDX).setSettings(build).execute().actionGet();
        // wait for more documents to be indexed post-recovery, also waits for
        // indexing thread to stop
        phase2finished.await();
        // stop failing
        keepFailing.set(false);
        // wait for more docs to be indexed
        phase3finished.await();
        ensureGreen(IDX);
        thread.join();
        logger.info("--> performing query");
        flushAndRefresh();

        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, counter.get());
    }

    public void testIndexWithShadowReplicasCleansUp() throws Exception {
        Path dataPath = createTempDir();
        Settings nodeSettings = nodeSettings(dataPath);

        final int nodeCount = randomIntBetween(2, 5);
        logger.info("--> starting {} nodes", nodeCount);
        final List<String> nodes = internalCluster().startNodesAsync(nodeCount, nodeSettings).get();
        final String IDX = "test";
        final Tuple<Integer, Integer> numPrimariesAndReplicas = randomPrimariesAndReplicas(nodeCount);
        final int numPrimaries = numPrimariesAndReplicas.v1();
        final int numReplicas = numPrimariesAndReplicas.v2();
        logger.info("--> creating index {} with {} primary shards and {} replicas", IDX, numPrimaries, numReplicas);

        Settings idxSettings = Settings.builder()
                                   .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numPrimaries)
                                   .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                                   .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                                   .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                                   .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                                   .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=text").get();
        ensureGreen(IDX);

        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();
        flushAndRefresh(IDX);

        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 2);

        logger.info("--> deleting index " + IDX);
        assertAcked(client().admin().indices().prepareDelete(IDX));
        assertAllIndicesRemovedAndDeletionCompleted(internalCluster().getInstances(IndicesService.class));
        assertPathHasBeenCleared(dataPath);
        //norelease
        //TODO: uncomment the test below when https://github.com/elastic/elasticsearch/issues/17695 is resolved.
        //assertIndicesDirsDeleted(nodes);
    }

    /**
     * Tests that shadow replicas can be "naturally" rebalanced and relocated
     * around the cluster. By "naturally" I mean without using the reroute API
     */
    public void testShadowReplicaNaturalRelocation() throws Exception {
        Path dataPath = createTempDir();
        Settings nodeSettings = nodeSettings(dataPath);

        final List<String> nodes = internalCluster().startNodesAsync(2, nodeSettings).get();
        String IDX = "test";

        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=text").get();
        ensureGreen(IDX);

        int docCount = randomIntBetween(10, 100);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            builders.add(client().prepareIndex(IDX, "doc", i + "").setSource("foo", "bar"));
        }
        indexRandom(true, true, true, builders);
        flushAndRefresh(IDX);

        // start a third node, with 5 shards each on the other nodes, they
        // should relocate some to the third node
        final String node3 = internalCluster().startNode(nodeSettings);
        nodes.add(node3);

        assertBusy(new Runnable() {
            @Override
            public void run() {
                client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
                ClusterStateResponse resp = client().admin().cluster().prepareState().get();
                RoutingNodes nodes = resp.getState().getRoutingNodes();
                for (RoutingNode node : nodes) {
                    logger.info("--> node has {} shards (needs at least 2)", node.numberOfOwningShards());
                    assertThat("at least 2 shards on node", node.numberOfOwningShards(), greaterThanOrEqualTo(2));
                }
            }
        });
        ensureYellow(IDX);

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, docCount);

        assertAcked(client().admin().indices().prepareDelete(IDX));
        assertAllIndicesRemovedAndDeletionCompleted(internalCluster().getInstances(IndicesService.class));
        assertPathHasBeenCleared(dataPath);
        //norelease
        //TODO: uncomment the test below when https://github.com/elastic/elasticsearch/issues/17695 is resolved.
        //assertIndicesDirsDeleted(nodes);
    }

    public void testShadowReplicasUsingFieldData() throws Exception {
        Path dataPath = createTempDir();
        Settings nodeSettings = nodeSettings(dataPath);

        internalCluster().startNodesAsync(3, nodeSettings).get();
        String IDX = "test";

        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=keyword").get();
        ensureGreen(IDX);

        client().prepareIndex(IDX, "doc", "1").setSource("foo", "foo").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "3").setSource("foo", "baz").get();
        client().prepareIndex(IDX, "doc", "4").setSource("foo", "eggplant").get();
        flushAndRefresh(IDX);

        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).addFieldDataField("foo").addSort("foo", SortOrder.ASC).get();
        assertHitCount(resp, 4);
        assertOrderedSearchHits(resp, "2", "3", "4", "1");
        SearchHit[] hits = resp.getHits().hits();
        assertThat(hits[0].field("foo").getValue().toString(), equalTo("bar"));
        assertThat(hits[1].field("foo").getValue().toString(), equalTo("baz"));
        assertThat(hits[2].field("foo").getValue().toString(), equalTo("eggplant"));
        assertThat(hits[3].field("foo").getValue().toString(), equalTo("foo"));
    }

    /** wait until none of the nodes have shards allocated on them */
    private void assertNoShardsOn(final List<String> nodeList) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterStateResponse resp = client().admin().cluster().prepareState().get();
                RoutingNodes nodes = resp.getState().getRoutingNodes();
                for (RoutingNode node : nodes) {
                    logger.info("--> node {} has {} shards", node.node().getName(), node.numberOfOwningShards());
                    if (nodeList.contains(node.node().getName())) {
                        assertThat("no shards on node", node.numberOfOwningShards(), equalTo(0));
                    }
                }
            }
        });
    }

    /** wait until the node has the specified number of shards allocated on it */
    private void assertShardCountOn(final String nodeName, final int shardCount) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterStateResponse resp = client().admin().cluster().prepareState().get();
                RoutingNodes nodes = resp.getState().getRoutingNodes();
                for (RoutingNode node : nodes) {
                    logger.info("--> node {} has {} shards", node.node().getName(), node.numberOfOwningShards());
                    if (nodeName.equals(node.node().getName())) {
                        assertThat(node.numberOfOwningShards(), equalTo(shardCount));
                    }
                }
            }
        });
    }

    public void testIndexOnSharedFSRecoversToAnyNode() throws Exception {
        Path dataPath = createTempDir();
        Settings nodeSettings = nodeSettings(dataPath);
        Settings fooSettings = Settings.builder().put(nodeSettings).put("node.attr.affinity", "foo").build();
        Settings barSettings = Settings.builder().put(nodeSettings).put("node.attr.affinity", "bar").build();

        final InternalTestCluster.Async<List<String>> fooNodes = internalCluster().startNodesAsync(2, fooSettings);
        final InternalTestCluster.Async<List<String>> barNodes = internalCluster().startNodesAsync(2, barSettings);
        fooNodes.get();
        barNodes.get();
        String IDX = "test";

        Settings includeFoo = Settings.builder()
                .put("index.routing.allocation.include.affinity", "foo")
                .build();
        Settings includeBar = Settings.builder()
                .put("index.routing.allocation.include.affinity", "bar")
                .build();

        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .put(IndexMetaData.SETTING_SHARED_FS_ALLOW_RECOVERY_ON_ANY_NODE, true)
                .put(includeFoo) // start with requiring the shards on "foo"
                .build();

        // only one node, so all primaries will end up on node1
        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=keyword").get();
        ensureGreen(IDX);

        // Index some documents
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "foo").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "3").setSource("foo", "baz").get();
        client().prepareIndex(IDX, "doc", "4").setSource("foo", "eggplant").get();
        flushAndRefresh(IDX);

        // put shards on "bar"
        client().admin().indices().prepareUpdateSettings(IDX).setSettings(includeBar).get();

        // wait for the shards to move from "foo" nodes to "bar" nodes
        assertNoShardsOn(fooNodes.get());

        // put shards back on "foo"
        client().admin().indices().prepareUpdateSettings(IDX).setSettings(includeFoo).get();

        // wait for the shards to move from "bar" nodes to "foo" nodes
        assertNoShardsOn(barNodes.get());

        // Stop a foo node
        logger.info("--> stopping first 'foo' node");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(fooNodes.get().get(0)));

        // Ensure that the other foo node has all the shards now
        assertShardCountOn(fooNodes.get().get(1), 5);

        // Assert no shards on the "bar" nodes
        assertNoShardsOn(barNodes.get());

        // Stop the second "foo" node
        logger.info("--> stopping second 'foo' node");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(fooNodes.get().get(1)));

        // The index should still be able to be allocated (on the "bar" nodes),
        // all the "foo" nodes are gone
        ensureGreen(IDX);

        // Start another "foo" node and make sure the index moves back
        logger.info("--> starting additional 'foo' node");
        String newFooNode = internalCluster().startNode(fooSettings);

        assertShardCountOn(newFooNode, 5);
        assertNoShardsOn(barNodes.get());
    }

    public void testDeletingClosedIndexRemovesFiles() throws Exception {
        Path dataPath = createTempDir();
        Settings nodeSettings = nodeSettings(dataPath.getParent());

        final int numNodes = randomIntBetween(2, 5);
        logger.info("--> starting {} nodes", numNodes);
        final List<String> nodes = internalCluster().startNodesAsync(numNodes, nodeSettings).get();
        final String IDX = "test";
        final Tuple<Integer, Integer> numPrimariesAndReplicas = randomPrimariesAndReplicas(numNodes);
        final int numPrimaries = numPrimariesAndReplicas.v1();
        final int numReplicas = numPrimariesAndReplicas.v2();
        logger.info("--> creating index {} with {} primary shards and {} replicas", IDX, numPrimaries, numReplicas);

        assert numPrimaries > 0;
        assert numReplicas >= 0;
        Settings idxSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numPrimaries)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=text").get();
        ensureGreen(IDX);

        int docCount = randomIntBetween(10, 100);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            builders.add(client().prepareIndex(IDX, "doc", i + "").setSource("foo", "bar"));
        }
        indexRandom(true, true, true, builders);
        flushAndRefresh(IDX);

        logger.info("--> closing index {}", IDX);
        client().admin().indices().prepareClose(IDX).get();
        ensureGreen(IDX);

        logger.info("--> deleting closed index");
        client().admin().indices().prepareDelete(IDX).get();
        assertAllIndicesRemovedAndDeletionCompleted(internalCluster().getInstances(IndicesService.class));
        assertPathHasBeenCleared(dataPath);
        assertIndicesDirsDeleted(nodes);
    }

    public void testNodeJoinsWithoutShadowReplicaConfigured() throws Exception {
        Path dataPath = createTempDir();
        Settings nodeSettings = nodeSettings(dataPath);

        internalCluster().startNodesAsync(2, nodeSettings).get();
        String IDX = "test";

        Settings idxSettings = Settings.builder()
                                       .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                       .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
                                       .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                                       .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                                       .put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true)
                                       .build();

        prepareCreate(IDX).setSettings(idxSettings).addMapping("doc", "foo", "type=text").get();
        ensureYellow(IDX);

        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();
        flushAndRefresh(IDX);

        internalCluster().startNodesAsync(1).get();
        ensureYellow(IDX);

        final ClusterHealthResponse clusterHealth = client().admin().cluster()
                                                                    .prepareHealth()
                                                                    .setWaitForEvents(Priority.LANGUID)
                                                                    .execute()
                                                                    .actionGet();
        assertThat(clusterHealth.getNumberOfNodes(), equalTo(3));
        // the new node is not configured for a shadow replica index, so no shards should have been assigned to it
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));
    }

    private static void assertIndicesDirsDeleted(final List<String> nodes) throws IOException {
        for (String node : nodes) {
            final NodeEnvironment nodeEnv = internalCluster().getInstance(NodeEnvironment.class, node);
            assertThat(nodeEnv.availableIndexFolders(), equalTo(Collections.emptySet()));
        }
    }

    private static Tuple<Integer, Integer> randomPrimariesAndReplicas(final int numNodes) {
        final int numPrimaries;
        final int numReplicas;
        if (randomBoolean()) {
            // test with some nodes having no shards
            numPrimaries = 1;
            numReplicas = randomIntBetween(0, numNodes - 2);
        } else {
            // test with all nodes having at least one shard
            numPrimaries = randomIntBetween(1, 5);
            numReplicas = numNodes - 1;
        }
        return Tuple.tuple(numPrimaries, numReplicas);
    }

}
