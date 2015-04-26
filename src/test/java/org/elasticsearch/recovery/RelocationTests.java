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

package org.elasticsearch.recovery;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.procedures.IntProcedure;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.recovery.RecoveryFileChunkRequest;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
@TestLogging("indices.recovery:TRACE,index.shard.service:TRACE")
public class RelocationTests extends ElasticsearchIntegrationTest {
    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(5, TimeUnit.MINUTES);

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName()).build();
    }


    @Test
    public void testSimpleRelocationNoIndexing() {
        logger.info("--> starting [node1] ...");
        final String node_1 = internalCluster().startNode();

        logger.info("--> creating test index ...");
        client().admin().indices().prepareCreate("test")
                .setSettings(ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                )
                .execute().actionGet();

        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }
        logger.info("--> flush so we have an actual index");
        client().admin().indices().prepareFlush().execute().actionGet();
        logger.info("--> index more docs so we have something in the translog");
        for (int i = 10; i < 20; i++) {
            client().prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value" + i).execute().actionGet();
        }

        logger.info("--> verifying count");
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client().prepareCount("test").execute().actionGet().getCount(), equalTo(20l));

        logger.info("--> start another node");
        final String node_2 = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("2").execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> relocate the shard from node1 to node2");
        client().admin().cluster().prepareReroute()
                .add(new MoveAllocationCommand(new ShardId("test", 0), node_1, node_2))
                .execute().actionGet();

        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForRelocatingShards(0).setTimeout(ACCEPTABLE_RELOCATION_TIME).execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
        clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForRelocatingShards(0).setTimeout(ACCEPTABLE_RELOCATION_TIME).execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> verifying count again...");
        client().admin().indices().prepareRefresh().execute().actionGet();
        assertThat(client().prepareCount("test").execute().actionGet().getCount(), equalTo(20l));
    }

    @Test
    @Slow
    public void testRelocationWhileIndexingRandom() throws Exception {
        int numberOfRelocations = scaledRandomIntBetween(1, rarely() ? 10 : 4);
        int numberOfReplicas = randomBoolean() ? 0 : 1;
        int numberOfNodes = numberOfReplicas == 0 ? 2 : 3;

        logger.info("testRelocationWhileIndexingRandom(numRelocations={}, numberOfReplicas={}, numberOfNodes={})", numberOfRelocations, numberOfReplicas, numberOfNodes);

        String[] nodes = new String[numberOfNodes];
        logger.info("--> starting [node1] ...");
        nodes[0] = internalCluster().startNode();

        logger.info("--> creating test index ...");
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", numberOfReplicas)
                ).execute().actionGet();


        for (int i = 1; i < numberOfNodes; i++) {
            logger.info("--> starting [node{}] ...", i + 1);
            nodes[i] = internalCluster().startNode();
            if (i != numberOfNodes - 1) {
                ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID)
                        .setWaitForNodes(Integer.toString(i + 1)).setWaitForGreenStatus().execute().actionGet();
                assertThat(healthResponse.isTimedOut(), equalTo(false));
            }
        }

        int numDocs = scaledRandomIntBetween(200, 2500);
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type1", client(), numDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", numDocs);
            waitForDocs(numDocs, indexer);
            logger.info("--> {} docs indexed", numDocs);

            logger.info("--> starting relocations...");
            int nodeShiftBased = numberOfReplicas; // if we have replicas shift those
            for (int i = 0; i < numberOfRelocations; i++) {
                int fromNode = (i % 2);
                int toNode = fromNode == 0 ? 1 : 0;
                fromNode += nodeShiftBased;
                toNode += nodeShiftBased;
                numDocs = scaledRandomIntBetween(200, 1000);
                logger.debug("--> Allow indexer to index [{}] documents", numDocs);
                indexer.continueIndexing(numDocs);
                logger.info("--> START relocate the shard from {} to {}", nodes[fromNode], nodes[toNode]);
                client().admin().cluster().prepareReroute()
                        .add(new MoveAllocationCommand(new ShardId("test", 0), nodes[fromNode], nodes[toNode]))
                        .get();
                if (rarely()) {
                    logger.debug("--> flushing");
                    client().admin().indices().prepareFlush().get();
                }
                ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForRelocatingShards(0).setTimeout(ACCEPTABLE_RELOCATION_TIME).execute().actionGet();
                assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
                clusterHealthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForRelocatingShards(0).setTimeout(ACCEPTABLE_RELOCATION_TIME).execute().actionGet();
                assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
                indexer.pauseIndexing();
                logger.info("--> DONE relocate the shard from {} to {}", fromNode, toNode);
            }
            logger.info("--> done relocations");
            logger.info("--> waiting for indexing threads to stop ...");
            indexer.stop();
            logger.info("--> indexing threads stopped");

            logger.info("--> refreshing the index");
            client().admin().indices().prepareRefresh("test").execute().actionGet();
            logger.info("--> searching the index");
            boolean ranOnce = false;
            for (int i = 0; i < 10; i++) {
                try {
                    logger.info("--> START search test round {}", i + 1);
                    SearchHits hits = client().prepareSearch("test").setQuery(matchAllQuery()).setSize((int) indexer.totalIndexedDocs()).setNoFields().execute().actionGet().getHits();
                    ranOnce = true;
                    if (hits.totalHits() != indexer.totalIndexedDocs()) {
                        int[] hitIds = new int[(int) indexer.totalIndexedDocs()];
                        for (int hit = 0; hit < indexer.totalIndexedDocs(); hit++) {
                            hitIds[hit] = hit + 1;
                        }
                        IntOpenHashSet set = IntOpenHashSet.from(hitIds);
                        for (SearchHit hit : hits.hits()) {
                            int id = Integer.parseInt(hit.id());
                            if (!set.remove(id)) {
                                logger.error("Extra id [{}]", id);
                            }
                        }
                        set.forEach(new IntProcedure() {

                            @Override
                            public void apply(int value) {
                                logger.error("Missing id [{}]", value);
                            }

                        });
                    }
                    assertThat(hits.totalHits(), equalTo(indexer.totalIndexedDocs()));
                    logger.info("--> DONE search test round {}", i + 1);
                } catch (SearchPhaseExecutionException ex) {
                    // TODO: the first run fails with this failure, waiting for relocating nodes set to 0 is not enough?
                    logger.warn("Got exception while searching.", ex);
                }
            }
            if (!ranOnce) {
                fail();
            }
        }
    }

    @Test
    @Slow
    public void testRelocationWhileRefreshing() throws Exception {
        int numberOfRelocations = scaledRandomIntBetween(1, rarely() ? 10 : 4);
        int numberOfReplicas = randomBoolean() ? 0 : 1;
        int numberOfNodes = numberOfReplicas == 0 ? 2 : 3;

        logger.info("testRelocationWhileIndexingRandom(numRelocations={}, numberOfReplicas={}, numberOfNodes={})", numberOfRelocations, numberOfReplicas, numberOfNodes);

        String[] nodes = new String[numberOfNodes];
        logger.info("--> starting [node_0] ...");
        nodes[0] = internalCluster().startNode();

        logger.info("--> creating test index ...");
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", numberOfReplicas)
                        .put("index.refresh_interval", -1) // we want to control refreshes c
                ).execute().actionGet();

        // make sure the first shard is started.
        ensureYellow();

        for (int i = 1; i < numberOfNodes; i++) {
            logger.info("--> starting [node_{}] ...", i);
            nodes[i] = internalCluster().startNode();
            if (i != numberOfNodes - 1) {
                ClusterHealthResponse healthResponse = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID)
                        .setWaitForNodes(Integer.toString(i + 1)).setWaitForGreenStatus().execute().actionGet();
                assertThat(healthResponse.isTimedOut(), equalTo(false));
            }
        }

        final Semaphore postRecoveryShards = new Semaphore(0);

        for (IndicesLifecycle indicesLifecycle : internalCluster().getInstances(IndicesLifecycle.class)) {
            indicesLifecycle.addListener(new IndicesLifecycle.Listener() {
                @Override
                public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState, IndexShardState currentState, @Nullable String reason) {
                    if (currentState == IndexShardState.POST_RECOVERY) {
                        postRecoveryShards.release();
                    }
                }
            });
        }


        logger.info("--> starting relocations...");
        int nodeShiftBased = numberOfReplicas; // if we have replicas shift those
        for (int i = 0; i < numberOfRelocations; i++) {
            int fromNode = (i % 2);
            int toNode = fromNode == 0 ? 1 : 0;
            fromNode += nodeShiftBased;
            toNode += nodeShiftBased;

            List<IndexRequestBuilder> builders1 = new ArrayList<>();
            for (int numDocs = randomIntBetween(10, 30); numDocs > 0; numDocs--) {
                builders1.add(client().prepareIndex("test", "type").setSource("{}"));
            }

            List<IndexRequestBuilder> builders2 = new ArrayList<>();
            for (int numDocs = randomIntBetween(10, 30); numDocs > 0; numDocs--) {
                builders2.add(client().prepareIndex("test", "type").setSource("{}"));
            }

            logger.info("--> START relocate the shard from {} to {}", nodes[fromNode], nodes[toNode]);


            client().admin().cluster().prepareReroute()
                    .add(new MoveAllocationCommand(new ShardId("test", 0), nodes[fromNode], nodes[toNode]))
                    .get();


            logger.debug("--> index [{}] documents", builders1.size());
            indexRandom(false, true, builders1);
            // wait for shard to reach post recovery
            postRecoveryShards.acquire(1);

            logger.debug("--> index [{}] documents", builders2.size());
            indexRandom(true, true, builders2);

            // verify cluster was finished.
            assertFalse(client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).setWaitForEvents(Priority.LANGUID).setTimeout("30s").get().isTimedOut());
            logger.info("--> DONE relocate the shard from {} to {}", fromNode, toNode);

            logger.debug("--> verifying all searches return the same number of docs");
            long expectedCount = -1;
            for (Client client : clients()) {
                SearchResponse response = client.prepareSearch("test").setPreference("_local").setSize(0).get();
                assertNoFailures(response);
                if (expectedCount < 0) {
                    expectedCount = response.getHits().totalHits();
                } else {
                    assertEquals(expectedCount, response.getHits().totalHits());
                }
            }

        }
    }

    @Test
    public void testMoveShardsWhileRelocation() throws Exception {
        final String indexName = "test";

        ListenableFuture<String> blueFuture = internalCluster().startNodeAsync(ImmutableSettings.builder().put("node.color", "blue").build());
        ListenableFuture<String> redFuture = internalCluster().startNodeAsync(ImmutableSettings.builder().put("node.color", "red").build());
        internalCluster().startNode(ImmutableSettings.builder().put("node.color", "green").build());
        final String blueNodeName = blueFuture.get();
        final String redNodeName = redFuture.get();

        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes(">=3").get();
        assertThat(response.isTimedOut(), is(false));


        client().admin().indices().prepareCreate(indexName)
                .setSettings(
                        ImmutableSettings.builder()
                                .put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "blue")
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                ).get();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numDocs; i++) {
            requests.add(client().prepareIndex(indexName, "type").setCreate(true).setSource("{}"));
        }
        indexRandom(true, requests);
        ensureSearchable(indexName);

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        String blueNodeId = internalCluster().getInstance(DiscoveryService.class, blueNodeName).localNode().id();

        assertFalse(stateResponse.getState().readOnlyRoutingNodes().node(blueNodeId).isEmpty());

        SearchResponse searchResponse = client().prepareSearch(indexName).get();
        assertHitCount(searchResponse, numDocs);

        // Slow down recovery in order to make recovery cancellations more likely
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).get();
        long chunkSize = statsResponse.getIndex(indexName).getShards()[0].getStats().getStore().size().bytes() / 10;
        assertTrue(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(ImmutableSettings.builder()
                                // one chunk per sec..
                                .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC, chunkSize)
                                .put(RecoverySettings.INDICES_RECOVERY_FILE_CHUNK_SIZE, chunkSize)
                )
                .get().isAcknowledged());

        client().admin().indices().prepareUpdateSettings(indexName).setSettings(
                ImmutableSettings.builder().put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red")
        ).get();

        // Lets wait a bit and then move again to hopefully trigger recovery cancellations.
        boolean applied = awaitBusy(
                new Predicate<Object>() {
                    @Override
                    public boolean apply(Object input) {
                        RecoveryResponse recoveryResponse = internalCluster().client(redNodeName).admin().indices().prepareRecoveries(indexName)
                                .get();
                        return !recoveryResponse.shardResponses().get(indexName).isEmpty();
                    }
                }
        );
        assertTrue(applied);
        client().admin().indices().prepareUpdateSettings(indexName).setSettings(
                ImmutableSettings.builder().put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "green")
        ).get();

        // Restore the recovery speed to not timeout cluster health call
        assertTrue(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(ImmutableSettings.builder()
                                .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC, "20mb")
                                .put(RecoverySettings.INDICES_RECOVERY_FILE_CHUNK_SIZE, "512kb")
                )
                .get().isAcknowledged());

        // this also waits for all ongoing recoveries to complete:
        ensureSearchable(indexName);
        searchResponse = client().prepareSearch(indexName).get();
        assertHitCount(searchResponse, numDocs);

        stateResponse = client().admin().cluster().prepareState().get();
        assertTrue(stateResponse.getState().readOnlyRoutingNodes().node(blueNodeId).isEmpty());
    }

    @Test
    @Slow
    @TestLogging("cluster.service:TRACE,indices.recovery:TRACE")
    public void testRelocationWithBusyClusterUpdateThread() throws Exception {
        final String indexName = "test";
        final Settings settings = ImmutableSettings.builder()
                .put("gateway.type", "local")
                .put(DiscoverySettings.PUBLISH_TIMEOUT, "1s")
                .put("indices.recovery.internal_action_timeout", "1s").build();
        String master = internalCluster().startNode(settings);
        ensureGreen();
        List<String> nodes = internalCluster().startNodesAsync(2, settings).get();
        final String node1 = nodes.get(0);
        final String node2 = nodes.get(1);
        ClusterHealthResponse response = client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        assertThat(response.isTimedOut(), is(false));


        client().admin().indices().prepareCreate(indexName)
                .setSettings(
                        ImmutableSettings.builder()
                                .put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "_name", node1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                ).get();


        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numDocs; i++) {
            requests.add(client().prepareIndex(indexName, "type").setCreate(true).setSource("{}"));
        }
        indexRandom(true, requests);
        ensureSearchable(indexName);

        // capture the incoming state indicate that the replicas have upgraded and assigned

        final CountDownLatch allReplicasAssigned = new CountDownLatch(1);
        final CountDownLatch releaseClusterState = new CountDownLatch(1);
        final CountDownLatch unassignedShardsAfterReplicasAssigned = new CountDownLatch(1);
        try {
            internalCluster().getInstance(ClusterService.class, node1).addLast(new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    ClusterState state = event.state();
                    if (state.routingTable().allShards().size() == 1 || state.routingNodes().hasUnassignedShards()) {
                        // we have no replicas or they are not assigned yet
                        return;
                    }

                    allReplicasAssigned.countDown();
                    try {
                        releaseClusterState.await();
                    } catch (InterruptedException e) {
                        //
                    }
                }

            });

            internalCluster().getInstance(ClusterService.class, master).addLast(new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    if (event.state().routingNodes().hasUnassigned() && allReplicasAssigned.getCount() == 0) {
                        unassignedShardsAfterReplicasAssigned.countDown();
                    }
                }
            });

            logger.info("--> starting replica recovery");
            // we don't expect this to be acknowledge by node1 where we block the cluster state thread
            assertFalse(client().admin().indices().prepareUpdateSettings(indexName)
                    .setSettings(ImmutableSettings.builder()
                                    .put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "_name", node1 + "," + node2)
                                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)

                    ).setTimeout("200ms")
                    .get().isAcknowledged());

            logger.info("--> waiting for node1 to process replica existence");
            allReplicasAssigned.await();
            logger.info("--> waiting for recovery to fail");
            unassignedShardsAfterReplicasAssigned.await();
        } finally {
            logger.info("--> releasing cluster state update thread");
            releaseClusterState.countDown();
        }
        logger.info("--> waiting for recovery to succeed");
        // force a move.
        client().admin().cluster().prepareReroute().get();
        ensureGreen();
    }

    @Test
    @Slow
    public void testCancellationCleansTempFiles() throws Exception {
        final String indexName = "test";

        final String p_node = internalCluster().startNode();

        client().admin().indices().prepareCreate(indexName)
                .setSettings(ImmutableSettings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)).get();

        internalCluster().startNodesAsync(2).get();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numDocs; i++) {
            requests.add(client().prepareIndex(indexName, "type").setCreate(true).setSource("{}"));
        }
        indexRandom(true, requests);
        assertFalse(client().admin().cluster().prepareHealth().setWaitForNodes("3").setWaitForGreenStatus().get().isTimedOut());
        flush();

        int allowedFailures = randomIntBetween(3, 10);
        logger.info("--> blocking recoveries from primary (allowed failures: [{}])", allowedFailures);
        CountDownLatch corruptionCount = new CountDownLatch(allowedFailures);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, p_node);
        MockTransportService mockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, p_node);
        for (DiscoveryNode node : clusterService.state().nodes()) {
            if (!node.equals(clusterService.localNode())) {
                mockTransportService.addDelegate(node, new RecoveryCorruption(mockTransportService.original(), corruptionCount));
            }
        }

        client().admin().indices().prepareUpdateSettings(indexName).setSettings(ImmutableSettings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)).get();

        corruptionCount.await();

        logger.info("--> stopping replica assignment");
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(ImmutableSettings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE, "none")));

        logger.info("--> wait for all replica shards to be removed, on all nodes");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                for (String node : internalCluster().getNodeNames()) {
                    if (node.equals(p_node)) {
                        continue;
                    }
                    ClusterState state = client(node).admin().cluster().prepareState().setLocal(true).get().getState();
                    assertThat(node + " indicates assigned replicas",
                            state.getRoutingTable().index(indexName).shardsWithState(ShardRoutingState.UNASSIGNED).size(), equalTo(1));
                }
            }
        });

        logger.info("--> verifying no temporary recoveries are left");
        for (String node : internalCluster().getNodeNames()) {
            NodeEnvironment nodeEnvironment = internalCluster().getInstance(NodeEnvironment.class, node);
            for (final Path shardLoc : nodeEnvironment.availableShardPaths(new ShardId(indexName, 0))) {
                if (Files.exists(shardLoc)) {
                    assertBusy(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Files.walkFileTree(shardLoc, new SimpleFileVisitor<Path>() {
                                    @Override
                                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                        assertThat("found a temporary recovery file: " + file, file.getFileName().toString(), not(startsWith("recovery.")));
                                        return FileVisitResult.CONTINUE;
                                    }
                                });
                            } catch (IOException e) {
                                throw new AssertionError("failed to walk file tree starting at [" + shardLoc + "]", e);
                            }
                        }
                    });
                }
            }
        }
    }

    class RecoveryCorruption extends MockTransportService.DelegateTransport {

        private final CountDownLatch corruptionCount;

        public RecoveryCorruption(Transport transport, CountDownLatch corruptionCount) {
            super(transport);
            this.corruptionCount = corruptionCount;
        }

        @Override
        public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
            if (action.equals(RecoveryTarget.Actions.FILE_CHUNK)) {
                RecoveryFileChunkRequest chunkRequest = (RecoveryFileChunkRequest) request;
                if (chunkRequest.name().startsWith(IndexFileNames.SEGMENTS)) {
                    // corrupting the segments_N files in order to make sure future recovery re-send files
                    logger.debug("corrupting [{}] to {}. file name: [{}]", action, node, chunkRequest.name());
                    byte[] array = chunkRequest.content().array();
                    array[0] = (byte) ~array[0]; // flip one byte in the content
                    corruptionCount.countDown();
                }
                transport.sendRequest(node, requestId, action, request, options);
            } else {
                transport.sendRequest(node, requestId, action, request, options);
            }
        }
    }
}
