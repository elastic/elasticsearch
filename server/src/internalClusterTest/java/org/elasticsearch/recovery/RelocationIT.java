/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.recovery;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.tests.util.English;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFileChunkRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockIndexEventListener;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.forEachFileRecursively;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class RelocationIT extends ESIntegTestCase {
    private final TimeValue ACCEPTABLE_RELOCATION_TIME = new TimeValue(5, TimeUnit.MINUTES);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, MockTransportService.TestPlugin.class, MockIndexEventListener.TestPlugin.class);
    }

    @Override
    protected void beforeIndexDeletion() throws Exception {
        super.beforeIndexDeletion();
        assertActiveCopiesEstablishedPeerRecoveryRetentionLeases();
        internalCluster().assertSeqNos();
        internalCluster().assertSameDocIdsOnShards();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            // sync global checkpoint quickly so we can verify seq_no_stats aligned between all copies after tests.
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
            .build();
    }

    public void testSimpleRelocationNoIndexing() {
        logger.info("--> starting [node1] ...");
        final String node_1 = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate("test", indexSettings(1, 0)).get();

        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        logger.info("--> flush so we have an actual index");
        indicesAdmin().prepareFlush().get();
        logger.info("--> index more docs so we have something in the translog");
        for (int i = 10; i < 20; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }

        logger.info("--> verifying count");
        indicesAdmin().prepareRefresh().get();
        assertHitCount(prepareSearch("test").setSize(0), 20L);

        logger.info("--> start another node");
        final String node_2 = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> relocate the shard from node1 to node2");
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand("test", 0, node_1, node_2));

        clusterHealthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> verifying count again...");
        indicesAdmin().prepareRefresh().get();
        assertHitCount(prepareSearch("test").setSize(0), 20);
    }

    // This tests that relocation can successfully suspend index throttling to grab
    // indexing permits required for relocation to succeed.
    public void testSimpleRelocationWithIndexingPaused() throws Exception {
        logger.info("--> starting [node1] ...");
        // Start node with PAUSE_INDEXING_ON_THROTTLE setting set to true. This means that if we activate
        // index throttling for a shard on this node, it will pause indexing for that shard until throttling
        // is deactivated.
        final String node_1 = internalCluster().startNode(
            Settings.builder().put(IndexingMemoryController.PAUSE_INDEXING_ON_THROTTLE.getKey(), true)
        );

        logger.info("--> creating test index ...");
        prepareCreate("test", indexSettings(1, 0)).get();

        logger.info("--> index docs");
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        logger.info("--> flush so we have an actual index");
        indicesAdmin().prepareFlush().get();

        logger.info("--> verifying count");
        indicesAdmin().prepareRefresh().get();
        assertHitCount(prepareSearch("test").setSize(0), numDocs);

        logger.info("--> start another node");
        final String node_2 = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        // Activate index throttling on "test" index primary shard
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node_1);
        IndexShard shard = indicesService.indexServiceSafe(resolveIndex("test")).getShard(0);
        shard.activateThrottling();
        // Verify that indexing is paused for the throttled shard
        Engine engine = shard.getEngineOrNull();
        assertThat(engine != null && engine.isThrottled(), equalTo(true));
        // Try to index a document into the "test" index which is currently throttled
        logger.info("--> Try to index a doc while indexing is paused");
        IndexRequestBuilder indexRequestBuilder = prepareIndex("test").setId(Integer.toString(20)).setSource("field", "value" + 20);
        var future = indexRequestBuilder.execute();
        expectThrows(ElasticsearchException.class, () -> future.actionGet(500, TimeUnit.MILLISECONDS));
        // Verify that the new document has not been indexed indicating that the indexing thread is paused.
        logger.info("--> verifying count is unchanged...");
        indicesAdmin().prepareRefresh().get();
        assertHitCount(prepareSearch("test").setSize(0), numDocs);

        logger.info("--> relocate the shard from node1 to node2");
        updateIndexSettings(Settings.builder().put("index.routing.allocation.include._name", node_2), "test");
        ensureGreen(ACCEPTABLE_RELOCATION_TIME, "test");

        // Relocation will suspend throttling for the paused shard, allow the indexing thread to proceed, thereby releasing
        // the indexing permit it holds, in turn allowing relocation to acquire the permits and proceed.
        clusterHealthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> verifying count after relocation ...");
        future.actionGet();
        indicesAdmin().prepareRefresh().get();
        assertHitCount(prepareSearch("test").setSize(0), numDocs + 1);
    }

    public void testRelocationWhileIndexingRandom() throws Exception {
        int numberOfRelocations = scaledRandomIntBetween(1, rarely() ? 10 : 4);
        int numberOfReplicas = randomBoolean() ? 0 : 1;
        int numberOfNodes = numberOfReplicas == 0 ? 2 : 3;
        boolean throttleIndexing = randomBoolean();

        logger.info(
            "testRelocationWhileIndexingRandom(numRelocations={}, numberOfReplicas={}, numberOfNodes={})",
            numberOfRelocations,
            numberOfReplicas,
            numberOfNodes
        );

        // Randomly use pause throttling vs lock throttling, to verify that relocations proceed regardless
        String[] nodes = new String[numberOfNodes];
        logger.info("--> starting [node1] ...");
        nodes[0] = internalCluster().startNode(
            Settings.builder().put(IndexingMemoryController.PAUSE_INDEXING_ON_THROTTLE.getKey(), randomBoolean()));

        logger.info("--> creating test index ...");
        prepareCreate("test", indexSettings(1, numberOfReplicas)).get();

        for (int i = 2; i <= numberOfNodes; i++) {
            logger.info("--> starting [node{}] ...", i);
            nodes[i - 1] = internalCluster().startNode(
                Settings.builder().put(IndexingMemoryController.PAUSE_INDEXING_ON_THROTTLE.getKey(), randomBoolean()));
            if (i != numberOfNodes) {
                ClusterHealthResponse healthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForNodes(Integer.toString(i))
                    .setWaitForGreenStatus()
                    .get();
                assertThat(healthResponse.isTimedOut(), equalTo(false));
            }
        }

        int numDocs = scaledRandomIntBetween(200, 2500);
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", client(), numDocs)) {
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

                // Throttle indexing on source shard
                if (throttleIndexing) {
                    IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodes[fromNode]);
                    IndexShard shard = indicesService.indexServiceSafe(resolveIndex("test")).getShard(0);
                    // Activate index throttling on "test" index primary shard
                    logger.info("--> activate throttling for shard on node {}...", nodes[fromNode]);
                    shard.activateThrottling();
                    // Verify that indexing is throttled for this shard
                    Engine engine = shard.getEngineOrNull();
                    assertThat(engine != null && engine.isThrottled(), equalTo(true));
                }
                logger.debug("--> Allow indexer to index [{}] documents", numDocs);
                indexer.continueIndexing(numDocs);
                logger.info("--> START relocate the shard from {} to {}", nodes[fromNode], nodes[toNode]);

                updateIndexSettings(Settings.builder().put("index.routing.allocation.include._name", nodes[toNode]), "test");
                ensureGreen(ACCEPTABLE_RELOCATION_TIME, "test");

                if (rarely()) {
                    logger.debug("--> flushing");
                    indicesAdmin().prepareFlush().get();
                }
                ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForNoRelocatingShards(true)
                    .setTimeout(ACCEPTABLE_RELOCATION_TIME)
                    .get();
                assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
                indexer.pauseIndexing();
                logger.info("--> DONE relocate the shard from {} to {}", fromNode, toNode);
                // Deactivate throttle on source shard
                if (throttleIndexing) {
                    IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodes[fromNode]);
                    IndexShard shard = indicesService.indexServiceSafe(resolveIndex("test")).getShard(0);
                    logger.info("--> deactivate throttling for shard on node {}...", nodes[fromNode]);
                    shard.deactivateThrottling();
                }
            }
            logger.info("--> done relocations");
            logger.info("--> waiting for indexing threads to stop ...");
            indexer.stopAndAwaitStopped();
            logger.info("--> indexing threads stopped");

            logger.info("--> refreshing the index");
            indicesAdmin().prepareRefresh("test").get();
            logger.info("--> searching the index");
            for (int i = 0; i < 10; i++) {
                final int idx = i;
                logger.info("--> START search test round {}", i + 1);
                assertResponse(
                    prepareSearch("test").setQuery(matchAllQuery()).setSize((int) indexer.totalIndexedDocs()).storedFields(),
                    response -> {
                        var hits = response.getHits();
                        if (hits.getTotalHits().value() != indexer.totalIndexedDocs()) {
                            int[] hitIds = new int[(int) indexer.totalIndexedDocs()];
                            for (int hit = 0; hit < indexer.totalIndexedDocs(); hit++) {
                                hitIds[hit] = hit + 1;
                            }
                            Set<Integer> set = Arrays.stream(hitIds).boxed().collect(Collectors.toSet());
                            for (SearchHit hit : hits.getHits()) {
                                int id = Integer.parseInt(hit.getId());
                                if (set.remove(id) == false) {
                                    logger.error("Extra id [{}]", id);
                                }
                            }
                            set.forEach(value -> logger.error("Missing id [{}]", value));
                        }
                        assertThat(hits.getTotalHits().value(), equalTo(indexer.totalIndexedDocs()));
                        logger.info("--> DONE search test round {}", idx + 1);
                    }
                );
            }
        }
    }

    public void testRelocationWhileRefreshing() throws Exception {
        int numberOfRelocations = scaledRandomIntBetween(1, rarely() ? 10 : 4);
        int numberOfReplicas = randomBoolean() ? 0 : 1;
        int numberOfNodes = numberOfReplicas == 0 ? 2 : 3;

        logger.info(
            "testRelocationWhileIndexingRandom(numRelocations={}, numberOfReplicas={}, numberOfNodes={})",
            numberOfRelocations,
            numberOfReplicas,
            numberOfNodes
        );

        String[] nodes = new String[numberOfNodes];
        logger.info("--> starting [node_0] ...");
        nodes[0] = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate(
            "test",
            // set refresh_interval because we want to control refreshes
            indexSettings(1, numberOfReplicas).put("index.refresh_interval", -1)
        ).get();

        for (int i = 1; i < numberOfNodes; i++) {
            logger.info("--> starting [node_{}] ...", i);
            nodes[i] = internalCluster().startNode();
            if (i != numberOfNodes - 1) {
                ClusterHealthResponse healthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                    .setWaitForEvents(Priority.LANGUID)
                    .setWaitForNodes(Integer.toString(i + 1))
                    .setWaitForGreenStatus()
                    .get();
                assertThat(healthResponse.isTimedOut(), equalTo(false));
            }
        }

        final Semaphore postRecoveryShards = new Semaphore(0);
        final IndexEventListener listener = new IndexEventListener() {
            @Override
            public void indexShardStateChanged(
                IndexShard indexShard,
                @Nullable IndexShardState previousState,
                IndexShardState currentState,
                @Nullable String reason
            ) {
                if (currentState == IndexShardState.POST_RECOVERY) {
                    postRecoveryShards.release();
                }
            }
        };
        for (MockIndexEventListener.TestEventListener eventListener : internalCluster().getInstances(
            MockIndexEventListener.TestEventListener.class
        )) {
            eventListener.setNewDelegate(listener);
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
                builders1.add(prepareIndex("test").setSource("{}", XContentType.JSON));
            }

            List<IndexRequestBuilder> builders2 = new ArrayList<>();
            for (int numDocs = randomIntBetween(10, 30); numDocs > 0; numDocs--) {
                builders2.add(prepareIndex("test").setSource("{}", XContentType.JSON));
            }

            logger.info("--> START relocate the shard from {} to {}", nodes[fromNode], nodes[toNode]);

            ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand("test", 0, nodes[fromNode], nodes[toNode]));

            logger.debug("--> index [{}] documents", builders1.size());
            indexRandom(false, true, builders1);
            // wait for shard to reach post recovery
            postRecoveryShards.acquire(1);

            logger.debug("--> index [{}] documents", builders2.size());
            indexRandom(true, true, builders2);

            // verify cluster was finished.
            assertFalse(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                    .setWaitForNoRelocatingShards(true)
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout(TimeValue.timeValueSeconds(30))
                    .get()
                    .isTimedOut()
            );
            logger.info("--> DONE relocate the shard from {} to {}", fromNode, toNode);

            logger.debug("--> verifying all searches return the same number of docs");
            long[] expectedCount = new long[] { -1 };
            for (Client client : clients()) {
                assertNoFailuresAndResponse(client.prepareSearch("test").setPreference("_local").setSize(0), response -> {
                    if (expectedCount[0] < 0) {
                        expectedCount[0] = response.getHits().getTotalHits().value();
                    } else {
                        assertEquals(expectedCount[0], response.getHits().getTotalHits().value());
                    }
                });
            }

        }

    }

    public void testCancellationCleansTempFiles() throws Exception {
        final String indexName = "test";

        final String p_node = internalCluster().startNode();

        prepareCreate(indexName, indexSettings(1, 0)).get();

        internalCluster().startNode();
        internalCluster().startNode();

        List<IndexRequestBuilder> requests = new ArrayList<>();
        int numDocs = scaledRandomIntBetween(25, 250);
        for (int i = 0; i < numDocs; i++) {
            requests.add(prepareIndex(indexName).setSource("{}", XContentType.JSON));
        }
        indexRandom(true, requests);
        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForNodes("3").setWaitForGreenStatus().get().isTimedOut());
        flush();

        int allowedFailures = randomIntBetween(3, 5); // the default of the `index.allocation.max_retries` is 5.
        logger.info("--> blocking recoveries from primary (allowed failures: [{}])", allowedFailures);
        CountDownLatch corruptionCount = new CountDownLatch(allowedFailures);
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, p_node);
        final var mockTransportService = MockTransportService.getInstance(p_node);
        for (DiscoveryNode node : clusterService.state().nodes()) {
            if (node.equals(clusterService.localNode()) == false) {
                mockTransportService.addSendBehavior(
                    internalCluster().getInstance(TransportService.class, node.getName()),
                    new RecoveryCorruption(corruptionCount)
                );
            }
        }
        setReplicaCount(1, indexName);
        corruptionCount.await();

        logger.info("--> stopping replica assignment");
        updateClusterSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none"));

        logger.info("--> wait for all replica shards to be removed, on all nodes");
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                if (node.equals(p_node)) {
                    continue;
                }
                ClusterState state = client(node).admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).setLocal(true).get().getState();
                assertThat(
                    node + " indicates assigned replicas",
                    state.getRoutingTable().index(indexName).shardsWithState(ShardRoutingState.UNASSIGNED).size(),
                    equalTo(1)
                );
            }
        });

        logger.info("--> verifying no temporary recoveries are left");
        for (String node : internalCluster().getNodeNames()) {
            NodeEnvironment nodeEnvironment = internalCluster().getInstance(NodeEnvironment.class, node);
            for (final Path shardLoc : nodeEnvironment.availableShardPaths(new ShardId(indexName, "_na_", 0))) {
                if (Files.exists(shardLoc)) {
                    assertBusy(() -> {
                        try {
                            forEachFileRecursively(
                                shardLoc,
                                (file, attrs) -> assertThat(
                                    "found a temporary recovery file: " + file,
                                    file.getFileName().toString(),
                                    not(startsWith("recovery."))
                                )
                            );
                        } catch (IOException e) {
                            throw new AssertionError("failed to walk file tree starting at [" + shardLoc + "]", e);
                        }
                    });
                }
            }
        }
    }

    public void testIndexSearchAndRelocateConcurrently() throws Exception {
        int halfNodes = randomIntBetween(1, 3);
        Settings[] nodeSettings = Stream.concat(
            Stream.generate(() -> Settings.builder().put("node.attr.color", "blue").build()).limit(halfNodes),
            Stream.generate(() -> Settings.builder().put("node.attr.color", "red").build()).limit(halfNodes)
        ).toArray(Settings[]::new);
        List<String> nodes = internalCluster().startNodes(nodeSettings);
        String[] blueNodes = nodes.subList(0, halfNodes).stream().toArray(String[]::new);
        String[] redNodes = nodes.subList(halfNodes, nodes.size()).stream().toArray(String[]::new);
        logger.info("blue nodes: {}", (Object) blueNodes);
        logger.info("red nodes: {}", (Object) redNodes);
        ensureStableCluster(halfNodes * 2);

        final Settings.Builder settings = Settings.builder()
            .put("index.routing.allocation.exclude.color", "blue")
            .put(indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(halfNodes - 1));
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), randomIntBetween(1, 10) + "s");
        }
        assertAcked(prepareCreate("test", settings));
        assertAllShardsOnNodes("test", redNodes);
        AtomicBoolean stopped = new AtomicBoolean(false);
        Thread[] searchThreads = randomBoolean() ? new Thread[0] : new Thread[randomIntBetween(1, 4)];
        for (int i = 0; i < searchThreads.length; i++) {
            searchThreads[i] = new Thread(() -> {
                while (stopped.get() == false) {
                    assertNoFailures(prepareSearch("test").setRequestCache(false));
                }
            });
            searchThreads[i].start();
        }
        int numDocs = randomIntBetween(100, 150);
        ArrayList<String> ids = new ArrayList<>();
        logger.info(" --> indexing [{}] docs", numDocs);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            String id = randomRealisticUnicodeOfLength(10) + String.valueOf(i);
            ids.add(id);
            docs[i] = prepareIndex("test").setId(id).setSource("field1", English.intToEnglish(i));
        }
        indexRandom(true, docs);
        assertHitCount(prepareSearch("test"), numDocs);

        logger.info(" --> moving index to new nodes");
        updateIndexSettings(
            Settings.builder().put("index.routing.allocation.exclude.color", "red").put("index.routing.allocation.include.color", "blue"),
            "test"
        );
        // index while relocating
        logger.info(" --> indexing [{}] more docs", numDocs);
        for (int i = 0; i < numDocs; i++) {
            String id = randomRealisticUnicodeOfLength(10) + String.valueOf(numDocs + i);
            ids.add(id);
            docs[i] = prepareIndex("test").setId(id).setSource("field1", English.intToEnglish(numDocs + i));
        }
        indexRandom(true, docs);

        logger.info(" --> waiting for relocation to complete");
        ensureGreen(TimeValue.timeValueSeconds(60), "test"); // move all shards to the new nodes (it waits on relocation)

        final int numIters = randomIntBetween(10, 20);
        for (int i = 0; i < numIters; i++) {
            logger.info(" --> checking iteration {}", i);
            assertSearchHitsWithoutFailures(prepareSearch().setSize(ids.size()), ids.toArray(Strings.EMPTY_ARRAY));
        }
        stopped.set(true);
        for (Thread searchThread : searchThreads) {
            searchThread.join();
        }
    }

    public void testRelocateWhileWaitingForRefresh() {
        logger.info("--> starting [node1] ...");
        final String node1 = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate(
            "test",
            indexSettings(1, 0)
                // we want to control refreshes
                .put("index.refresh_interval", -1)
        ).get();

        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        logger.info("--> flush so we have an actual index");
        indicesAdmin().prepareFlush().get();
        logger.info("--> index more docs so we have something in the translog");
        for (int i = 10; i < 20; i++) {
            prepareIndex("test").setId(Integer.toString(i))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .setSource("field", "value" + i)
                .execute();
        }

        logger.info("--> start another node");
        final String node2 = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> relocate the shard from node1 to node2");
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand("test", 0, node1, node2));

        clusterHealthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> verifying count");
        indicesAdmin().prepareRefresh().get();
        assertHitCount(prepareSearch("test").setSize(0), 20);
    }

    public void testRelocateWhileContinuouslyIndexingAndWaitingForRefresh() throws Exception {
        logger.info("--> starting [node1] ...");
        final String node1 = internalCluster().startNode();

        logger.info("--> creating test index ...");
        prepareCreate(
            "test",
            // we want to control refreshes
            indexSettings(1, 0).put("index.refresh_interval", -1)
        ).get();

        logger.info("--> index 10 docs");
        for (int i = 0; i < 10; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        logger.info("--> flush so we have an actual index");
        indicesAdmin().prepareFlush().get();
        logger.info("--> index more docs so we have something in the translog");
        final List<ActionFuture<DocWriteResponse>> pendingIndexResponses = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            pendingIndexResponses.add(
                prepareIndex("test").setId(Integer.toString(i))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }

        logger.info("--> start another node");
        final String node2 = internalCluster().startNode();
        ClusterHealthResponse clusterHealthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("2")
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> relocate the shard from node1 to node2");
        ActionFuture<ClusterRerouteResponse> relocationListener = client().execute(
            TransportClusterRerouteAction.TYPE,
            new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).add(new MoveAllocationCommand("test", 0, node1, node2))
        );
        logger.info("--> index 100 docs while relocating");
        for (int i = 20; i < 120; i++) {
            pendingIndexResponses.add(
                prepareIndex("test").setId(Integer.toString(i))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }
        safeGet(relocationListener);
        clusterHealthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(ACCEPTABLE_RELOCATION_TIME)
            .get();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        logger.info("--> verifying count");
        assertBusy(() -> {
            indicesAdmin().prepareRefresh().get();
            assertTrue(pendingIndexResponses.stream().allMatch(ActionFuture::isDone));
        }, 1, TimeUnit.MINUTES);

        assertHitCount(prepareSearch("test").setSize(0), 120);
    }

    public void testRelocationEstablishedPeerRecoveryRetentionLeases() throws Exception {
        int halfNodes = randomIntBetween(1, 3);
        String indexName = "test";
        Settings[] nodeSettings = Stream.concat(
            Stream.generate(() -> Settings.builder().put("node.attr.color", "blue").build()).limit(halfNodes),
            Stream.generate(() -> Settings.builder().put("node.attr.color", "red").build()).limit(halfNodes)
        ).toArray(Settings[]::new);
        List<String> nodes = internalCluster().startNodes(nodeSettings);
        String[] blueNodes = nodes.subList(0, halfNodes).toArray(String[]::new);
        String[] redNodes = nodes.subList(halfNodes, nodes.size()).toArray(String[]::new);
        logger.debug("--> blue nodes: [{}], red nodes: [{}]", blueNodes, redNodes);
        ensureStableCluster(halfNodes * 2);
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, halfNodes - 1))
                        .put("index.routing.allocation.include.color", "blue")
                )
        );
        ensureGreen("test");
        assertBusy(() -> assertAllShardsOnNodes(indexName, blueNodes));
        assertActiveCopiesEstablishedPeerRecoveryRetentionLeases();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.include.color", "red"), indexName);
        assertBusy(() -> assertAllShardsOnNodes(indexName, redNodes));
        ensureGreen("test");
        assertActiveCopiesEstablishedPeerRecoveryRetentionLeases();
    }

    private void assertActiveCopiesEstablishedPeerRecoveryRetentionLeases() throws Exception {
        assertBusy(() -> {
            for (String index : clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                .get()
                .getState()
                .metadata()
                .getProject()
                .indices()
                .keySet()) {
                Map<ShardId, List<ShardStats>> byShardId = Stream.of(indicesAdmin().prepareStats(index).get().getShards())
                    .collect(Collectors.groupingBy(l -> l.getShardRouting().shardId()));
                for (List<ShardStats> shardStats : byShardId.values()) {
                    Set<String> expectedLeaseIds = shardStats.stream()
                        .map(s -> ReplicationTracker.getPeerRecoveryRetentionLeaseId(s.getShardRouting()))
                        .collect(Collectors.toSet());
                    for (ShardStats shardStat : shardStats) {
                        Set<String> actualLeaseIds = shardStat.getRetentionLeaseStats()
                            .retentionLeases()
                            .leases()
                            .stream()
                            .map(RetentionLease::id)
                            .collect(Collectors.toSet());
                        assertThat(expectedLeaseIds, everyItem(in(actualLeaseIds)));
                    }
                }
            }
        });
    }

    class RecoveryCorruption implements StubbableTransport.SendRequestBehavior {

        private final CountDownLatch corruptionCount;

        RecoveryCorruption(CountDownLatch corruptionCount) {
            this.corruptionCount = corruptionCount;
        }

        @Override
        public void sendRequest(
            Transport.Connection connection,
            long requestId,
            String action,
            TransportRequest request,
            TransportRequestOptions options
        ) throws IOException {
            if (action.equals(PeerRecoveryTargetService.Actions.FILE_CHUNK)) {
                RecoveryFileChunkRequest chunkRequest = (RecoveryFileChunkRequest) request;
                if (chunkRequest.name().startsWith(IndexFileNames.SEGMENTS)) {
                    // corrupting the segments_N files in order to make sure future recovery re-send files
                    logger.debug("corrupting [{}] to {}. file name: [{}]", action, connection.getNode(), chunkRequest.name());
                    assert chunkRequest.content().toBytesRef().bytes == chunkRequest.content().toBytesRef().bytes
                        : "no internal reference!!";
                    byte[] array = chunkRequest.content().toBytesRef().bytes;
                    array[0] = (byte) ~array[0]; // flip one byte in the content
                    corruptionCount.countDown();
                }
                connection.sendRequest(requestId, action, request, options);
            } else {
                connection.sendRequest(requestId, action, request, options);
            }
        }
    }
}
