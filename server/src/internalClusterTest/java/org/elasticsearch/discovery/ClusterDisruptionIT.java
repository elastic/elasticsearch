/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.LagDetector;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.Bridge;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.elasticsearch.action.DocWriteResponse.Result.CREATED;
import static org.elasticsearch.action.DocWriteResponse.Result.UPDATED;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

/**
 * Tests various cluster operations (e.g., indexing) during disruptions.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterDisruptionIT extends AbstractDisruptionTestCase {

    private enum ConflictMode {
        none,
        external,
        create;

        static ConflictMode randomMode() {
            ConflictMode[] values = values();
            return values[randomInt(values.length - 1)];
        }
    }

    /**
     * Test that we do not loose document whose indexing request was successful, under a randomly selected disruption scheme
     * We also collect &amp; report the type of indexing failures that occur.
     * <p>
     * This test is a superset of tests run in the Jepsen test suite, with the exception of versioned updates
     */
    @TestIssueLogging(
        value = "_root:DEBUG,org.elasticsearch.action.bulk:TRACE,org.elasticsearch.action.get:TRACE,"
            + "org.elasticsearch.discovery:TRACE,org.elasticsearch.action.support.replication:TRACE,"
            + "org.elasticsearch.cluster.service:TRACE,org.elasticsearch.indices.recovery:TRACE,"
            + "org.elasticsearch.indices.cluster:TRACE,org.elasticsearch.index.shard:TRACE",
        issueUrl = "https://github.com/elastic/elasticsearch/issues/41068"
    )
    public void testAckedIndexing() throws Exception {

        final int seconds = (TEST_NIGHTLY && rarely()) == false ? 1 : 5;
        final TimeValue timeout = TimeValue.timeValueSeconds(seconds);

        final List<String> nodes = startCluster(rarely() ? 5 : 3);

        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1 + randomInt(2))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
            )
        );
        ensureGreen();

        ServiceDisruptionScheme disruptionScheme = addRandomDisruptionScheme();
        logger.info("disruption scheme [{}] added", disruptionScheme);

        final ConcurrentHashMap<String, String> ackedDocs = new ConcurrentHashMap<>(); // id -> node sent.

        final AtomicBoolean stop = new AtomicBoolean(false);
        List<Thread> indexers = new ArrayList<>(nodes.size());
        List<Semaphore> semaphores = new ArrayList<>(nodes.size());
        final AtomicInteger idGenerator = new AtomicInteger(0);
        final AtomicReference<CountDownLatch> countDownLatchRef = new AtomicReference<>();
        final List<Exception> exceptedExceptions = new CopyOnWriteArrayList<>();

        final ConflictMode conflictMode = ConflictMode.randomMode();
        final List<String> fieldNames = IntStream.rangeClosed(0, randomInt(10)).mapToObj(n -> "f" + n).toList();

        logger.info("starting indexers using conflict mode " + conflictMode);
        try {
            for (final String node : nodes) {
                final Semaphore semaphore = new Semaphore(0);
                semaphores.add(semaphore);
                final Client client = client(node);
                final String name = "indexer_" + indexers.size();
                final int numPrimaries = getNumShards("test").numPrimaries;
                Thread thread = new Thread(() -> {
                    while (stop.get() == false) {
                        String id = null;
                        try {
                            if (semaphore.tryAcquire(10, TimeUnit.SECONDS) == false) {
                                continue;
                            }
                            logger.info("[{}] Acquired semaphore and it has {} permits left", name, semaphore.availablePermits());
                            try {
                                id = Integer.toString(idGenerator.incrementAndGet());
                                int shard = Math.floorMod(Murmur3HashFunction.hash(id), numPrimaries);
                                logger.trace("[{}] indexing id [{}] through node [{}] targeting shard [{}]", name, id, node, shard);
                                IndexRequestBuilder indexRequestBuilder = client.prepareIndex("test")
                                    .setId(id)
                                    .setSource(Map.of(randomFrom(fieldNames), randomNonNegativeLong()), XContentType.JSON)
                                    .setTimeout(timeout);

                                if (conflictMode == ConflictMode.external) {
                                    indexRequestBuilder.setVersion(randomIntBetween(1, 10)).setVersionType(VersionType.EXTERNAL);
                                } else if (conflictMode == ConflictMode.create) {
                                    indexRequestBuilder.setCreate(true);
                                }

                                DocWriteResponse response = indexRequestBuilder.get(timeout);
                                assertThat(response.getResult(), is(oneOf(CREATED, UPDATED)));
                                ackedDocs.put(id, node);
                                logger.trace("[{}] indexed id [{}] through node [{}], response [{}]", name, id, node, response);
                            } catch (ElasticsearchException e) {
                                exceptedExceptions.add(e);
                                final String docId = id;
                                logger.trace(() -> format("[%s] failed id [%s] through node [%s]", name, docId, node), e);
                            } finally {
                                countDownLatchRef.get().countDown();
                                logger.trace("[{}] decreased counter : {}", name, countDownLatchRef.get().getCount());
                            }
                        } catch (InterruptedException e) {
                            // fine - semaphore interrupt
                        } catch (AssertionError | Exception e) {
                            logger.info(() -> "unexpected exception in background thread of [" + node + "]", e);
                        }
                    }
                });

                thread.setName(name);
                thread.start();
                indexers.add(thread);
            }

            int docsPerIndexer = randomInt(3);
            logger.info("indexing {} docs per indexer before partition", docsPerIndexer);
            countDownLatchRef.set(new CountDownLatch(docsPerIndexer * indexers.size()));
            for (Semaphore semaphore : semaphores) {
                semaphore.release(docsPerIndexer);
            }
            assertTrue(countDownLatchRef.get().await(1, TimeUnit.MINUTES));

            for (int iter = 1 + randomInt(2); iter > 0; iter--) {
                logger.info("starting disruptions & indexing (iteration [{}])", iter);
                disruptionScheme.startDisrupting();

                docsPerIndexer = 1 + randomInt(5);
                logger.info("indexing {} docs per indexer during partition", docsPerIndexer);
                countDownLatchRef.set(new CountDownLatch(docsPerIndexer * indexers.size()));
                Collections.shuffle(semaphores, random());
                for (Semaphore semaphore : semaphores) {
                    assertThat(semaphore.availablePermits(), equalTo(0));
                    semaphore.release(docsPerIndexer);
                }
                logger.info("waiting for indexing requests to complete");
                assertTrue(countDownLatchRef.get().await(docsPerIndexer * seconds * 1000 + 2000, TimeUnit.MILLISECONDS));

                logger.info("stopping disruption");
                disruptionScheme.stopDisrupting();
                for (String node : internalCluster().getNodeNames()) {
                    ensureStableCluster(
                        nodes.size(),
                        TimeValue.timeValueMillis(disruptionScheme.expectedTimeToHeal().millis() + DISRUPTION_HEALING_OVERHEAD.millis()),
                        true,
                        node
                    );
                }
                // in case of a bridge partition, shard allocation can fail "index.allocation.max_retries" times if the master
                // is the super-connected node and recovery source and target are on opposite sides of the bridge
                if (disruptionScheme instanceof NetworkDisruption networkDisruption
                    && networkDisruption.getDisruptedLinks() instanceof Bridge) {
                    assertBusy(() -> assertAcked(clusterAdmin().prepareReroute().setRetryFailed(true)));
                }
                ensureGreen("test");

                logger.info("validating successful docs");
                assertBusy(() -> {
                    for (String node : nodes) {
                        try {
                            logger.debug("validating through node [{}] ([{}] acked docs)", node, ackedDocs.size());
                            for (String id : ackedDocs.keySet()) {
                                assertTrue(
                                    "doc [" + id + "] indexed via node [" + ackedDocs.get(id) + "] not found",
                                    client(node).prepareGet("test", id).setPreference("_local").get().isExists()
                                );
                            }
                        } catch (AssertionError | NoShardAvailableActionException e) {
                            throw new AssertionError(e.getMessage() + " (checked via node [" + node + "]", e);
                        }
                    }
                }, 30, TimeUnit.SECONDS);

                logger.info("done validating (iteration [{}])", iter);
            }
        } finally {
            logger.info("shutting down indexers");
            stop.set(true);
            for (Thread indexer : indexers) {
                indexer.interrupt();
                indexer.join(60000);
            }
            if (exceptedExceptions.size() > 0) {
                StringBuilder sb = new StringBuilder();
                for (Exception e : exceptedExceptions) {
                    sb.append("\n").append(e.getMessage());
                }
                logger.debug("Indexing exceptions during disruption: {}", sb);
            }
        }
    }

    /**
     * Test that a document which is indexed on the majority side of a partition, is available from the minority side,
     * once the partition is healed
     */
    public void testRejoinDocumentExistsInAllShardCopies() throws Exception {
        List<String> nodes = startCluster(3);

        assertAcked(prepareCreate("test").setSettings(indexSettings(1, 2)).get());
        ensureGreen("test");

        nodes = new ArrayList<>(nodes);
        Collections.shuffle(nodes, random());
        String isolatedNode = nodes.get(0);
        String notIsolatedNode = nodes.get(1);

        TwoPartitions partitions = isolateNode(isolatedNode);
        NetworkDisruption scheme = addRandomDisruptionType(partitions);
        scheme.startDisrupting();
        ensureStableCluster(2, notIsolatedNode);
        assertFalse(client(notIsolatedNode).admin().cluster().prepareHealth("test").setWaitForYellowStatus().get().isTimedOut());

        DocWriteResponse indexResponse = internalCluster().client(notIsolatedNode).prepareIndex("test").setSource("field", "value").get();
        assertThat(indexResponse.getVersion(), equalTo(1L));

        logger.info("Verifying if document exists via node[{}]", notIsolatedNode);
        GetResponse getResponse = internalCluster().client(notIsolatedNode)
            .prepareGet("test", indexResponse.getId())
            .setPreference("_local")
            .get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getVersion(), equalTo(1L));
        assertThat(getResponse.getId(), equalTo(indexResponse.getId()));

        scheme.stopDisrupting();

        ensureStableCluster(3);
        ensureGreen("test");

        for (String node : nodes) {
            logger.info("Verifying if document exists after isolating node[{}] via node[{}]", isolatedNode, node);
            getResponse = internalCluster().client(node).prepareGet("test", indexResponse.getId()).setPreference("_local").get();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getVersion(), equalTo(1L));
            assertThat(getResponse.getId(), equalTo(indexResponse.getId()));
        }
    }

    // simulate handling of sending shard failure during an isolation
    public void testSendingShardFailure() throws Exception {
        List<String> nodes = startCluster(3);
        String masterNode = internalCluster().getMasterName();
        List<String> nonMasterNodes = nodes.stream().filter(node -> node.equals(masterNode) == false).toList();
        String nonMasterNode = randomFrom(nonMasterNodes);
        assertAcked(prepareCreate("test").setSettings(indexSettings(3, 2)));
        ensureGreen();
        String nonMasterNodeId = internalCluster().clusterService(nonMasterNode).localNode().getId();

        // fail a random shard
        ShardRouting failedShard = randomFrom(
            clusterService().state().getRoutingNodes().node(nonMasterNodeId).shardsWithState(ShardRoutingState.STARTED).toList()
        );
        ShardStateAction service = internalCluster().getInstance(ShardStateAction.class, nonMasterNode);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean();

        String isolatedNode = randomBoolean() ? masterNode : nonMasterNode;
        TwoPartitions partitions = isolateNode(isolatedNode);
        // we cannot use the NetworkUnresponsive disruption type here as it will swallow the "shard failed" request, calling neither
        // onSuccess nor onFailure on the provided listener.
        NetworkDisruption networkDisruption = new NetworkDisruption(partitions, NetworkDisruption.DISCONNECT);
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        service.localShardFailed(
            failedShard,
            "simulated",
            new CorruptIndexException("simulated", (String) null),
            new ActionListener<Void>() {
                @Override
                public void onResponse(final Void aVoid) {
                    success.set(true);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    success.set(false);
                    latch.countDown();
                    assert false;
                }
            }
        );

        if (isolatedNode.equals(nonMasterNode)) {
            assertNoMaster(nonMasterNode);
        } else {
            ensureStableCluster(2, nonMasterNode);
        }

        // heal the partition
        networkDisruption.removeAndEnsureHealthy(internalCluster());

        // the cluster should stabilize
        ensureStableCluster(3);

        latch.await();

        // the listener should be notified
        assertTrue(success.get());

        // the failed shard should be gone
        List<ShardRouting> shards = clusterService().state().getRoutingTable().allShards("test");
        for (ShardRouting shard : shards) {
            assertThat(shard.allocationId(), not(equalTo(failedShard.allocationId())));
        }
    }

    public void testCannotJoinIfMasterLostDataFolder() throws Exception {
        String masterNode = internalCluster().startMasterOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();

        internalCluster().restartNode(masterNode, new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }

            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder()
                    .put(ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey(), nodeName)
                    /*
                     * the data node might join while the master is still not fully established as master just yet and bypasses the join
                     * validation that is done before adding the node to the cluster. Only the join validation when handling the publish
                     * request takes place, but at this point the cluster state has been successfully committed, and will subsequently be
                     * exposed to the applier. The health check below therefore sees the cluster state with the 2 nodes and thinks all is
                     * good, even though the data node never accepted this state. What's worse is that it takes 90 seconds for the data
                     * node to be kicked out of the cluster (lag detection). We speed this up here.
                     */
                    .put(LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.getKey(), "10s")
                    .build();
            }

            @Override
            public boolean validateClusterForming() {
                return false;
            }
        });

        assertBusy(() -> {
            assertFalse(internalCluster().client(masterNode).admin().cluster().prepareHealth().get().isTimedOut());
            assertTrue(
                internalCluster().client(masterNode)
                    .admin()
                    .cluster()
                    .prepareHealth()
                    .setWaitForNodes("2")
                    .setTimeout("2s")
                    .get()
                    .isTimedOut()
            );
        }, 30, TimeUnit.SECONDS);
        internalCluster().stopNode(dataNode); // otherwise we will fail during clean-up
    }

    /**
     * Tests that indices are properly deleted even if there is a master transition in between.
     * Test for https://github.com/elastic/elasticsearch/issues/11665
     */
    public void testIndicesDeleted() throws Exception {
        final String idxName = "test";
        final List<String> allMasterEligibleNodes = internalCluster().startMasterOnlyNodes(2);
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        assertAcked(prepareCreate("test"));

        final String masterNode1 = internalCluster().getMasterName();
        NetworkDisruption networkDisruption = new NetworkDisruption(
            new TwoPartitions(masterNode1, dataNode),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        // We know this will time out due to the partition, we check manually below to not proceed until
        // the delete has been applied to the master node and the master eligible node.
        internalCluster().client(masterNode1).admin().indices().prepareDelete(idxName).setTimeout("0s").get();
        // Don't restart the master node until we know the index deletion has taken effect on master and the master eligible node.
        assertBusy(() -> {
            for (String masterNode : allMasterEligibleNodes) {
                final ClusterState masterState = internalCluster().clusterService(masterNode).state();
                assertTrue("index not deleted on " + masterNode, masterState.metadata().hasIndex(idxName) == false);
            }
        });
        internalCluster().restartNode(masterNode1, InternalTestCluster.EMPTY_CALLBACK);
        ensureYellow();
        assertFalse(indexExists(idxName));
    }

    public void testRestartNodeWhileIndexing() throws Exception {
        startCluster(3);
        String index = "restart_while_indexing";
        createIndex(index, 1, between(1, 2));
        AtomicBoolean stopped = new AtomicBoolean();
        Thread[] threads = new Thread[between(1, 4)];
        AtomicInteger docID = new AtomicInteger();
        Set<String> ackedDocs = ConcurrentCollections.newConcurrentSet();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                while (stopped.get() == false && docID.get() < 5000) {
                    String id = Integer.toString(docID.incrementAndGet());
                    try {
                        DocWriteResponse response = prepareIndex(index).setId(id)
                            .setSource(Map.of("f" + randomIntBetween(1, 10), randomNonNegativeLong()), XContentType.JSON)
                            .get();
                        assertThat(response.getResult(), is(oneOf(CREATED, UPDATED)));
                        logger.info("--> index id={} seq_no={}", response.getId(), response.getSeqNo());
                        ackedDocs.add(response.getId());
                    } catch (ElasticsearchException ignore) {
                        logger.info("--> fail to index id={}", id);
                    }
                }
            });
            threads[i].start();
        }
        ensureGreen(index);
        assertBusy(() -> assertThat(docID.get(), greaterThanOrEqualTo(100)), 1L, TimeUnit.MINUTES);
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback());
        ensureGreen(index);
        assertBusy(() -> assertThat(docID.get(), greaterThanOrEqualTo(200)), 1L, TimeUnit.MINUTES);
        stopped.set(true);
        for (Thread thread : threads) {
            thread.join();
        }
        ClusterState clusterState = internalCluster().clusterService().state();
        for (ShardRouting shardRouting : clusterState.routingTable().allShards(index)) {
            String nodeName = clusterState.nodes().get(shardRouting.currentNodeId()).getName();
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
            IndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
            Set<String> docs = IndexShardTestCase.getShardDocUIDs(shard);
            assertThat(
                "shard [" + shard.routingEntry() + "] docIds [" + docs + "] vs  acked docIds [" + ackedDocs + "]",
                ackedDocs,
                everyItem(is(in(docs)))
            );
        }
    }

    public void testRejoinWhileBeingRemoved() {
        final String masterNode = internalCluster().startMasterOnlyNode(
            Settings.builder()
                .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );
        final String dataNode = internalCluster().startDataOnlyNode(
            Settings.builder()
                .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
                .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
                .build()
        );

        final ClusterService masterClusterService = internalCluster().getInstance(ClusterService.class, masterNode);
        final PlainActionFuture<Void> removedNode = new PlainActionFuture<>();
        masterClusterService.addListener(clusterChangedEvent -> {
            if (removedNode.isDone() == false && clusterChangedEvent.state().nodes().getDataNodes().isEmpty()) {
                removedNode.onResponse(null);
            }
        });

        final ClusterService dataClusterService = internalCluster().getInstance(ClusterService.class, dataNode);
        final PlainActionFuture<Void> failedLeader = new PlainActionFuture<>() {
            @Override
            protected boolean blockingAllowed() {
                // we're deliberately blocking the cluster applier on the master until the data node starts to rejoin
                return true;
            }
        };
        final AtomicBoolean dataNodeHasMaster = new AtomicBoolean(true);
        dataClusterService.addListener(clusterChangedEvent -> {
            dataNodeHasMaster.set(clusterChangedEvent.state().nodes().getMasterNode() != null);
            if (failedLeader.isDone() == false && dataNodeHasMaster.get() == false) {
                failedLeader.onResponse(null);
            }
        });

        masterClusterService.addHighPriorityApplier(event -> {
            // This is applicable for events related to the cluster disruption
            if (event.state().nodes().getDataNodes().isEmpty()) {
                failedLeader.actionGet();
                if (dataNodeHasMaster.get() == false) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        fail(e);
                    }
                }
            }
        });

        MockTransportService.getInstance(dataNode)
            .addRequestHandlingBehavior(FollowersChecker.FOLLOWER_CHECK_ACTION_NAME, (handler, request, channel, task) -> {
                if (removedNode.isDone() == false) {
                    channel.sendResponse(new ElasticsearchException("simulated check failure"));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });

        removedNode.actionGet(10, TimeUnit.SECONDS);
        ensureStableCluster(2);
    }
}
