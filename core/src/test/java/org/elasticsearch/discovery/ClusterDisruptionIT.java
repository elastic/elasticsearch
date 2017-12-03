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

package org.elasticsearch.discovery;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.store.IndicesStoreIntegrationIT;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.Bridge;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDisconnect;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkLinkDisruptionType;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Tests various cluster operations (e.g., indexing) during disruptions.
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0, autoMinMasterNodes = false)
@TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE")
public class ClusterDisruptionIT extends AbstractDisruptionTestCase {

    /**
     * Test that we do not loose document whose indexing request was successful, under a randomly selected disruption scheme
     * We also collect &amp; report the type of indexing failures that occur.
     * <p>
     * This test is a superset of tests run in the Jepsen test suite, with the exception of versioned updates
     */
    @TestLogging("_root:DEBUG,org.elasticsearch.action.bulk:TRACE,org.elasticsearch.action.get:TRACE,discovery:TRACE," +
        "org.elasticsearch.cluster.service:TRACE,org.elasticsearch.indices.recovery:TRACE," +
        "org.elasticsearch.indices.cluster:TRACE,org.elasticsearch.index.shard:TRACE")
    public void testAckedIndexing() throws Exception {

        final int seconds = !(TEST_NIGHTLY && rarely()) ? 1 : 5;
        final String timeout = seconds + "s";

        final List<String> nodes = startCluster(rarely() ? 5 : 3);

        assertAcked(prepareCreate("test")
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1 + randomInt(2))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
            ));
        ensureGreen();

        ServiceDisruptionScheme disruptionScheme = addRandomDisruptionScheme();
        logger.info("disruption scheme [{}] added", disruptionScheme);

        final ConcurrentHashMap<String, String> ackedDocs = new ConcurrentHashMap<>(); // id -> node sent.

        final AtomicBoolean stop = new AtomicBoolean(false);
        List<Thread> indexers = new ArrayList<>(nodes.size());
        List<Semaphore> semaphores = new ArrayList<>(nodes.size());
        final AtomicInteger idGenerator = new AtomicInteger(0);
        final AtomicReference<CountDownLatch> countDownLatchRef = new AtomicReference<>();
        final List<Exception> exceptedExceptions = Collections.synchronizedList(new ArrayList<Exception>());

        logger.info("starting indexers");
        try {
            for (final String node : nodes) {
                final Semaphore semaphore = new Semaphore(0);
                semaphores.add(semaphore);
                final Client client = client(node);
                final String name = "indexer_" + indexers.size();
                final int numPrimaries = getNumShards("test").numPrimaries;
                Thread thread = new Thread(() -> {
                    while (!stop.get()) {
                        String id = null;
                        try {
                            if (!semaphore.tryAcquire(10, TimeUnit.SECONDS)) {
                                continue;
                            }
                            logger.info("[{}] Acquired semaphore and it has {} permits left", name, semaphore.availablePermits());
                            try {
                                id = Integer.toString(idGenerator.incrementAndGet());
                                int shard = Math.floorMod(Murmur3HashFunction.hash(id), numPrimaries);
                                logger.trace("[{}] indexing id [{}] through node [{}] targeting shard [{}]", name, id, node, shard);
                                IndexResponse response =
                                        client.prepareIndex("test", "type", id)
                                                .setSource("{}", XContentType.JSON)
                                                .setTimeout(timeout)
                                                .get(timeout);
                                assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
                                ackedDocs.put(id, node);
                                logger.trace("[{}] indexed id [{}] through node [{}]", name, id, node);
                            } catch (ElasticsearchException e) {
                                exceptedExceptions.add(e);
                                final String docId = id;
                                logger.trace(
                                    (Supplier<?>)
                                        () -> new ParameterizedMessage("[{}] failed id [{}] through node [{}]", name, docId, node), e);
                            } finally {
                                countDownLatchRef.get().countDown();
                                logger.trace("[{}] decreased counter : {}", name, countDownLatchRef.get().getCount());
                            }
                        } catch (InterruptedException e) {
                            // fine - semaphore interrupt
                        } catch (AssertionError | Exception e) {
                            logger.info(
                                    (Supplier<?>) () -> new ParameterizedMessage("unexpected exception in background thread of [{}]", node),
                                    e);
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
                    ensureStableCluster(nodes.size(), TimeValue.timeValueMillis(disruptionScheme.expectedTimeToHeal().millis() +
                        DISRUPTION_HEALING_OVERHEAD.millis()), true, node);
                }
                // in case of a bridge partition, shard allocation can fail "index.allocation.max_retries" times if the master
                // is the super-connected node and recovery source and target are on opposite sides of the bridge
                if (disruptionScheme instanceof NetworkDisruption &&
                    ((NetworkDisruption) disruptionScheme).getDisruptedLinks() instanceof Bridge) {
                    assertAcked(client().admin().cluster().prepareReroute().setRetryFailed(true));
                }
                ensureGreen("test");

                logger.info("validating successful docs");
                assertBusy(() -> {
                    for (String node : nodes) {
                        try {
                            logger.debug("validating through node [{}] ([{}] acked docs)", node, ackedDocs.size());
                            for (String id : ackedDocs.keySet()) {
                                assertTrue("doc [" + id + "] indexed via node [" + ackedDocs.get(id) + "] not found",
                                    client(node).prepareGet("test", "type", id).setPreference("_local").get().isExists());
                            }
                        } catch (AssertionError | NoShardAvailableActionException e) {
                            throw new AssertionError(e.getMessage() + " (checked via node [" + node + "]", e);
                        }
                    }
                }, 30, TimeUnit.SECONDS);

                logger.info("done validating (iteration [{}])", iter);
            }
        } finally {
            if (exceptedExceptions.size() > 0) {
                StringBuilder sb = new StringBuilder();
                for (Exception e : exceptedExceptions) {
                    sb.append("\n").append(e.getMessage());
                }
                logger.debug("Indexing exceptions during disruption: {}", sb);
            }
            logger.info("shutting down indexers");
            stop.set(true);
            for (Thread indexer : indexers) {
                indexer.interrupt();
                indexer.join(60000);
            }
        }
    }

    /**
     * Test that a document which is indexed on the majority side of a partition, is available from the minority side,
     * once the partition is healed
     */
    public void testRejoinDocumentExistsInAllShardCopies() throws Exception {
        List<String> nodes = startCluster(3);

        assertAcked(prepareCreate("test")
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
            )
            .get());
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


        IndexResponse indexResponse = internalCluster().client(notIsolatedNode).prepareIndex("test", "type").setSource("field", "value")
            .get();
        assertThat(indexResponse.getVersion(), equalTo(1L));

        logger.info("Verifying if document exists via node[{}]", notIsolatedNode);
        GetResponse getResponse = internalCluster().client(notIsolatedNode).prepareGet("test", "type", indexResponse.getId())
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
            getResponse = internalCluster().client(node).prepareGet("test", "type", indexResponse.getId())
                .setPreference("_local")
                .get();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getVersion(), equalTo(1L));
            assertThat(getResponse.getId(), equalTo(indexResponse.getId()));
        }
    }

    // simulate handling of sending shard failure during an isolation
    public void testSendingShardFailure() throws Exception {
        List<String> nodes = startCluster(3, 2);
        String masterNode = internalCluster().getMasterName();
        List<String> nonMasterNodes = nodes.stream().filter(node -> !node.equals(masterNode)).collect(Collectors.toList());
        String nonMasterNode = randomFrom(nonMasterNodes);
        assertAcked(prepareCreate("test")
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
            ));
        ensureGreen();
        String nonMasterNodeId = internalCluster().clusterService(nonMasterNode).localNode().getId();

        // fail a random shard
        ShardRouting failedShard =
            randomFrom(clusterService().state().getRoutingNodes().node(nonMasterNodeId).shardsWithState(ShardRoutingState.STARTED));
        ShardStateAction service = internalCluster().getInstance(ShardStateAction.class, nonMasterNode);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean();

        String isolatedNode = randomBoolean() ? masterNode : nonMasterNode;
        TwoPartitions partitions = isolateNode(isolatedNode);
        // we cannot use the NetworkUnresponsive disruption type here as it will swallow the "shard failed" request, calling neither
        // onSuccess nor onFailure on the provided listener.
        NetworkLinkDisruptionType disruptionType = new NetworkDisconnect();
        NetworkDisruption networkDisruption = new NetworkDisruption(partitions, disruptionType);
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        service.localShardFailed(failedShard, "simulated", new CorruptIndexException("simulated", (String) null), new
            ShardStateAction.Listener() {
                @Override
                public void onSuccess() {
                    success.set(true);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    success.set(false);
                    latch.countDown();
                    assert false;
                }
            });

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

    /**
     * This test creates a scenario where a primary shard (0 replicas) relocates and is in POST_RECOVERY on the target
     * node but already deleted on the source node. Search request should still work.
     */
    public void testSearchWithRelocationAndSlowClusterStateProcessing() throws Exception {
        // don't use DEFAULT settings (which can cause node disconnects on a slow CI machine)
        configureCluster(Settings.EMPTY, 3, null, 1);
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String node_1 = internalCluster().startDataOnlyNode();

        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
            Settings.builder().put(indexSettings())
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        ensureGreen("test");

        final String node_2 = internalCluster().startDataOnlyNode();
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            indexRequestBuilderList.add(client().prepareIndex().setIndex("test").setType("doc")
                .setSource("{\"int_field\":1}", XContentType.JSON));
        }
        indexRandom(true, indexRequestBuilderList);

        IndicesStoreIntegrationIT.relocateAndBlockCompletion(logger, "test", 0, node_1, node_2);
        // now search for the documents and see if we get a reply
        assertThat(client().prepareSearch().setSize(0).get().getHits().getTotalHits(), equalTo(100L));
    }

    public void testIndexImportedFromDataOnlyNodesIfMasterLostDataFolder() throws Exception {
        // test for https://github.com/elastic/elasticsearch/issues/8823
        configureCluster(2, null, 1);
        String masterNode = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNode(Settings.EMPTY);

        ensureStableCluster(2);
        assertAcked(prepareCreate("index").setSettings(Settings.builder().put("index.number_of_replicas", 0)));
        index("index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        ensureGreen();

        internalCluster().restartNode(masterNode, new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });

        ensureGreen("index");
        assertTrue(client().prepareGet("index", "doc", "1").get().isExists());
    }

    /**
     * Tests that indices are properly deleted even if there is a master transition in between.
     * Test for https://github.com/elastic/elasticsearch/issues/11665
     */
    public void testIndicesDeleted() throws Exception {
        final Settings settings = Settings.builder()
            .put(DEFAULT_SETTINGS)
            .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "0s") // don't wait on isolated data node
            .put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "30s") // wait till cluster state is committed
            .build();
        final String idxName = "test";
        configureCluster(settings, 3, null, 2);
        final List<String> allMasterEligibleNodes = internalCluster().startMasterOnlyNodes(2);
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        assertAcked(prepareCreate("test"));

        final String masterNode1 = internalCluster().getMasterName();
        NetworkDisruption networkDisruption =
                new NetworkDisruption(new TwoPartitions(masterNode1, dataNode), new NetworkDisruption.NetworkUnresponsive());
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        // We know this will time out due to the partition, we check manually below to not proceed until
        // the delete has been applied to the master node and the master eligible node.
        internalCluster().client(masterNode1).admin().indices().prepareDelete(idxName).setTimeout("0s").get();
        // Don't restart the master node until we know the index deletion has taken effect on master and the master eligible node.
        assertBusy(() -> {
            for (String masterNode : allMasterEligibleNodes) {
                final ClusterState masterState = internalCluster().clusterService(masterNode).state();
                assertTrue("index not deleted on " + masterNode, masterState.metaData().hasIndex(idxName) == false);
            }
        });
        internalCluster().restartNode(masterNode1, InternalTestCluster.EMPTY_CALLBACK);
        ensureYellow();
        assertFalse(client().admin().indices().prepareExists(idxName).get().isExists());
    }

}
