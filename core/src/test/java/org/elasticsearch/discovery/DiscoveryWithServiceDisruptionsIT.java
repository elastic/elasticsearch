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

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.ZenDiscovery;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.fd.FaultDetection;
import org.elasticsearch.discovery.zen.membership.MembershipAction;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.discovery.zen.ping.ZenPingService;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.discovery.zen.publish.PublishClusterStateAction;
import org.elasticsearch.indices.store.IndicesStoreIntegrationIT;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.disruption.BridgePartition;
import org.elasticsearch.test.disruption.IntermittentLongGCDisruption;
import org.elasticsearch.test.disruption.LongGCDisruption;
import org.elasticsearch.test.disruption.NetworkDelaysPartition;
import org.elasticsearch.test.disruption.NetworkDisconnectPartition;
import org.elasticsearch.test.disruption.NetworkPartition;
import org.elasticsearch.test.disruption.NetworkUnresponsivePartition;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SingleNodeDisruption;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
@ESIntegTestCase.SuppressLocalMode
@TestLogging("_root:DEBUG,cluster.service:TRACE")
public class DiscoveryWithServiceDisruptionsIT extends ESIntegTestCase {

    private static final TimeValue DISRUPTION_HEALING_OVERHEAD = TimeValue.timeValueSeconds(40); // we use 30s as timeout in many places.

    private ClusterDiscoveryConfiguration discoveryConfig;


    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return discoveryConfig.nodeSettings(nodeOrdinal);
    }

    @Before
    public void clearConfig() {
        discoveryConfig = null;
    }

    @Override
    protected int numberOfShards() {
        return 3;
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Override
    protected void beforeIndexDeletion() {
        // some test may leave operations in flight
        // this is because the disruption schemes swallow requests by design
        // as such, these operations will never be marked as finished
    }

    private List<String> startCluster(int numberOfNodes) throws ExecutionException, InterruptedException {
        return startCluster(numberOfNodes, -1);
    }

    private List<String> startCluster(int numberOfNodes, int minimumMasterNode) throws ExecutionException, InterruptedException {
        return startCluster(numberOfNodes, minimumMasterNode, null);
    }

    private List<String> startCluster(int numberOfNodes, int minimumMasterNode, @Nullable int[] unicastHostsOrdinals) throws
            ExecutionException, InterruptedException {
        configureUnicastCluster(numberOfNodes, unicastHostsOrdinals, minimumMasterNode);
        List<String> nodes = internalCluster().startNodesAsync(numberOfNodes).get();
        ensureStableCluster(numberOfNodes);

        // TODO: this is a temporary solution so that nodes will not base their reaction to a partition based on previous successful results
        for (ZenPingService pingService : internalCluster().getInstances(ZenPingService.class)) {
            for (ZenPing zenPing : pingService.zenPings()) {
                if (zenPing instanceof UnicastZenPing) {
                    ((UnicastZenPing) zenPing).clearTemporalResponses();
                }
            }
        }
        return nodes;
    }

    static final Settings DEFAULT_SETTINGS = Settings.builder()
            .put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), "1s") // for hitting simulated network failures quickly
            .put(FaultDetection.PING_RETRIES_SETTING.getKey(), "1") // for hitting simulated network failures quickly
            .put("discovery.zen.join_timeout", "10s")  // still long to induce failures but to long so test won't time out
            .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "1s") // <-- for hitting simulated network failures quickly
            .build();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(MockTransportService.TestPlugin.class);
    }

    private void configureUnicastCluster(
        int numberOfNodes,
        @Nullable int[] unicastHostsOrdinals,
        int minimumMasterNode
    ) throws ExecutionException, InterruptedException {
        configureUnicastCluster(DEFAULT_SETTINGS, numberOfNodes, unicastHostsOrdinals, minimumMasterNode);
    }

    private void configureUnicastCluster(
        Settings settings,
        int numberOfNodes,
        @Nullable int[] unicastHostsOrdinals,
        int minimumMasterNode
    ) throws ExecutionException, InterruptedException {
        if (minimumMasterNode < 0) {
            minimumMasterNode = numberOfNodes / 2 + 1;
        }
        logger.info("---> configured unicast");
        // TODO: Rarely use default settings form some of these
        Settings nodeSettings = Settings.builder()
                .put(settings)
                .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), minimumMasterNode)
                .build();

        if (discoveryConfig == null) {
            if (unicastHostsOrdinals == null) {
                discoveryConfig = new ClusterDiscoveryConfiguration.UnicastZen(numberOfNodes, nodeSettings);
            } else {
                discoveryConfig = new ClusterDiscoveryConfiguration.UnicastZen(numberOfNodes, nodeSettings, unicastHostsOrdinals);
            }
        }
    }

    /**
     * Test that no split brain occurs under partial network partition. See https://github.com/elastic/elasticsearch/issues/2488
     */
    public void testFailWithMinimumMasterNodesConfigured() throws Exception {
        List<String> nodes = startCluster(3);

        // Figure out what is the elected master node
        final String masterNode = internalCluster().getMasterName();
        logger.info("---> legit elected master node={}", masterNode);

        // Pick a node that isn't the elected master.
        Set<String> nonMasters = new HashSet<>(nodes);
        nonMasters.remove(masterNode);
        final String unluckyNode = randomFrom(nonMasters.toArray(Strings.EMPTY_ARRAY));


        // Simulate a network issue between the unlucky node and elected master node in both directions.

        NetworkDisconnectPartition networkDisconnect = new NetworkDisconnectPartition(masterNode, unluckyNode, random());
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();

        // Wait until elected master has removed that the unlucky node...
        ensureStableCluster(2, masterNode);

        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        assertNoMaster(unluckyNode);

        networkDisconnect.stopDisrupting();

        // Wait until the master node sees all 3 nodes again.
        ensureStableCluster(3);

        // The elected master shouldn't have changed, since the unlucky node never could have elected himself as
        // master since m_m_n of 2 could never be satisfied.
        assertMaster(masterNode, nodes);
    }


    /**
     * Verify that nodes fault detection works after master (re) election
     */
    public void testNodesFDAfterMasterReelection() throws Exception {
        startCluster(4);

        logger.info("--> stopping current master");
        internalCluster().stopCurrentMasterNode();

        ensureStableCluster(3);

        logger.info("--> reducing min master nodes to 2");
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 2))
                .get());

        String master = internalCluster().getMasterName();
        String nonMaster = null;
        for (String node : internalCluster().getNodeNames()) {
            if (!node.equals(master)) {
                nonMaster = node;
            }
        }

        logger.info("--> isolating [{}]", nonMaster);
        addRandomIsolation(nonMaster).startDisrupting();

        logger.info("--> waiting for master to remove it");
        ensureStableCluster(2, master);
    }

    /**
     * Verify that the proper block is applied when nodes loose their master
     */
    public void testVerifyApiBlocksDuringPartition() throws Exception {
        startCluster(3);

        // Makes sure that the get request can be executed on each node locally:
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
        ));

        // Everything is stable now, it is now time to simulate evil...
        // but first make sure we have no initializing shards and all is green
        // (waiting for green here, because indexing / search in a yellow index is fine as long as no other nodes go down)
        ensureGreen("test");

        NetworkPartition networkPartition = addRandomPartition();

        assertEquals(1, networkPartition.getMinoritySide().size());
        final String isolatedNode = networkPartition.getMinoritySide().iterator().next();
        assertEquals(2, networkPartition.getMajoritySide().size());
        final String nonIsolatedNode = networkPartition.getMajoritySide().iterator().next();

        // Simulate a network issue between the unlucky node and the rest of the cluster.
        networkPartition.startDisrupting();


        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        logger.info("waiting for isolated node [{}] to have no master", isolatedNode);
        assertNoMaster(isolatedNode, DiscoverySettings.NO_MASTER_BLOCK_WRITES, TimeValue.timeValueSeconds(10));


        logger.info("wait until elected master has been removed and a new 2 node cluster was from (via [{}])", isolatedNode);
        ensureStableCluster(2, nonIsolatedNode);

        for (String node : networkPartition.getMajoritySide()) {
            ClusterState nodeState = getNodeClusterState(node);
            boolean success = true;
            if (nodeState.nodes().getMasterNode() == null) {
                success = false;
            }
            if (!nodeState.blocks().global().isEmpty()) {
                success = false;
            }
            if (!success) {
                fail("node [" + node + "] has no master or has blocks, despite of being on the right side of the partition. State dump:\n"
                        + nodeState.prettyPrint());
            }
        }


        networkPartition.stopDisrupting();

        // Wait until the master node sees al 3 nodes again.
        ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + networkPartition.expectedTimeToHeal().millis()));

        logger.info("Verify no master block with {} set to {}", DiscoverySettings.NO_MASTER_BLOCK_SETTING.getKey(), "all");
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(DiscoverySettings.NO_MASTER_BLOCK_SETTING.getKey(), "all"))
                .get();

        networkPartition.startDisrupting();


        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        logger.info("waiting for isolated node [{}] to have no master", isolatedNode);
        assertNoMaster(isolatedNode, DiscoverySettings.NO_MASTER_BLOCK_ALL, TimeValue.timeValueSeconds(10));

        // make sure we have stable cluster & cross partition recoveries are canceled by the removal of the missing node
        // the unresponsive partition causes recoveries to only time out after 15m (default) and these will cause
        // the test to fail due to unfreed resources
        ensureStableCluster(2, nonIsolatedNode);

    }

    /**
     * This test isolates the master from rest of the cluster, waits for a new master to be elected, restores the partition
     * and verifies that all node agree on the new cluster state
     */
    @TestLogging("_root:DEBUG,cluster.service:TRACE,gateway:TRACE,indices.store:TRACE")
    public void testIsolateMasterAndVerifyClusterStateConsensus() throws Exception {
        final List<String> nodes = startCluster(3);

        assertAcked(prepareCreate("test")
                .setSettings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1 + randomInt(2))
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomInt(2))
                ));

        ensureGreen();
        String isolatedNode = internalCluster().getMasterName();
        NetworkPartition networkPartition = addRandomIsolation(isolatedNode);
        networkPartition.startDisrupting();

        String nonIsolatedNode = networkPartition.getMajoritySide().iterator().next();

        // make sure cluster reforms
        ensureStableCluster(2, nonIsolatedNode);

        // make sure isolated need picks up on things.
        assertNoMaster(isolatedNode, TimeValue.timeValueSeconds(40));

        // restore isolation
        networkPartition.stopDisrupting();

        for (String node : nodes) {
            ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + networkPartition.expectedTimeToHeal().millis()),
                    true, node);
        }

        logger.info("issue a reroute");
        // trigger a reroute now, instead of waiting for the background reroute of RerouteService
        assertAcked(client().admin().cluster().prepareReroute());
        // and wait for it to finish and for the cluster to stabilize
        ensureGreen("test");

        // verify all cluster states are the same
        ClusterState state = null;
        for (String node : nodes) {
            ClusterState nodeState = getNodeClusterState(node);
            if (state == null) {
                state = nodeState;
                continue;
            }
            // assert nodes are identical
            try {
                assertEquals("unequal versions", state.version(), nodeState.version());
                assertEquals("unequal node count", state.nodes().getSize(), nodeState.nodes().getSize());
                assertEquals("different masters ", state.nodes().getMasterNodeId(), nodeState.nodes().getMasterNodeId());
                assertEquals("different meta data version", state.metaData().version(), nodeState.metaData().version());
                if (!state.routingTable().prettyPrint().equals(nodeState.routingTable().prettyPrint())) {
                    fail("different routing");
                }
            } catch (AssertionError t) {
                fail("failed comparing cluster state: " + t.getMessage() + "\n" +
                        "--- cluster state of node [" + nodes.get(0) + "]: ---\n" + state.prettyPrint() +
                        "\n--- cluster state [" + node + "]: ---\n" + nodeState.prettyPrint());
            }

        }
    }

    /**
     * Test that we do not loose document whose indexing request was successful, under a randomly selected disruption scheme
     * We also collect &amp; report the type of indexing failures that occur.
     * <p>
     * This test is a superset of tests run in the Jepsen test suite, with the exception of versioned updates
     */
    @TestLogging("_root:DEBUG,action.index:TRACE,action.get:TRACE,discovery:TRACE,cluster.service:TRACE,"
            + "indices.recovery:TRACE,indices.cluster:TRACE")
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
                                        client.prepareIndex("test", "type", id).setSource("{}").setTimeout(timeout).get(timeout);
                                assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
                                ackedDocs.put(id, node);
                                logger.trace("[{}] indexed id [{}] through node [{}]", name, id, node);
                            } catch (ElasticsearchException e) {
                                exceptedExceptions.add(e);
                                logger.trace("[{}] failed id [{}] through node [{}]", e, name, id, node);
                            } finally {
                                countDownLatchRef.get().countDown();
                                logger.trace("[{}] decreased counter : {}", name, countDownLatchRef.get().getCount());
                            }
                        } catch (InterruptedException e) {
                            // fine - semaphore interrupt
                        } catch (AssertionError | Exception e) {
                            logger.info("unexpected exception in background thread of [{}]", e, node);
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
                ensureGreen("test");

                logger.info("validating successful docs");
                for (String node : nodes) {
                    try {
                        logger.debug("validating through node [{}] ([{}] acked docs)", node, ackedDocs.size());
                        for (String id : ackedDocs.keySet()) {
                            assertTrue("doc [" + id + "] indexed via node [" + ackedDocs.get(id) + "] not found",
                                    client(node).prepareGet("test", "type", id).setPreference("_local").get().isExists());
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError(e.getMessage() + " (checked via node [" + node + "]", e);
                    }
                }

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
     * Test that cluster recovers from a long GC on master that causes other nodes to elect a new one
     */
    public void testMasterNodeGCs() throws Exception {
        List<String> nodes = startCluster(3, -1);

        String oldMasterNode = internalCluster().getMasterName();
        // a very long GC, but it's OK as we remove the disruption when it has had an effect
        SingleNodeDisruption masterNodeDisruption = new IntermittentLongGCDisruption(oldMasterNode, random(), 100, 200, 30000, 60000);
        internalCluster().setDisruptionScheme(masterNodeDisruption);
        masterNodeDisruption.startDisrupting();

        Set<String> oldNonMasterNodesSet = new HashSet<>(nodes);
        oldNonMasterNodesSet.remove(oldMasterNode);

        List<String> oldNonMasterNodes = new ArrayList<>(oldNonMasterNodesSet);

        logger.info("waiting for nodes to de-elect master [{}]", oldMasterNode);
        for (String node : oldNonMasterNodesSet) {
            assertDifferentMaster(node, oldMasterNode);
        }

        logger.info("waiting for nodes to elect a new master");
        ensureStableCluster(2, oldNonMasterNodes.get(0));

        logger.info("waiting for any pinging to stop");
        assertDiscoveryCompleted(oldNonMasterNodes);

        // restore GC
        masterNodeDisruption.stopDisrupting();
        ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + masterNodeDisruption.expectedTimeToHeal().millis()), false, oldNonMasterNodes.get(0));

        // make sure all nodes agree on master
        String newMaster = internalCluster().getMasterName();
        assertThat(newMaster, not(equalTo(oldMasterNode)));
        assertMaster(newMaster, nodes);
    }

    /**
     * Tests that emulates a frozen elected master node that unfreezes and pushes his cluster state to other nodes
     * that already are following another elected master node. These nodes should reject this cluster state and prevent
     * them from following the stale master.
     */
    public void testStaleMasterNotHijackingMajority() throws Exception {
        // 3 node cluster with unicast discovery and minimum_master_nodes set to 2:
        final List<String> nodes = startCluster(3, 2);

        // Save the current master node as old master node, because that node will get frozen
        final String oldMasterNode = internalCluster().getMasterName();
        for (String node : nodes) {
            ensureStableCluster(3, node);
        }
        assertMaster(oldMasterNode, nodes);

        // Simulating a painful gc by suspending all threads for a long time on the current elected master node.
        SingleNodeDisruption masterNodeDisruption = new LongGCDisruption(random(), oldMasterNode);

        // Save the majority side
        final List<String> majoritySide = new ArrayList<>(nodes);
        majoritySide.remove(oldMasterNode);

        // Keeps track of the previous and current master when a master node transition took place on each node on the majority side:
        final Map<String, List<Tuple<String, String>>> masters = Collections.synchronizedMap(new HashMap<String, List<Tuple<String,
                String>>>());
        for (final String node : majoritySide) {
            masters.put(node, new ArrayList<Tuple<String, String>>());
            internalCluster().getInstance(ClusterService.class, node).add(new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
                    DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
                    if (!Objects.equals(previousMaster, currentMaster)) {
                        logger.info("node {} received new cluster state: {} \n and had previous cluster state: {}", node, event.state(),
                                event.previousState());
                        String previousMasterNodeName = previousMaster != null ? previousMaster.getName() : null;
                        String currentMasterNodeName = currentMaster != null ? currentMaster.getName() : null;
                        masters.get(node).add(new Tuple<>(previousMasterNodeName, currentMasterNodeName));
                    }
                }
            });
        }

        final CountDownLatch oldMasterNodeSteppedDown = new CountDownLatch(1);
        internalCluster().getInstance(ClusterService.class, oldMasterNode).add(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().nodes().getMasterNodeId() == null) {
                    oldMasterNodeSteppedDown.countDown();
                }
            }
        });

        internalCluster().setDisruptionScheme(masterNodeDisruption);
        logger.info("freezing node [{}]", oldMasterNode);
        masterNodeDisruption.startDisrupting();

        // Wait for the majority side to get stable
        assertDifferentMaster(majoritySide.get(0), oldMasterNode);
        assertDifferentMaster(majoritySide.get(1), oldMasterNode);
        assertDiscoveryCompleted(majoritySide);

        // The old master node is frozen, but here we submit a cluster state update task that doesn't get executed,
        // but will be queued and once the old master node un-freezes it gets executed.
        // The old master node will send this update + the cluster state where he is flagged as master to the other
        // nodes that follow the new master. These nodes should ignore this update.
        internalCluster().getInstance(ClusterService.class, oldMasterNode).submitStateUpdateTask("sneaky-update", new
                ClusterStateUpdateTask(Priority.IMMEDIATE) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failure [{}]", e, source);
            }
        });

        // Save the new elected master node
        final String newMasterNode = internalCluster().getMasterName(majoritySide.get(0));
        logger.info("new detected master node [{}]", newMasterNode);

        // Stop disruption
        logger.info("Unfreeze node [{}]", oldMasterNode);
        masterNodeDisruption.stopDisrupting();

        oldMasterNodeSteppedDown.await(30, TimeUnit.SECONDS);
        // Make sure that the end state is consistent on all nodes:
        assertDiscoveryCompleted(nodes);
        // Use assertBusy(...) because the unfrozen node may take a while to actually join the cluster.
        // The assertDiscoveryCompleted(...) can't know if all nodes have the old master node in all of the local cluster states
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertMaster(newMasterNode, nodes);
            }
        });


        assertThat(masters.size(), equalTo(2));
        for (Map.Entry<String, List<Tuple<String, String>>> entry : masters.entrySet()) {
            String nodeName = entry.getKey();
            List<Tuple<String, String>> recordedMasterTransition = entry.getValue();
            assertThat("[" + nodeName + "] Each node should only record two master node transitions", recordedMasterTransition.size(),
                    equalTo(2));
            assertThat("[" + nodeName + "] First transition's previous master should be [null]", recordedMasterTransition.get(0).v1(),
                    equalTo(oldMasterNode));
            assertThat("[" + nodeName + "] First transition's current master should be [" + newMasterNode + "]", recordedMasterTransition
                    .get(0).v2(), nullValue());
            assertThat("[" + nodeName + "] Second transition's previous master should be [null]", recordedMasterTransition.get(1).v1(),
                    nullValue());
            assertThat("[" + nodeName + "] Second transition's current master should be [" + newMasterNode + "]",
                    recordedMasterTransition.get(1).v2(), equalTo(newMasterNode));
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

        ServiceDisruptionScheme scheme = addRandomIsolation(isolatedNode);
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

    /**
     * A 4 node cluster with m_m_n set to 3 and each node has one unicast endpoint. One node partitions from the master node.
     * The temporal unicast responses is empty. When partition is solved the one ping response contains a master node.
     * The rejoining node should take this master node and connect.
     */
    public void testUnicastSinglePingResponseContainsMaster() throws Exception {
        List<String> nodes = startCluster(4, -1, new int[]{0});
        // Figure out what is the elected master node
        final String masterNode = internalCluster().getMasterName();
        logger.info("---> legit elected master node={}", masterNode);
        List<String> otherNodes = new ArrayList<>(nodes);
        otherNodes.remove(masterNode);
        otherNodes.remove(nodes.get(0)); // <-- Don't isolate the node that is in the unicast endpoint for all the other nodes.
        final String isolatedNode = otherNodes.get(0);

        // Forcefully clean temporal response lists on all nodes. Otherwise the node in the unicast host list
        // includes all the other nodes that have pinged it and the issue doesn't manifest
        for (ZenPingService pingService : internalCluster().getInstances(ZenPingService.class)) {
            for (ZenPing zenPing : pingService.zenPings()) {
                ((UnicastZenPing) zenPing).clearTemporalResponses();
            }
        }

        // Simulate a network issue between the unlucky node and elected master node in both directions.
        NetworkDisconnectPartition networkDisconnect = new NetworkDisconnectPartition(masterNode, isolatedNode, random());
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();
        // Wait until elected master has removed that the unlucky node...
        ensureStableCluster(3, masterNode);

        // The isolate master node must report no master, so it starts with pinging
        assertNoMaster(isolatedNode);
        networkDisconnect.stopDisrupting();
        // Wait until the master node sees all 4 nodes again.
        ensureStableCluster(4);
        // The elected master shouldn't have changed, since the isolated node never could have elected himself as
        // master since m_m_n of 3 could never be satisfied.
        assertMaster(masterNode, nodes);
    }

    public void testIsolatedUnicastNodes() throws Exception {
        List<String> nodes = startCluster(4, -1, new int[]{0});
        // Figure out what is the elected master node
        final String unicastTarget = nodes.get(0);

        Set<String> unicastTargetSide = new HashSet<>();
        unicastTargetSide.add(unicastTarget);

        Set<String> restOfClusterSide = new HashSet<>();
        restOfClusterSide.addAll(nodes);
        restOfClusterSide.remove(unicastTarget);

        // Forcefully clean temporal response lists on all nodes. Otherwise the node in the unicast host list
        // includes all the other nodes that have pinged it and the issue doesn't manifest
        for (ZenPingService pingService : internalCluster().getInstances(ZenPingService.class)) {
            for (ZenPing zenPing : pingService.zenPings()) {
                ((UnicastZenPing) zenPing).clearTemporalResponses();
            }
        }

        // Simulate a network issue between the unicast target node and the rest of the cluster
        NetworkDisconnectPartition networkDisconnect = new NetworkDisconnectPartition(unicastTargetSide, restOfClusterSide, random());
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();
        // Wait until elected master has removed that the unlucky node...
        ensureStableCluster(3, nodes.get(1));

        // The isolate master node must report no master, so it starts with pinging
        assertNoMaster(unicastTarget);
        networkDisconnect.stopDisrupting();
        // Wait until the master node sees all 3 nodes again.
        ensureStableCluster(4);
    }


    /**
     * Test cluster join with issues in cluster state publishing *
     */
    public void testClusterJoinDespiteOfPublishingIssues() throws Exception {
        List<String> nodes = startCluster(2, 1);

        String masterNode = internalCluster().getMasterName();
        String nonMasterNode;
        if (masterNode.equals(nodes.get(0))) {
            nonMasterNode = nodes.get(1);
        } else {
            nonMasterNode = nodes.get(0);
        }

        DiscoveryNodes discoveryNodes = internalCluster().getInstance(ClusterService.class, nonMasterNode).state().nodes();

        TransportService masterTranspotService =
                internalCluster().getInstance(TransportService.class, discoveryNodes.getMasterNode().getName());

        logger.info("blocking requests from non master [{}] to master [{}]", nonMasterNode, masterNode);
        MockTransportService nonMasterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class,
                nonMasterNode);
        nonMasterTransportService.addFailToSendNoConnectRule(masterTranspotService);

        assertNoMaster(nonMasterNode);

        logger.info("blocking cluster state publishing from master [{}] to non master [{}]", masterNode, nonMasterNode);
        MockTransportService masterTransportService =
            (MockTransportService) internalCluster().getInstance(TransportService.class, masterNode);
        TransportService localTransportService =
            internalCluster().getInstance(TransportService.class, discoveryNodes.getLocalNode().getName());
        if (randomBoolean()) {
            masterTransportService.addFailToSendNoConnectRule(localTransportService, PublishClusterStateAction.SEND_ACTION_NAME);
        } else {
            masterTransportService.addFailToSendNoConnectRule(localTransportService, PublishClusterStateAction.COMMIT_ACTION_NAME);
        }

        logger.info("allowing requests from non master [{}] to master [{}], waiting for two join request", nonMasterNode, masterNode);
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        nonMasterTransportService.addDelegate(masterTranspotService, new MockTransportService.DelegateTransport(nonMasterTransportService
                .original()) {
            @Override
            public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions
                    options) throws IOException, TransportException {
                if (action.equals(MembershipAction.DISCOVERY_JOIN_ACTION_NAME)) {
                    countDownLatch.countDown();
                }
                super.sendRequest(node, requestId, action, request, options);
            }
        });

        countDownLatch.await();

        logger.info("waiting for cluster to reform");
        masterTransportService.clearRule(localTransportService);
        nonMasterTransportService.clearRule(localTransportService);

        ensureStableCluster(2);

        // shutting down the nodes, to avoid the leakage check tripping
        // on the states associated with the commit requests we may have dropped
        internalCluster().stopRandomNonMasterNode();
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
        NetworkPartition networkPartition = addRandomIsolation(isolatedNode);
        networkPartition.startDisrupting();

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
        networkPartition.removeAndEnsureHealthy(internalCluster());

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

    public void testClusterFormingWithASlowNode() throws Exception {
        configureUnicastCluster(3, null, 2);

        SlowClusterStateProcessing disruption = new SlowClusterStateProcessing(random(), 0, 0, 1000, 2000);

        // don't wait for initial state, wat want to add the disruption while the cluster is forming..
        internalCluster().startNodesAsync(3,
                Settings.builder()
                        .put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "1ms")
                        .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "3s")
                        .build()).get();

        logger.info("applying disruption while cluster is forming ...");

        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        ensureStableCluster(3);
    }

    /**
     * Adds an asymmetric break between a master and one of the nodes and makes
     * sure that the node is removed form the cluster, that the node start pinging and that
     * the cluster reforms when healed.
     */
    public void testNodeNotReachableFromMaster() throws Exception {
        startCluster(3);

        String masterNode = internalCluster().getMasterName();
        String nonMasterNode = null;
        while (nonMasterNode == null) {
            nonMasterNode = randomFrom(internalCluster().getNodeNames());
            if (nonMasterNode.equals(masterNode)) {
                nonMasterNode = null;
            }
        }

        logger.info("blocking request from master [{}] to [{}]", masterNode, nonMasterNode);
        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class,
                masterNode);
        if (randomBoolean()) {
            masterTransportService.addUnresponsiveRule(internalCluster().getInstance(TransportService.class, nonMasterNode));
        } else {
            masterTransportService.addFailToSendNoConnectRule(internalCluster().getInstance(TransportService.class, nonMasterNode));
        }

        logger.info("waiting for [{}] to be removed from cluster", nonMasterNode);
        ensureStableCluster(2, masterNode);

        logger.info("waiting for [{}] to have no master", nonMasterNode);
        assertNoMaster(nonMasterNode);

        logger.info("healing partition and checking cluster reforms");
        masterTransportService.clearAllRules();

        ensureStableCluster(3);
    }

    /**
     * This test creates a scenario where a primary shard (0 replicas) relocates and is in POST_RECOVERY on the target
     * node but already deleted on the source node. Search request should still work.
     */
    public void testSearchWithRelocationAndSlowClusterStateProcessing() throws Exception {
        configureUnicastCluster(3, null, 1);
        InternalTestCluster.Async<String> masterNodeFuture = internalCluster().startMasterOnlyNodeAsync();
        InternalTestCluster.Async<String> node_1Future = internalCluster().startDataOnlyNodeAsync();

        final String node_1 = node_1Future.get();
        final String masterNode = masterNodeFuture.get();
        logger.info("--> creating index [test] with one shard and on replica");
        assertAcked(prepareCreate("test").setSettings(
                Settings.builder().put(indexSettings())
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
        );
        ensureGreen("test");

        InternalTestCluster.Async<String> node_2Future = internalCluster().startDataOnlyNodeAsync();
        final String node_2 = node_2Future.get();
        List<IndexRequestBuilder> indexRequestBuilderList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            indexRequestBuilderList.add(client().prepareIndex().setIndex("test").setType("doc").setSource("{\"int_field\":1}"));
        }
        indexRandom(true, indexRequestBuilderList);
        SingleNodeDisruption disruption = new BlockClusterStateProcessing(node_2, random());

        internalCluster().setDisruptionScheme(disruption);
        MockTransportService transportServiceNode2 = (MockTransportService) internalCluster().getInstance(TransportService.class, node_2);
        CountDownLatch beginRelocationLatch = new CountDownLatch(1);
        CountDownLatch endRelocationLatch = new CountDownLatch(1);
        transportServiceNode2.addTracer(new IndicesStoreIntegrationIT.ReclocationStartEndTracer(logger, beginRelocationLatch,
                endRelocationLatch));
        internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, node_1, node_2)).get();
        // wait for relocation to start
        beginRelocationLatch.await();
        disruption.startDisrupting();
        // wait for relocation to finish
        endRelocationLatch.await();
        // now search for the documents and see if we get a reply
        assertThat(client().prepareSearch().setSize(0).get().getHits().totalHits(), equalTo(100L));
    }

    public void testIndexImportedFromDataOnlyNodesIfMasterLostDataFolder() throws Exception {
        // test for https://github.com/elastic/elasticsearch/issues/8823
        configureUnicastCluster(2, null, 1);
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
        configureUnicastCluster(settings, 3, null, 2);
        InternalTestCluster.Async<List<String>> masterNodes = internalCluster().startMasterOnlyNodesAsync(2);
        InternalTestCluster.Async<String> dataNode = internalCluster().startDataOnlyNodeAsync();
        dataNode.get();
        final List<String> allMasterEligibleNodes = masterNodes.get();
        ensureStableCluster(3);
        assertAcked(prepareCreate("test"));

        final String masterNode1 = internalCluster().getMasterName();
        NetworkPartition networkPartition = new NetworkUnresponsivePartition(masterNode1, dataNode.get(), random());
        internalCluster().setDisruptionScheme(networkPartition);
        networkPartition.startDisrupting();
        // We know this will time out due to the partition, we check manually below to not proceed until
        // the delete has been applied to the master node and the master eligible node.
        internalCluster().client(masterNode1).admin().indices().prepareDelete(idxName).setTimeout("0s").get();
        // Don't restart the master node until we know the index deletion has taken effect on master and the master eligible node.
        assertBusy(() -> {
            for (String masterNode : allMasterEligibleNodes) {
                final ClusterState masterState = internalCluster().clusterService(masterNode).state();
                assertTrue("index not deleted on " + masterNode, masterState.metaData().hasIndex(idxName) == false &&
                                                                 masterState.status() == ClusterState.ClusterStateStatus.APPLIED);
            }
        });
        internalCluster().restartNode(masterNode1, InternalTestCluster.EMPTY_CALLBACK);
        ensureYellow();
        assertFalse(client().admin().indices().prepareExists(idxName).get().isExists());
    }

    protected NetworkPartition addRandomPartition() {
        NetworkPartition partition;
        if (randomBoolean()) {
            partition = new NetworkUnresponsivePartition(random());
        } else {
            partition = new NetworkDisconnectPartition(random());
        }

        setDisruptionScheme(partition);

        return partition;
    }

    protected NetworkPartition addRandomIsolation(String isolatedNode) {
        Set<String> side1 = new HashSet<>();
        Set<String> side2 = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
        side1.add(isolatedNode);
        side2.remove(isolatedNode);

        NetworkPartition partition;
        if (randomBoolean()) {
            partition = new NetworkUnresponsivePartition(side1, side2, random());
        } else {
            partition = new NetworkDisconnectPartition(side1, side2, random());
        }

        internalCluster().setDisruptionScheme(partition);

        return partition;
    }

    private ServiceDisruptionScheme addRandomDisruptionScheme() {
        // TODO: add partial partitions
        List<ServiceDisruptionScheme> list = Arrays.asList(
                new NetworkUnresponsivePartition(random()),
                new NetworkDelaysPartition(random()),
                new NetworkDisconnectPartition(random()),
                new SlowClusterStateProcessing(random()),
                new BridgePartition(random(), randomBoolean())
        );
        Collections.shuffle(list, random());
        setDisruptionScheme(list.get(0));
        return list.get(0);
    }

    private ClusterState getNodeClusterState(String node) {
        return client(node).admin().cluster().prepareState().setLocal(true).get().getState();
    }

    private void assertNoMaster(final String node) throws Exception {
        assertNoMaster(node, null, TimeValue.timeValueSeconds(10));
    }

    private void assertNoMaster(final String node, TimeValue maxWaitTime) throws Exception {
        assertNoMaster(node, null, maxWaitTime);
    }

    private void assertNoMaster(final String node, @Nullable final ClusterBlock expectedBlocks, TimeValue maxWaitTime) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState state = getNodeClusterState(node);
                assertNull("node [" + node + "] still has [" + state.nodes().getMasterNode() + "] as master", state.nodes().getMasterNode());
                if (expectedBlocks != null) {
                    for (ClusterBlockLevel level : expectedBlocks.levels()) {
                        assertTrue("node [" + node + "] does have level [" + level + "] in it's blocks", state.getBlocks().hasGlobalBlock
                                (level));
                    }
                }
            }
        }, maxWaitTime.getMillis(), TimeUnit.MILLISECONDS);
    }

    private void assertDifferentMaster(final String node, final String oldMasterNode) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterState state = getNodeClusterState(node);
                String masterNode = null;
                if (state.nodes().getMasterNode() != null) {
                    masterNode = state.nodes().getMasterNode().getName();
                }
                logger.trace("[{}] master is [{}]", node, state.nodes().getMasterNode());
                assertThat("node [" + node + "] still has [" + masterNode + "] as master",
                        oldMasterNode, not(equalTo(masterNode)));
            }
        }, 10, TimeUnit.SECONDS);
    }

    private void assertMaster(String masterNode, List<String> nodes) {
        for (String node : nodes) {
            ClusterState state = getNodeClusterState(node);
            String failMsgSuffix = "cluster_state:\n" + state.prettyPrint();
            assertThat("wrong node count on [" + node + "]. " + failMsgSuffix, state.nodes().getSize(), equalTo(nodes.size()));
            String otherMasterNodeName = state.nodes().getMasterNode() != null ? state.nodes().getMasterNode().getName() : null;
            assertThat("wrong master on node [" + node + "]. " + failMsgSuffix, otherMasterNodeName, equalTo(masterNode));
        }
    }

    private void assertDiscoveryCompleted(List<String> nodes) throws InterruptedException {
        for (final String node : nodes) {
            assertTrue(
                    "node [" + node + "] is still joining master",
                    awaitBusy(
                            () -> !((ZenDiscovery) internalCluster().getInstance(Discovery.class, node)).joiningCluster(),
                            30,
                            TimeUnit.SECONDS
                    )
            );
        }
    }
}
