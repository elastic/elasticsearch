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

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.DjbHashFunction;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
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
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoverySource;
import org.elasticsearch.indices.store.IndicesStoreIntegrationIT;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.discovery.ClusterDiscoveryConfiguration;
import org.elasticsearch.test.disruption.*;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
@ESIntegTestCase.SuppressLocalMode
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

    private List<String> startCluster(int numberOfNodes) throws ExecutionException, InterruptedException {
        return startCluster(numberOfNodes, -1);
    }

    private List<String> startCluster(int numberOfNodes, int minimumMasterNode) throws ExecutionException, InterruptedException {
        return startCluster(numberOfNodes, minimumMasterNode, null);
    }

    private List<String> startCluster(int numberOfNodes, int minimumMasterNode, @Nullable int[] unicastHostsOrdinals) throws ExecutionException, InterruptedException {
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

    final static Settings DEFAULT_SETTINGS = Settings.builder()
            .put(FaultDetection.SETTING_PING_TIMEOUT, "1s") // for hitting simulated network failures quickly
            .put(FaultDetection.SETTING_PING_RETRIES, "1") // for hitting simulated network failures quickly
            .put("discovery.zen.join_timeout", "10s")  // still long to induce failures but to long so test won't time out
            .put(DiscoverySettings.PUBLISH_TIMEOUT, "1s") // <-- for hitting simulated network failures quickly
            .put("http.enabled", false) // just to make test quicker
            .put("transport.host", "127.0.0.1") // only bind on one IF we use v4 here by default
            .put("transport.bind_host", "127.0.0.1")
            .put("transport.publish_host", "127.0.0.1")
            .put("gateway.local.list_timeout", "10s") // still long to induce failures but to long so test won't time out
            .build();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(MockTransportService.TestPlugin.class);
    }

    private void configureUnicastCluster(int numberOfNodes, @Nullable int[] unicastHostsOrdinals, int minimumMasterNode) throws ExecutionException, InterruptedException {
        if (minimumMasterNode < 0) {
            minimumMasterNode = numberOfNodes / 2 + 1;
        }
        logger.info("---> configured unicast");
        // TODO: Rarely use default settings form some of these
        Settings nodeSettings = Settings.builder()
                .put(DEFAULT_SETTINGS)
                .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, minimumMasterNode)
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
     * Test that no split brain occurs under partial network partition. See https://github.com/elasticsearch/elasticsearch/issues/2488
     *
     * @throws Exception
     */
    @Test
    public void failWithMinimumMasterNodesConfigured() throws Exception {

        List<String> nodes = startCluster(3);

        // Figure out what is the elected master node
        final String masterNode = internalCluster().getMasterName();
        logger.info("---> legit elected master node=" + masterNode);

        // Pick a node that isn't the elected master.
        Set<String> nonMasters = new HashSet<>(nodes);
        nonMasters.remove(masterNode);
        final String unluckyNode = randomFrom(nonMasters.toArray(Strings.EMPTY_ARRAY));


        // Simulate a network issue between the unlucky node and elected master node in both directions.

        NetworkDisconnectPartition networkDisconnect = new NetworkDisconnectPartition(masterNode, unluckyNode, getRandom());
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
    @Test
    public void testNodesFDAfterMasterReelection() throws Exception {
        startCluster(4);

        logger.info("--> stopping current master");
        internalCluster().stopCurrentMasterNode();

        ensureStableCluster(3);

        logger.info("--> reducing min master nodes to 2");
        assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 2)).get());

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
    @Test
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

        logger.info("Verify no master block with {} set to {}", DiscoverySettings.NO_MASTER_BLOCK, "all");
        client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(DiscoverySettings.NO_MASTER_BLOCK, "all"))
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
    @Test
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
            ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + networkPartition.expectedTimeToHeal().millis()), true, node);
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
                assertEquals("unequal node count", state.nodes().size(), nodeState.nodes().size());
                assertEquals("different masters ", state.nodes().masterNodeId(), nodeState.nodes().masterNodeId());
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
     * We also collect & report the type of indexing failures that occur.
     * <p/>
     * This test is a superset of tests run in the Jepsen test suite, with the exception of versioned updates
     */
    @Test
    // NOTE: if you remove the awaitFix, make sure to port the test to the 1.x branch
    @LuceneTestCase.AwaitsFix(bugUrl = "needs some more work to stabilize")
    @TestLogging("action.index:TRACE,action.get:TRACE,discovery:TRACE,cluster.service:TRACE,indices.recovery:TRACE,indices.cluster:TRACE")
    public void testAckedIndexing() throws Exception {
        // TODO: add node count randomizaion
        final List<String> nodes = startCluster(3);

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
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (!stop.get()) {
                            String id = null;
                            try {
                                if (!semaphore.tryAcquire(10, TimeUnit.SECONDS)) {
                                    continue;
                                }
                                logger.info("[{}] Acquired semaphore and it has {} permits left", name, semaphore.availablePermits());
                                try {
                                    id = Integer.toString(idGenerator.incrementAndGet());
                                    int shard = ((InternalTestCluster) cluster()).getInstance(DjbHashFunction.class).hash(id) % numPrimaries;
                                    logger.trace("[{}] indexing id [{}] through node [{}] targeting shard [{}]", name, id, node, shard);
                                    IndexResponse response = client.prepareIndex("test", "type", id).setSource("{}").setTimeout("1s").get();
                                    assertThat(response.getVersion(), equalTo(1l));
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
                            } catch (Throwable t) {
                                logger.info("unexpected exception in background thread of [{}]", t, node);
                            }
                        }
                    }
                });

                thread.setName(name);
                thread.setDaemon(true);
                thread.start();
                indexers.add(thread);
            }

            int docsPerIndexer = randomInt(3);
            logger.info("indexing " + docsPerIndexer + " docs per indexer before partition");
            countDownLatchRef.set(new CountDownLatch(docsPerIndexer * indexers.size()));
            for (Semaphore semaphore : semaphores) {
                semaphore.release(docsPerIndexer);
            }
            assertTrue(countDownLatchRef.get().await(1, TimeUnit.MINUTES));

            for (int iter = 1 + randomInt(2); iter > 0; iter--) {
                logger.info("starting disruptions & indexing (iteration [{}])", iter);
                disruptionScheme.startDisrupting();

                docsPerIndexer = 1 + randomInt(5);
                logger.info("indexing " + docsPerIndexer + " docs per indexer during partition");
                countDownLatchRef.set(new CountDownLatch(docsPerIndexer * indexers.size()));
                Collections.shuffle(semaphores);
                for (Semaphore semaphore : semaphores) {
                    assertThat(semaphore.availablePermits(), equalTo(0));
                    semaphore.release(docsPerIndexer);
                }
                assertTrue(countDownLatchRef.get().await(60000 + disruptionScheme.expectedTimeToHeal().millis() * (docsPerIndexer * indexers.size()), TimeUnit.MILLISECONDS));

                logger.info("stopping disruption");
                disruptionScheme.stopDisrupting();
                ensureStableCluster(3, TimeValue.timeValueMillis(disruptionScheme.expectedTimeToHeal().millis() + DISRUPTION_HEALING_OVERHEAD.millis()));
                ensureGreen("test");

                logger.info("validating successful docs");
                for (String node : nodes) {
                    try {
                        logger.debug("validating through node [{}]", node);
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
                StringBuilder sb = new StringBuilder("Indexing exceptions during disruption:");
                for (Exception e : exceptedExceptions) {
                    sb.append("\n").append(e.getMessage());
                }
                logger.debug(sb.toString());
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
    @Test
    public void testMasterNodeGCs() throws Exception {
        List<String> nodes = startCluster(3, -1);

        String oldMasterNode = internalCluster().getMasterName();
        // a very long GC, but it's OK as we remove the disruption when it has had an effect
        SingleNodeDisruption masterNodeDisruption = new IntermittentLongGCDisruption(oldMasterNode, getRandom(), 100, 200, 30000, 60000);
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
        ensureStableCluster(3, new TimeValue(DISRUPTION_HEALING_OVERHEAD.millis() + masterNodeDisruption.expectedTimeToHeal().millis()), false,
                oldNonMasterNodes.get(0));

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
    @Test
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
        SingleNodeDisruption masterNodeDisruption = new LongGCDisruption(getRandom(), oldMasterNode);

        // Save the majority side
        final List<String> majoritySide = new ArrayList<>(nodes);
        majoritySide.remove(oldMasterNode);

        // Keeps track of the previous and current master when a master node transition took place on each node on the majority side:
        final Map<String, List<Tuple<String, String>>> masters = Collections.synchronizedMap(new HashMap<String, List<Tuple<String, String>>>());
        for (final String node : majoritySide) {
            masters.put(node, new ArrayList<Tuple<String, String>>());
            internalCluster().getInstance(ClusterService.class, node).add(new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
                    DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
                    if (!Objects.equals(previousMaster, currentMaster)) {
                        logger.info("node {} received new cluster state: {} \n and had previous cluster state: {}", node, event.state(), event.previousState());
                        String previousMasterNodeName = previousMaster != null ? previousMaster.name() : null;
                        String currentMasterNodeName = currentMaster != null ? currentMaster.name() : null;
                        masters.get(node).add(new Tuple<>(previousMasterNodeName, currentMasterNodeName));
                    }
                }
            });
        }

        final CountDownLatch oldMasterNodeSteppedDown = new CountDownLatch(1);
        internalCluster().getInstance(ClusterService.class, oldMasterNode).add(new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().nodes().masterNodeId() == null) {
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
        internalCluster().getInstance(ClusterService.class, oldMasterNode).submitStateUpdateTask("sneaky-update", Priority.IMMEDIATE, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState).build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.warn("failure [{}]", t, source);
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
            assertThat("[" + nodeName + "] Each node should only record two master node transitions", recordedMasterTransition.size(), equalTo(2));
            assertThat("[" + nodeName + "] First transition's previous master should be [null]", recordedMasterTransition.get(0).v1(), equalTo(oldMasterNode));
            assertThat("[" + nodeName + "] First transition's current master should be [" + newMasterNode + "]", recordedMasterTransition.get(0).v2(), nullValue());
            assertThat("[" + nodeName + "] Second transition's previous master should be [null]", recordedMasterTransition.get(1).v1(), nullValue());
            assertThat("[" + nodeName + "] Second transition's current master should be [" + newMasterNode + "]", recordedMasterTransition.get(1).v2(), equalTo(newMasterNode));
        }
    }

    /**
     * Test that a document which is indexed on the majority side of a partition, is available from the minority side,
     * once the partition is healed
     *
     * @throws Exception
     */
    @Test
    @TestLogging(value = "cluster.service:TRACE")
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
        Collections.shuffle(nodes, getRandom());
        String isolatedNode = nodes.get(0);
        String notIsolatedNode = nodes.get(1);

        ServiceDisruptionScheme scheme = addRandomIsolation(isolatedNode);
        scheme.startDisrupting();
        ensureStableCluster(2, notIsolatedNode);
        assertFalse(client(notIsolatedNode).admin().cluster().prepareHealth("test").setWaitForYellowStatus().get().isTimedOut());


        IndexResponse indexResponse = internalCluster().client(notIsolatedNode).prepareIndex("test", "type").setSource("field", "value").get();
        assertThat(indexResponse.getVersion(), equalTo(1l));

        logger.info("Verifying if document exists via node[" + notIsolatedNode + "]");
        GetResponse getResponse = internalCluster().client(notIsolatedNode).prepareGet("test", "type", indexResponse.getId())
                .setPreference("_local")
                .get();
        assertThat(getResponse.isExists(), is(true));
        assertThat(getResponse.getVersion(), equalTo(1l));
        assertThat(getResponse.getId(), equalTo(indexResponse.getId()));

        scheme.stopDisrupting();

        ensureStableCluster(3);
        ensureGreen("test");

        for (String node : nodes) {
            logger.info("Verifying if document exists after isolating node[" + isolatedNode + "] via node[" + node + "]");
            getResponse = internalCluster().client(node).prepareGet("test", "type", indexResponse.getId())
                    .setPreference("_local")
                    .get();
            assertThat(getResponse.isExists(), is(true));
            assertThat(getResponse.getVersion(), equalTo(1l));
            assertThat(getResponse.getId(), equalTo(indexResponse.getId()));
        }
    }

    /**
     * A 4 node cluster with m_m_n set to 3 and each node has one unicast enpoint. One node partitions from the master node.
     * The temporal unicast responses is empty. When partition is solved the one ping response contains a master node.
     * The rejoining node should take this master node and connect.
     */
    @Test
    public void unicastSinglePingResponseContainsMaster() throws Exception {
        List<String> nodes = startCluster(4, -1, new int[]{0});
        // Figure out what is the elected master node
        final String masterNode = internalCluster().getMasterName();
        logger.info("---> legit elected master node=" + masterNode);
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
        NetworkDisconnectPartition networkDisconnect = new NetworkDisconnectPartition(masterNode, isolatedNode, getRandom());
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

    @Test
    @TestLogging("discovery.zen:TRACE,cluster.service:TRACE")
    public void isolatedUnicastNodes() throws Exception {
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
        NetworkDisconnectPartition networkDisconnect = new NetworkDisconnectPartition(unicastTargetSide, restOfClusterSide, getRandom());
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
    @Test
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

        logger.info("blocking requests from non master [{}] to master [{}]", nonMasterNode, masterNode);
        MockTransportService nonMasterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, nonMasterNode);
        nonMasterTransportService.addFailToSendNoConnectRule(discoveryNodes.masterNode());

        assertNoMaster(nonMasterNode);

        logger.info("blocking cluster state publishing from master [{}] to non master [{}]", masterNode, nonMasterNode);
        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, masterNode);
        if (randomBoolean()) {
            masterTransportService.addFailToSendNoConnectRule(discoveryNodes.localNode(), PublishClusterStateAction.SEND_ACTION_NAME);
        } else {
            masterTransportService.addFailToSendNoConnectRule(discoveryNodes.localNode(), PublishClusterStateAction.COMMIT_ACTION_NAME);
        }

        logger.info("allowing requests from non master [{}] to master [{}], waiting for two join request", nonMasterNode, masterNode);
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        nonMasterTransportService.addDelegate(discoveryNodes.masterNode(), new MockTransportService.DelegateTransport(nonMasterTransportService.original()) {
            @Override
            public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
                if (action.equals(MembershipAction.DISCOVERY_JOIN_ACTION_NAME)) {
                    countDownLatch.countDown();
                }
                super.sendRequest(node, requestId, action, request, options);
            }
        });

        countDownLatch.await();

        logger.info("waiting for cluster to reform");
        masterTransportService.clearRule(discoveryNodes.localNode());
        nonMasterTransportService.clearRule(discoveryNodes.masterNode());

        ensureStableCluster(2);

        // shutting down the nodes, to avoid the leakage check tripping
        // on the states associated with the commit requests we may have dropped
        internalCluster().stopRandomNonMasterNode();
    }


    @Test
    public void testClusterFormingWithASlowNode() throws Exception {
        configureUnicastCluster(3, null, 2);

        SlowClusterStateProcessing disruption = new SlowClusterStateProcessing(getRandom(), 0, 0, 1000, 2000);

        // don't wait for initial state, wat want to add the disruption while the cluster is forming..
        internalCluster().startNodesAsync(3,
                Settings.builder()
                        .put(DiscoveryService.SETTING_INITIAL_STATE_TIMEOUT, "1ms")
                        .put(DiscoverySettings.PUBLISH_TIMEOUT, "3s")
                        .build()).get();

        logger.info("applying disruption while cluster is forming ...");

        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        ensureStableCluster(3);
    }

    /**
     * Adds an asymetric break between a master and one of the nodes and makes
     * sure that the node is removed form the cluster, that the node start pinging and that
     * the cluster reforms when healed.
     */
    @Test
    @TestLogging("discovery.zen:TRACE,action:TRACE")
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
        MockTransportService masterTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, masterNode);
        if (randomBoolean()) {
            masterTransportService.addUnresponsiveRule(internalCluster().getInstance(ClusterService.class, nonMasterNode).localNode());
        } else {
            masterTransportService.addFailToSendNoConnectRule(internalCluster().getInstance(ClusterService.class, nonMasterNode).localNode());
        }

        logger.info("waiting for [{}] to be removed from cluster", nonMasterNode);
        ensureStableCluster(2, masterNode);

        logger.info("waiting for [{}] to have no master", nonMasterNode);
        assertNoMaster(nonMasterNode);

        logger.info("healing partition and checking cluster reforms");
        masterTransportService.clearAllRules();

        ensureStableCluster(3);
    }

    /*
     * Tests a visibility issue if a shard is in POST_RECOVERY
     *
     * When a user indexes a document, then refreshes and then a executes a search and all are successful and no timeouts etc then
     * the document must be visible for the search.
     *
     * When a primary is relocating from node_1 to node_2, there can be a short time where both old and new primary
     * are started and accept indexing and read requests. However, the new primary might not be visible to nodes
     * that lag behind one cluster state. If such a node then sends a refresh to the index, this refresh request
     * must reach the new primary on node_2 too. Otherwise a different node that searches on the new primary might not
     * find the indexed document although a refresh was executed before.
     *
     * In detail:
     * Cluster state 0:
     * node_1: [index][0] STARTED   (ShardRoutingState)
     * node_2: no shard
     *
     * 0. primary ([index][0]) relocates from node_1 to node_2
     * Cluster state 1:
     * node_1: [index][0] RELOCATING   (ShardRoutingState), (STARTED from IndexShardState perspective on node_1)
     * node_2: [index][0] INITIALIZING (ShardRoutingState), (IndexShardState on node_2 is RECOVERING)
     *
     * 1. node_2 is done recovering, moves its shard to IndexShardState.POST_RECOVERY and sends a message to master that the shard is ShardRoutingState.STARTED
     * Cluster state is still the same but the IndexShardState on node_2 has changed and it now accepts writes and reads:
     * node_1: [index][0] RELOCATING   (ShardRoutingState), (STARTED from IndexShardState perspective on node_1)
     * node_2: [index][0] INITIALIZING (ShardRoutingState), (IndexShardState on node_2 is POST_RECOVERY)
     *
     * 2. any node receives an index request which is then executed on node_1 and node_2
     *
     * 3. node_3 sends a refresh but it is a little behind with cluster state processing and still on cluster state 0.
     * If refresh was a broadcast operation it send it to node_1 only because it does not know node_2 has a shard too
     *
     * 4. node_3 catches up with the cluster state and acks it to master which now can process the shard started message
     *  from node_2 before and updates cluster state to:
     * Cluster state 2:
     * node_1: [index][0] no shard
     * node_2: [index][0] STARTED (ShardRoutingState), (IndexShardState on node_2 is still POST_RECOVERY)
     *
     * master sends this to all nodes.
     *
     * 5. node_4 and node_3 process cluster state 2, but node_1 and node_2 have not yet
     *
     * If now node_4 searches for document that was indexed before, it will search at node_2 because it is on
     * cluster state 2. It should be able to retrieve it with a search because the refresh from before was
     * successful.
     */
    @Test
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/13316")
    public void testReadOnPostRecoveryShards() throws Exception {
        List<BlockClusterStateProcessing> clusterStateBlocks = new ArrayList<>();
        try {
            configureUnicastCluster(5, null, 1);
            // we could probably write a test without a dedicated master node but it is easier if we use one
            InternalTestCluster.Async<String> masterNodeFuture = internalCluster().startMasterOnlyNodeAsync();
            // node_1 will have the shard in the beginning
            InternalTestCluster.Async<String> node1Future = internalCluster().startDataOnlyNodeAsync();
            final String masterNode = masterNodeFuture.get();
            final String node_1 = node1Future.get();
            logger.info("--> creating index [test] with one shard and zero replica");
            assertAcked(prepareCreate("test").setSettings(
                            Settings.builder().put(indexSettings())
                                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                                    .put(IndexShard.INDEX_REFRESH_INTERVAL, -1))
                            .addMapping("doc", jsonBuilder().startObject().startObject("doc")
                                    .startObject("properties").startObject("text").field("type", "string").endObject().endObject()
                                    .endObject().endObject())
            );
            ensureGreen("test");
            logger.info("--> starting three more data nodes");
            List<String> nodeNamesFuture = internalCluster().startDataOnlyNodesAsync(3).get();
            final String node_2 = nodeNamesFuture.get(0);
            final String node_3 = nodeNamesFuture.get(1);
            final String node_4 = nodeNamesFuture.get(2);
            logger.info("--> running cluster_health");
            ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth()
                    .setWaitForNodes("5")
                    .setWaitForRelocatingShards(0)
                    .get();
            assertThat(clusterHealth.isTimedOut(), equalTo(false));

            logger.info("--> move shard from node_1 to node_2, and wait for relocation to finish");

            // block cluster state updates on node_3 so that it only sees the shard on node_1
            BlockClusterStateProcessing disruptionNode3 = new BlockClusterStateProcessing(node_3, getRandom());
            clusterStateBlocks.add(disruptionNode3);
            internalCluster().setDisruptionScheme(disruptionNode3);
            disruptionNode3.startDisrupting();
            // register a Tracer that notifies begin and end of a relocation
            MockTransportService transportServiceNode2 = (MockTransportService) internalCluster().getInstance(TransportService.class, node_2);
            CountDownLatch beginRelocationLatchNode2 = new CountDownLatch(1);
            CountDownLatch endRelocationLatchNode2 = new CountDownLatch(1);
            transportServiceNode2.addTracer(new StartRecoveryToShardStaredTracer(logger, beginRelocationLatchNode2, endRelocationLatchNode2));

            // block cluster state updates on node_1 and node_2 so that we end up with two primaries
            BlockClusterStateProcessing disruptionNode2 = new BlockClusterStateProcessing(node_2, getRandom());
            clusterStateBlocks.add(disruptionNode2);
            disruptionNode2.applyToCluster(internalCluster());
            BlockClusterStateProcessing disruptionNode1 = new BlockClusterStateProcessing(node_1, getRandom());
            clusterStateBlocks.add(disruptionNode1);
            disruptionNode1.applyToCluster(internalCluster());

            logger.info("--> move shard from node_1 to node_2");
            // don't block on the relocation. cluster state updates are blocked on node_3 and the relocation would timeout
            Future<ClusterRerouteResponse> rerouteFuture = internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(new ShardId("test", 0), node_1, node_2)).setTimeout(new TimeValue(1000, TimeUnit.MILLISECONDS)).execute();

            logger.info("--> wait for relocation to start");
            // wait for relocation to start
            beginRelocationLatchNode2.await();
            // start to block cluster state updates on node_1 and node_2 so that we end up with two primaries
            // one STARTED on node_1 and one in POST_RECOVERY on node_2
            disruptionNode1.startDisrupting();
            disruptionNode2.startDisrupting();
            endRelocationLatchNode2.await();
            final Client node3Client = internalCluster().client(node_3);
            final Client node2Client = internalCluster().client(node_2);
            final Client node1Client = internalCluster().client(node_1);
            final Client node4Client = internalCluster().client(node_4);
            logger.info("--> index doc");
            logLocalClusterStates(node1Client, node2Client, node3Client, node4Client);
            assertTrue(node3Client.prepareIndex("test", "doc").setSource("{\"text\":\"a\"}").get().isCreated());
            //sometimes refresh and sometimes flush
            int refreshOrFlushType = randomIntBetween(1, 2);
            switch (refreshOrFlushType) {
                case 1: {
                    logger.info("--> refresh from node_3");
                    RefreshResponse refreshResponse = node3Client.admin().indices().prepareRefresh().get();
                    assertThat(refreshResponse.getFailedShards(), equalTo(0));
                    // the total shards is num replicas + 1 so that can be lower here because one shard
                    // is relocating and counts twice as successful
                    assertThat(refreshResponse.getTotalShards(), equalTo(2));
                    assertThat(refreshResponse.getSuccessfulShards(), equalTo(2));
                    break;
                }
                case 2: {
                    logger.info("--> flush from node_3");
                    FlushResponse flushResponse = node3Client.admin().indices().prepareFlush().get();
                    assertThat(flushResponse.getFailedShards(), equalTo(0));
                    // the total shards is num replicas + 1 so that can be lower here because one shard
                    // is relocating and counts twice as successful
                    assertThat(flushResponse.getTotalShards(), equalTo(2));
                    assertThat(flushResponse.getSuccessfulShards(), equalTo(2));
                    break;
                }
                default:
                    fail("this is  test bug, number should be between 1 and 2");
            }
            // now stop disrupting so that node_3 can ack last cluster state to master and master can continue
            // to publish the next cluster state
            logger.info("--> stop disrupting node_3");
            disruptionNode3.stopDisrupting();
            rerouteFuture.get();
            logger.info("--> wait for node_4 to get new cluster state");
            // wait until node_4 actually has the new cluster state in which node_1 has no shard
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    ClusterState clusterState = node4Client.admin().cluster().prepareState().setLocal(true).get().getState();
                    // get the node id from the name. TODO: Is there a better way to do this?
                    String nodeId = null;
                    for (RoutingNode node : clusterState.getRoutingNodes()) {
                        if (node.node().name().equals(node_1)) {
                            nodeId = node.nodeId();
                        }
                    }
                    assertNotNull(nodeId);
                    // check that node_1 does not have the shard in local cluster state
                    assertFalse(clusterState.getRoutingNodes().routingNodeIter(nodeId).hasNext());
                }
            });

            logger.info("--> run count from node_4");
            logLocalClusterStates(node1Client, node2Client, node3Client, node4Client);
            CountResponse countResponse = node4Client.prepareCount("test").setPreference("local").get();
            assertThat(countResponse.getCount(), equalTo(1l));
            logger.info("--> stop disrupting node_1 and node_2");
            disruptionNode2.stopDisrupting();
            disruptionNode1.stopDisrupting();
            // wait for relocation to finish
            logger.info("--> wait for relocation to finish");
            clusterHealth = client().admin().cluster().prepareHealth()
                    .setWaitForRelocatingShards(0)
                    .get();
            assertThat(clusterHealth.isTimedOut(), equalTo(false));
        } catch (AssertionError e) {
            for (BlockClusterStateProcessing blockClusterStateProcessing : clusterStateBlocks) {
                blockClusterStateProcessing.stopDisrupting();
            }
            throw e;
        }
    }

    /**
     * This Tracer can be used to signal start of a recovery and shard started event after translog was copied
     */
    public static class StartRecoveryToShardStaredTracer extends MockTransportService.Tracer {
        private final ESLogger logger;
        private final CountDownLatch beginRelocationLatch;
        private final CountDownLatch sentShardStartedLatch;

        public StartRecoveryToShardStaredTracer(ESLogger logger, CountDownLatch beginRelocationLatch, CountDownLatch sentShardStartedLatch) {
            this.logger = logger;
            this.beginRelocationLatch = beginRelocationLatch;
            this.sentShardStartedLatch = sentShardStartedLatch;
        }

        @Override
        public void requestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
            if (action.equals(RecoverySource.Actions.START_RECOVERY)) {
                logger.info("sent: {}, relocation starts", action);
                beginRelocationLatch.countDown();
            }
            if (action.equals(ShardStateAction.SHARD_STARTED_ACTION_NAME)) {
                logger.info("sent: {}, shard started", action);
                sentShardStartedLatch.countDown();
            }
        }
    }

    private void logLocalClusterStates(Client... clients) {
        int counter = 1;
        for (Client client : clients) {
            ClusterState clusterState = client.admin().cluster().prepareState().setLocal(true).get().getState();
            logger.info("--> cluster state on node_{} {}", counter, clusterState.prettyPrint());
            counter++;
        }
    }

    /**
     * This test creates a scenario where a primary shard (0 replicas) relocates and is in POST_RECOVERY on the target
     * node but already deleted on the source node. Search request should still work.
     */
    @Test
    public void searchWithRelocationAndSlowClusterStateProcessing() throws Exception {
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
        SingleNodeDisruption disruption = new BlockClusterStateProcessing(node_2, getRandom());

        internalCluster().setDisruptionScheme(disruption);
        MockTransportService transportServiceNode2 = (MockTransportService) internalCluster().getInstance(TransportService.class, node_2);
        CountDownLatch beginRelocationLatch = new CountDownLatch(1);
        CountDownLatch endRelocationLatch = new CountDownLatch(1);
        transportServiceNode2.addTracer(new IndicesStoreIntegrationIT.ReclocationStartEndTracer(logger, beginRelocationLatch, endRelocationLatch));
        internalCluster().client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(new ShardId("test", 0), node_1, node_2)).get();
        // wait for relocation to start
        beginRelocationLatch.await();
        disruption.startDisrupting();
        // wait for relocation to finish
        endRelocationLatch.await();
        // now search for the documents and see if we get a reply
        assertThat(client().prepareCount().get().getCount(), equalTo(100l));
    }

    @Test
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

    // tests if indices are really deleted even if a master transition inbetween
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/11665")
    @Test
    public void testIndicesDeleted() throws Exception {
        configureUnicastCluster(3, null, 2);
        InternalTestCluster.Async<List<String>> masterNodes= internalCluster().startMasterOnlyNodesAsync(2);
        InternalTestCluster.Async<String> dataNode = internalCluster().startDataOnlyNodeAsync();
        dataNode.get();
        masterNodes.get();
        ensureStableCluster(3);
        assertAcked(prepareCreate("test"));
        ensureYellow();

        String masterNode1 = internalCluster().getMasterName();
        NetworkPartition networkPartition = new NetworkUnresponsivePartition(masterNode1, dataNode.get(), getRandom());
        internalCluster().setDisruptionScheme(networkPartition);
        networkPartition.startDisrupting();
        internalCluster().client(masterNode1).admin().indices().prepareDelete("test").setTimeout("1s").get();
        internalCluster().restartNode(masterNode1, InternalTestCluster.EMPTY_CALLBACK);
        ensureYellow();
        assertFalse(client().admin().indices().prepareExists("test").get().isExists());
    }

    protected NetworkPartition addRandomPartition() {
        NetworkPartition partition;
        if (randomBoolean()) {
            partition = new NetworkUnresponsivePartition(getRandom());
        } else {
            partition = new NetworkDisconnectPartition(getRandom());
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
            partition = new NetworkUnresponsivePartition(side1, side2, getRandom());
        } else {
            partition = new NetworkDisconnectPartition(side1, side2, getRandom());
        }

        internalCluster().setDisruptionScheme(partition);

        return partition;
    }

    private ServiceDisruptionScheme addRandomDisruptionScheme() {
        // TODO: add partial partitions
        List<ServiceDisruptionScheme> list = Arrays.asList(
                new NetworkUnresponsivePartition(getRandom()),
                new NetworkDelaysPartition(getRandom()),
                new NetworkDisconnectPartition(getRandom()),
                new SlowClusterStateProcessing(getRandom())
        );
        Collections.shuffle(list);
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
                assertNull("node [" + node + "] still has [" + state.nodes().masterNode() + "] as master", state.nodes().masterNode());
                if (expectedBlocks != null) {
                    for (ClusterBlockLevel level : expectedBlocks.levels()) {
                        assertTrue("node [" + node + "] does have level [" + level + "] in it's blocks", state.getBlocks().hasGlobalBlock(level));
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
                if (state.nodes().masterNode() != null) {
                    masterNode = state.nodes().masterNode().name();
                }
                logger.trace("[{}] master is [{}]", node, state.nodes().masterNode());
                assertThat("node [" + node + "] still has [" + masterNode + "] as master",
                        oldMasterNode, not(equalTo(masterNode)));
            }
        }, 10, TimeUnit.SECONDS);
    }

    private void assertMaster(String masterNode, List<String> nodes) {
        for (String node : nodes) {
            ClusterState state = getNodeClusterState(node);
            String failMsgSuffix = "cluster_state:\n" + state.prettyPrint();
            assertThat("wrong node count on [" + node + "]. " + failMsgSuffix, state.nodes().size(), equalTo(nodes.size()));
            String otherMasterNodeName = state.nodes().masterNode() != null ? state.nodes().masterNode().name() : null;
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
