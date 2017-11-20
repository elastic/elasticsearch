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
package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.shuffle;
import static org.elasticsearch.cluster.ESAllocationTestCase.createAllocationService;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.elasticsearch.cluster.routing.RoutingTableTests.updateActiveAllocations;
import static org.elasticsearch.cluster.service.MasterServiceTests.discoveryState;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.elasticsearch.test.VersionUtils.allVersions;
import static org.elasticsearch.test.VersionUtils.getPreviousVersion;
import static org.elasticsearch.test.VersionUtils.randomCompatibleVersion;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@TestLogging("org.elasticsearch.discovery.zen:TRACE,org.elasticsearch.cluster.service:TRACE")
public class NodeJoinControllerTests extends ESTestCase {

    private static ThreadPool threadPool;

    private MasterService masterService;
    private NodeJoinController nodeJoinController;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("NodeJoinControllerTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        masterService.close();
    }

    private static ClusterState initialState(boolean withMaster) {
        DiscoveryNode localNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
            new HashSet<>(Arrays.asList(DiscoveryNode.Role.values())),Version.CURRENT);
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder()
                .add(localNode)
                .localNodeId(localNode.getId())
                .masterNodeId(withMaster ? localNode.getId() : null))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
        return initialClusterState;
    }

    private void setupMasterServiceAndNodeJoinController(ClusterState initialState) {
        if (masterService != null || nodeJoinController != null) {
            throw new IllegalStateException("method setupMasterServiceAndNodeJoinController can only be called once");
        }
        masterService = ClusterServiceUtils.createMasterService(threadPool, initialState);
        nodeJoinController = new NodeJoinController(masterService, createAllocationService(Settings.EMPTY),
            new ElectMasterService(Settings.EMPTY), Settings.EMPTY);
    }

    public void testSimpleJoinAccumulation() throws InterruptedException, ExecutionException {
        setupMasterServiceAndNodeJoinController(initialState(true));
        List<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(discoveryState(masterService).nodes().getLocalNode());

        int nodeId = 0;
        for (int i = randomInt(5); i > 0; i--) {
            DiscoveryNode node = newNode(nodeId++);
            nodes.add(node);
            joinNode(node);
        }
        nodeJoinController.startElectionContext();
        ArrayList<Future<Void>> pendingJoins = new ArrayList<>();
        for (int i = randomInt(5); i > 0; i--) {
            DiscoveryNode node = newNode(nodeId++);
            nodes.add(node);
            pendingJoins.add(joinNodeAsync(node));
        }
        nodeJoinController.stopElectionContext("test");
        boolean hadSyncJoin = false;
        for (int i = randomInt(5); i > 0; i--) {
            DiscoveryNode node = newNode(nodeId++);
            nodes.add(node);
            joinNode(node);
            hadSyncJoin = true;
        }
        if (hadSyncJoin) {
            for (Future<Void> joinFuture : pendingJoins) {
                assertThat(joinFuture.isDone(), equalTo(true));
            }
        }
        for (Future<Void> joinFuture : pendingJoins) {
            joinFuture.get();
        }
    }

    public void testFailingJoinsWhenNotMaster() throws ExecutionException, InterruptedException {
        setupMasterServiceAndNodeJoinController(initialState(false));
        int nodeId = 0;
        try {
            joinNode(newNode(nodeId++));
            fail("failed to fail node join when not a master");
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(NotMasterException.class));
        }

        logger.debug("--> testing joins fail post accumulation");
        ArrayList<Future<Void>> pendingJoins = new ArrayList<>();
        nodeJoinController.startElectionContext();
        for (int i = 1 + randomInt(5); i > 0; i--) {
            DiscoveryNode node = newNode(nodeId++);
            final Future<Void> future = joinNodeAsync(node);
            pendingJoins.add(future);
            assertThat(future.isDone(), equalTo(false));
        }
        nodeJoinController.stopElectionContext("test");
        for (Future<Void> future : pendingJoins) {
            try {
                future.get();
                fail("failed to fail accumulated node join when not a master");
            } catch (ExecutionException e) {
                assertThat(e.getCause(), instanceOf(NotMasterException.class));
            }
        }
    }

    public void testSimpleMasterElectionWithoutRequiredJoins() throws InterruptedException, ExecutionException {
        setupMasterServiceAndNodeJoinController(initialState(false));
        int nodeId = 0;
        final int requiredJoins = 0;
        logger.debug("--> using requiredJoins [{}]", requiredJoins);
        // initial (failing) joins shouldn't count
        for (int i = randomInt(5); i > 0; i--) {
            try {
                joinNode(newNode(nodeId++));
                fail("failed to fail node join when not a master");
            } catch (ExecutionException e) {
                assertThat(e.getCause(), instanceOf(NotMasterException.class));
            }
        }

        nodeJoinController.startElectionContext();
        final SimpleFuture electionFuture = new SimpleFuture("master election");
        final Thread masterElection = new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                logger.error("unexpected error from waitToBeElectedAsMaster", e);
                electionFuture.markAsFailed(e);
            }

            @Override
            protected void doRun() throws Exception {
                nodeJoinController.waitToBeElectedAsMaster(requiredJoins, TimeValue.timeValueHours(30),
                    new NodeJoinController.ElectionCallback() {
                    @Override
                    public void onElectedAsMaster(ClusterState state) {
                        assertThat("callback called with elected as master, but state disagrees", state.nodes().isLocalNodeElectedMaster(),
                            equalTo(true));
                        electionFuture.markAsDone();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.error("unexpected error while waiting to be elected as master", t);
                        electionFuture.markAsFailed(t);
                    }
                });
            }
        });
        masterElection.start();

        logger.debug("--> requiredJoins is set to 0. verifying election finished");
        electionFuture.get();
    }

    public void testSimpleMasterElection() throws InterruptedException, ExecutionException {
        setupMasterServiceAndNodeJoinController(initialState(false));
        int nodeId = 0;
        final int requiredJoins = 1 + randomInt(5);
        logger.debug("--> using requiredJoins [{}]", requiredJoins);
        // initial (failing) joins shouldn't count
        for (int i = randomInt(5); i > 0; i--) {
            try {
                joinNode(newNode(nodeId++));
                fail("failed to fail node join when not a master");
            } catch (ExecutionException e) {
                assertThat(e.getCause(), instanceOf(NotMasterException.class));
            }
        }

        nodeJoinController.startElectionContext();
        final SimpleFuture electionFuture = new SimpleFuture("master election");
        final Thread masterElection = new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                logger.error("unexpected error from waitToBeElectedAsMaster", e);
                electionFuture.markAsFailed(e);
            }

            @Override
            protected void doRun() throws Exception {
                nodeJoinController.waitToBeElectedAsMaster(requiredJoins, TimeValue.timeValueHours(30),
                    new NodeJoinController.ElectionCallback() {
                    @Override
                    public void onElectedAsMaster(ClusterState state) {
                        assertThat("callback called with elected as master, but state disagrees", state.nodes().isLocalNodeElectedMaster(),
                            equalTo(true));
                        electionFuture.markAsDone();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.error("unexpected error while waiting to be elected as master", t);
                        electionFuture.markAsFailed(t);
                    }
                });
            }
        });
        masterElection.start();
        assertThat("election finished immediately but required joins is [" + requiredJoins + "]", electionFuture.isDone(), equalTo(false));

        final int initialJoins = randomIntBetween(0, requiredJoins - 1);
        final ArrayList<SimpleFuture> pendingJoins = new ArrayList<>();
        ArrayList<DiscoveryNode> nodesToJoin = new ArrayList<>();
        for (int i = 0; i < initialJoins; i++) {
            DiscoveryNode node = newNode(nodeId++, true);
            for (int j = 1 + randomInt(3); j > 0; j--) {
                nodesToJoin.add(node);
            }
        }

        // data nodes shouldn't count
        for (int i = 0; i < requiredJoins; i++) {
            DiscoveryNode node = newNode(nodeId++, false);
            for (int j = 1 + randomInt(3); j > 0; j--) {
                nodesToJoin.add(node);
            }
        }

        // add

        shuffle(nodesToJoin, random());
        logger.debug("--> joining [{}] unique master nodes. Total of [{}] join requests", initialJoins, nodesToJoin.size());
        for (DiscoveryNode node : nodesToJoin) {
            pendingJoins.add(joinNodeAsync(node));
        }

        logger.debug("--> asserting master election didn't finish yet");
        assertThat("election finished after [" + initialJoins + "] master nodes but required joins is [" + requiredJoins + "]",
            electionFuture.isDone(), equalTo(false));

        final int finalJoins = requiredJoins - initialJoins + randomInt(5);
        nodesToJoin.clear();
        for (int i = 0; i < finalJoins; i++) {
            DiscoveryNode node = newNode(nodeId++, true);
            for (int j = 1 + randomInt(3); j > 0; j--) {
                nodesToJoin.add(node);
            }
        }

        for (int i = 0; i < requiredJoins; i++) {
            DiscoveryNode node = newNode(nodeId++, false);
            for (int j = 1 + randomInt(3); j > 0; j--) {
                nodesToJoin.add(node);
            }
        }

        shuffle(nodesToJoin, random());
        logger.debug("--> joining [{}] nodes, with repetition a total of [{}]", finalJoins, nodesToJoin.size());
        for (DiscoveryNode node : nodesToJoin) {
            pendingJoins.add(joinNodeAsync(node));
        }
        logger.debug("--> waiting for master election to with no exception");
        electionFuture.get();

        logger.debug("--> waiting on all joins to be processed");
        for (SimpleFuture future : pendingJoins) {
            logger.debug("waiting on {}", future);
            future.get(); // throw any exception
        }

        logger.debug("--> testing accumulation stopped");
        nodeJoinController.startElectionContext();
        nodeJoinController.stopElectionContext("test");

    }

    public void testMasterElectionTimeout() throws InterruptedException {
        setupMasterServiceAndNodeJoinController(initialState(false));
        int nodeId = 0;
        final int requiredJoins = 1 + randomInt(5);
        logger.debug("--> using requiredJoins [{}]", requiredJoins);
        // initial (failing) joins shouldn't count
        for (int i = randomInt(5); i > 0; i--) {
            try {
                joinNode(newNode(nodeId++));
                fail("failed to fail node join when not a master");
            } catch (ExecutionException e) {
                assertThat(e.getCause(), instanceOf(NotMasterException.class));
            }
        }

        nodeJoinController.startElectionContext();
        final int initialJoins = randomIntBetween(0, requiredJoins - 1);
        final ArrayList<SimpleFuture> pendingJoins = new ArrayList<>();
        ArrayList<DiscoveryNode> nodesToJoin = new ArrayList<>();
        for (int i = 0; i < initialJoins; i++) {
            DiscoveryNode node = newNode(nodeId++);
            for (int j = 1 + randomInt(3); j > 0; j--) {
                nodesToJoin.add(node);
            }
        }
        shuffle(nodesToJoin, random());
        logger.debug("--> joining [{}] nodes, with repetition a total of [{}]", initialJoins, nodesToJoin.size());
        for (DiscoveryNode node : nodesToJoin) {
            pendingJoins.add(joinNodeAsync(node));
        }

        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        nodeJoinController.waitToBeElectedAsMaster(requiredJoins, TimeValue.timeValueMillis(1), new NodeJoinController.ElectionCallback() {
            @Override
            public void onElectedAsMaster(ClusterState state) {
                assertThat("callback called with elected as master, but state disagrees", state.nodes().isLocalNodeElectedMaster(),
                    equalTo(true));
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                failure.set(t);
                latch.countDown();
            }
        });
        latch.await();
        logger.debug("--> verifying election timed out");
        assertThat(failure.get(), instanceOf(NotMasterException.class));

        logger.debug("--> verifying all joins are failed");
        for (SimpleFuture future : pendingJoins) {
            logger.debug("waiting on {}", future);
            try {
                future.get(); // throw any exception
                fail("failed to fail node join [" + future + "]");
            } catch (ExecutionException e) {
                assertThat(e.getCause(), instanceOf(NotMasterException.class));
            }
        }
    }

    public void testNewClusterStateOnExistingNodeJoin() throws InterruptedException, ExecutionException {
        ClusterState state = initialState(true);
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(state.nodes());
        final DiscoveryNode other_node = new DiscoveryNode("other_node", buildNewFakeTransportAddress(),
            emptyMap(), emptySet(), Version.CURRENT);
        nodesBuilder.add(other_node);
        setupMasterServiceAndNodeJoinController(ClusterState.builder(state).nodes(nodesBuilder).build());

        state = discoveryState(masterService);
        joinNode(other_node);
        assertTrue("failed to publish a new state upon existing join", discoveryState(masterService) != state);
    }

    public void testNormalConcurrentJoins() throws InterruptedException {
        setupMasterServiceAndNodeJoinController(initialState(true));
        Thread[] threads = new Thread[3 + randomInt(5)];
        ArrayList<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(discoveryState(masterService).nodes().getLocalNode());
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        final List<Throwable> backgroundExceptions = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threads.length; i++) {
            final DiscoveryNode node = newNode(i);
            final int iterations = rarely() ? randomIntBetween(1, 4) : 1;
            nodes.add(node);
            threads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.error("unexpected error in join thread", e);
                    backgroundExceptions.add(e);
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    for (int i = 0; i < iterations; i++) {
                        logger.debug("{} joining", node);
                        joinNode(node);
                    }
                }
            }, "t_" + i);
            threads[i].start();
        }

        logger.info("--> waiting for joins to complete");
        for (Thread thread : threads) {
            thread.join();
        }

        assertNodesInCurrentState(nodes);
    }

    public void testElectionWithConcurrentJoins() throws InterruptedException, BrokenBarrierException {
        setupMasterServiceAndNodeJoinController(initialState(false));

        nodeJoinController.startElectionContext();

        Thread[] threads = new Thread[3 + randomInt(5)];
        final int requiredJoins = randomInt(threads.length);
        ArrayList<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(discoveryState(masterService).nodes().getLocalNode());
        final CyclicBarrier barrier = new CyclicBarrier(threads.length + 1);
        final List<Throwable> backgroundExceptions = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threads.length; i++) {
            final DiscoveryNode node = newNode(i, true);
            final int iterations = rarely() ? randomIntBetween(1, 4) : 1;
            nodes.add(node);
            threads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.error("unexpected error in join thread", e);
                    backgroundExceptions.add(e);
                }

                @Override
                protected void doRun() throws Exception {
                    barrier.await();
                    for (int i = 0; i < iterations; i++) {
                        logger.debug("{} joining", node);
                        joinNode(node);
                    }
                }
            }, "t_" + i);
            threads[i].start();
        }

        barrier.await();
        logger.info("--> waiting to be elected as master (required joins [{}])", requiredJoins);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        nodeJoinController.waitToBeElectedAsMaster(requiredJoins, TimeValue.timeValueHours(30), new NodeJoinController.ElectionCallback() {
            @Override
            public void onElectedAsMaster(ClusterState state) {
                assertThat("callback called with elected as master, but state disagrees", state.nodes().isLocalNodeElectedMaster(),
                    equalTo(true));
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("unexpected error while waiting to be elected as master", t);
                failure.set(t);
                latch.countDown();
            }
        });
        latch.await();
        ExceptionsHelper.reThrowIfNotNull(failure.get());


        logger.info("--> waiting for joins to complete");
        for (Thread thread : threads) {
            thread.join();
        }

        assertNodesInCurrentState(nodes);
    }

    public void testRejectingJoinWithSameAddressButDifferentId() throws InterruptedException, ExecutionException {
        addNodes(randomInt(5));
        ClusterState state = discoveryState(masterService);
        final DiscoveryNode existing = randomFrom(StreamSupport.stream(state.nodes().spliterator(), false).collect(Collectors.toList()));
        final DiscoveryNode other_node = new DiscoveryNode("other_node", existing.getAddress(), emptyMap(), emptySet(), Version.CURRENT);

        ExecutionException e = expectThrows(ExecutionException.class, () -> joinNode(other_node));
        assertThat(e.getMessage(), containsString("found existing node"));
    }

    public void testRejectingJoinWithSameIdButDifferentNode() throws InterruptedException, ExecutionException {
        addNodes(randomInt(5));
        ClusterState state = discoveryState(masterService);
        final DiscoveryNode existing = randomFrom(StreamSupport.stream(state.nodes().spliterator(), false).collect(Collectors.toList()));
        final DiscoveryNode other_node = new DiscoveryNode(
            randomBoolean() ? existing.getName() : "other_name",
            existing.getId(),
            randomBoolean() ? existing.getAddress() : buildNewFakeTransportAddress(),
            randomBoolean() ? existing.getAttributes() : Collections.singletonMap("attr", "other"),
            randomBoolean() ? existing.getRoles() : new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))),
            existing.getVersion());

        ExecutionException e = expectThrows(ExecutionException.class, () -> joinNode(other_node));
        assertThat(e.getMessage(), containsString("found existing node"));
    }

    public void testRejectingRestartedNodeJoinsBeforeProcessingNodeLeft() throws InterruptedException, ExecutionException {
        addNodes(randomInt(5));
        ClusterState state = discoveryState(masterService);
        final DiscoveryNode existing = randomFrom(StreamSupport.stream(state.nodes().spliterator(), false).collect(Collectors.toList()));
        joinNode(existing); // OK

        final DiscoveryNode other_node = new DiscoveryNode(existing.getId(), existing.getAddress(), existing.getAttributes(),
            existing.getRoles(), Version.CURRENT);

        ExecutionException e = expectThrows(ExecutionException.class, () -> joinNode(other_node));
        assertThat(e.getMessage(), containsString("found existing node"));
    }

    public void testRejectingJoinWithIncompatibleVersion() throws InterruptedException, ExecutionException {
        addNodes(randomInt(5));
        final Version badVersion;
        if (randomBoolean()) {
            badVersion = getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion());
        } else {
            badVersion = randomFrom(allVersions().stream().filter(v -> v.major < Version.CURRENT.major).collect(Collectors.toList()));
        }
        final DiscoveryNode badNode = new DiscoveryNode("badNode", buildNewFakeTransportAddress(), emptyMap(),
            new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))), badVersion);

        final Version goodVersion =
            randomFrom(allVersions().stream().filter(v -> v.major >= Version.CURRENT.major).collect(Collectors.toList()));
        final DiscoveryNode goodNode = new DiscoveryNode("goodNode", buildNewFakeTransportAddress(), emptyMap(),
            new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))), goodVersion);

        CountDownLatch latch = new CountDownLatch(1);
        // block cluster state
        masterService.submitStateUpdateTask("test", new ClusterStateUpdateTask(Priority.IMMEDIATE) {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                latch.await();
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new AssertionError(e);
            }
        });

        final SimpleFuture badJoin;
        final SimpleFuture goodJoin;
        if (randomBoolean()) {
            badJoin = joinNodeAsync(badNode);
            goodJoin = joinNodeAsync(goodNode);
        } else {
            goodJoin = joinNodeAsync(goodNode);
            badJoin = joinNodeAsync(badNode);
        }
        assert goodJoin.isDone() == false;
        assert badJoin.isDone() == false;
        latch.countDown();
        goodJoin.get();
        ExecutionException e = expectThrows(ExecutionException.class, badJoin::get);
        assertThat(e.getCause(), instanceOf(IllegalStateException.class));
        assertThat(e.getCause().getMessage(), allOf(containsString("node version"), containsString("not supported")));
    }

    public void testRejectingJoinWithIncompatibleVersionWithUnrecoveredState() throws InterruptedException, ExecutionException {
        addNodes(randomInt(5));
        ClusterState.Builder builder = ClusterState.builder(discoveryState(masterService));
        builder.blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
        setState(masterService, builder.build());
        final Version badVersion = getPreviousVersion(Version.CURRENT.minimumCompatibilityVersion());
        final DiscoveryNode badNode = new DiscoveryNode("badNode", buildNewFakeTransportAddress(), emptyMap(),
            new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))), badVersion);

        final Version goodVersion = randomFrom(randomCompatibleVersion(random(), Version.CURRENT));
        final DiscoveryNode goodNode = new DiscoveryNode("goodNode", buildNewFakeTransportAddress(), emptyMap(),
            new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))), goodVersion);

        CountDownLatch latch = new CountDownLatch(1);
        // block cluster state
        masterService.submitStateUpdateTask("test", new ClusterStateUpdateTask(Priority.IMMEDIATE) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                latch.await();
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new AssertionError(e);
            }
        });

        final SimpleFuture badJoin;
        final SimpleFuture goodJoin;
        if (randomBoolean()) {
            badJoin = joinNodeAsync(badNode);
            goodJoin = joinNodeAsync(goodNode);
        } else {
            goodJoin = joinNodeAsync(goodNode);
            badJoin = joinNodeAsync(badNode);
        }
        assert goodJoin.isDone() == false;
        assert badJoin.isDone() == false;
        latch.countDown();
        goodJoin.get();
        ExecutionException e = expectThrows(ExecutionException.class, badJoin::get);
        assertThat(e.getCause(), instanceOf(IllegalStateException.class));
        assertThat(e.getCause().getMessage(), allOf(containsString("node version"), containsString("not supported")));
    }

    /**
     * Tests tha node can become a master, even though the last cluster state it knows contains
     * nodes that conflict with the joins it got and needs to become a master
     */
    public void testElectionBasedOnConflictingNodes() throws InterruptedException, ExecutionException {
        ClusterState initialState = initialState(true);
        final DiscoveryNode masterNode = initialState.nodes().getLocalNode();
        final DiscoveryNode otherNode = new DiscoveryNode("other_node", buildNewFakeTransportAddress(), emptyMap(),
            EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT);
        // simulate master going down with stale nodes in it's cluster state (for example when min master nodes is set to 2)
        // also add some shards to that node
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder(initialState.nodes());
        discoBuilder.masterNodeId(null);
        discoBuilder.add(otherNode);
        ClusterState.Builder stateBuilder = ClusterState.builder(initialState).nodes(discoBuilder);
        if (randomBoolean()) {
            IndexMetaData indexMetaData = IndexMetaData.builder("test").settings(Settings.builder()
                .put(SETTING_VERSION_CREATED, Version.CURRENT)
                .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 1)
                .put(SETTING_CREATION_DATE, System.currentTimeMillis())).build();
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetaData.getIndex());
            RoutingTable.Builder routing = new RoutingTable.Builder();
            routing.addAsNew(indexMetaData);
            final ShardId shardId = new ShardId("test", "_na_", 0);
            IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);

            final DiscoveryNode primaryNode = randomBoolean() ? masterNode : otherNode;
            final DiscoveryNode replicaNode = primaryNode.equals(masterNode) ? otherNode : masterNode;
            final boolean primaryStarted = randomBoolean();
            indexShardRoutingBuilder.addShard(TestShardRouting.newShardRouting("test", 0, primaryNode.getId(), null, true,
                primaryStarted ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING,
                primaryStarted ? null : new UnassignedInfo(UnassignedInfo.Reason.INDEX_REOPENED, "getting there")));
            if (primaryStarted) {
                boolean replicaStared = randomBoolean();
                indexShardRoutingBuilder.addShard(TestShardRouting.newShardRouting("test", 0, replicaNode.getId(), null, false,
                    replicaStared ? ShardRoutingState.STARTED : ShardRoutingState.INITIALIZING,
                    replicaStared ? null : new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "getting there")));
            } else {
                indexShardRoutingBuilder.addShard(TestShardRouting.newShardRouting("test", 0, null, null, false,
                    ShardRoutingState.UNASSIGNED, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "life sucks")));
            }
            indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
            IndexRoutingTable indexRoutingTable = indexRoutingTableBuilder.build();
            IndexMetaData updatedIndexMetaData = updateActiveAllocations(indexRoutingTable, indexMetaData);
            stateBuilder.metaData(MetaData.builder().put(updatedIndexMetaData, false).generateClusterUuidIfNeeded())
                        .routingTable(RoutingTable.builder().add(indexRoutingTable).build());
        }

        setupMasterServiceAndNodeJoinController(stateBuilder.build());

        // conflict on node id or address
        final DiscoveryNode conflictingNode = randomBoolean() ?
            new DiscoveryNode(otherNode.getId(), randomBoolean() ? otherNode.getAddress() : buildNewFakeTransportAddress(),
                otherNode.getAttributes(), otherNode.getRoles(), Version.CURRENT) :
            new DiscoveryNode("conflicting_address_node", otherNode.getAddress(), otherNode.getAttributes(), otherNode.getRoles(),
                Version.CURRENT);

        nodeJoinController.startElectionContext();
        final SimpleFuture joinFuture = joinNodeAsync(conflictingNode);
        final CountDownLatch elected = new CountDownLatch(1);
        nodeJoinController.waitToBeElectedAsMaster(1, TimeValue.timeValueHours(5), new NodeJoinController.ElectionCallback() {
            @Override
            public void onElectedAsMaster(ClusterState state) {
                elected.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("failed to be elected as master", t);
                throw new AssertionError("failed to be elected as master", t);
            }
        });

        elected.await();

        joinFuture.get(); // throw any exception

        final ClusterState finalState = discoveryState(masterService);
        final DiscoveryNodes finalNodes = finalState.nodes();
        assertTrue(finalNodes.isLocalNodeElectedMaster());
        assertThat(finalNodes.getLocalNode(), equalTo(masterNode));
        assertThat(finalNodes.getSize(), equalTo(2));
        assertThat(finalNodes.get(conflictingNode.getId()), equalTo(conflictingNode));
        List<ShardRouting> activeShardsOnRestartedNode =
            StreamSupport.stream(finalState.getRoutingNodes().node(conflictingNode.getId()).spliterator(), false)
                .filter(ShardRouting::active).collect(Collectors.toList());
        assertThat(activeShardsOnRestartedNode, empty());
    }


    private void addNodes(int count) {
        ClusterState state = initialState(true);
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(state.nodes());
        for (int i = 0;i< count;i++) {
            final DiscoveryNode node = new DiscoveryNode("node_" + state.nodes().getSize() + i, buildNewFakeTransportAddress(),
                emptyMap(), new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))), Version.CURRENT);
            nodesBuilder.add(node);
        }
        setupMasterServiceAndNodeJoinController(ClusterState.builder(state).nodes(nodesBuilder).build());
    }

    protected void assertNodesInCurrentState(List<DiscoveryNode> expectedNodes) {
        final ClusterState state = discoveryState(masterService);
        logger.info("assert for [{}] in:\n{}", expectedNodes, state);
        DiscoveryNodes discoveryNodes = state.nodes();
        for (DiscoveryNode node : expectedNodes) {
            assertThat("missing " + node + "\n" + discoveryNodes, discoveryNodes.get(node.getId()), equalTo(node));
        }
        assertThat(discoveryNodes.getSize(), equalTo(expectedNodes.size()));
    }

    static class SimpleFuture extends BaseFuture<Void> {
        final String description;

        SimpleFuture(String description) {
            this.description = description;
        }

        public void markAsDone() {
            set(null);
        }

        public void markAsFailed(Throwable t) {
            setException(t);
        }

        @Override
        public String toString() {
            return "future [" + description + "]";
        }
    }

    static final AtomicInteger joinId = new AtomicInteger();

    private SimpleFuture joinNodeAsync(final DiscoveryNode node) throws InterruptedException {
        final SimpleFuture future = new SimpleFuture("join of " + node + " (id [" + joinId.incrementAndGet() + "]");
        logger.debug("starting {}", future);
        // clone the node before submitting to simulate an incoming join, which is guaranteed to have a new
        // disco node object serialized off the network
        nodeJoinController.handleJoinRequest(cloneNode(node), new MembershipAction.JoinCallback() {
            @Override
            public void onSuccess() {
                logger.debug("{} completed", future);
                future.markAsDone();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("unexpected error for {}", future), e);
                future.markAsFailed(e);
            }
        });
        return future;
    }

    /**
     * creates an object clone of node, so it will be a different object instance
     */
    private DiscoveryNode cloneNode(DiscoveryNode node) {
        return new DiscoveryNode(node.getName(), node.getId(), node.getEphemeralId(), node.getHostName(), node.getHostAddress(),
            node.getAddress(), node.getAttributes(), node.getRoles(), node.getVersion());
    }

    private void joinNode(final DiscoveryNode node) throws InterruptedException, ExecutionException {
        joinNodeAsync(node).get();
    }

    protected DiscoveryNode newNode(int i) {
        return newNode(i, randomBoolean());
    }

    protected DiscoveryNode newNode(int i, boolean master) {
        Set<DiscoveryNode.Role> roles = new HashSet<>();
        if (master) {
            roles.add(DiscoveryNode.Role.MASTER);
        }
        final String prefix = master ? "master_" : "data_";
        return new DiscoveryNode(prefix + i, i + "", buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }
}
