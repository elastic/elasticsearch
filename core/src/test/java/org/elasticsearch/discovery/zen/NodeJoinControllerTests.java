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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.membership.MembershipAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
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

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.shuffle;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@TestLogging("discovery.zen:TRACE")
public class NodeJoinControllerTests extends ESTestCase {

    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private NodeJoinController nodeJoinController;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("ShardReplicationTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createClusterService(threadPool);
        final DiscoveryNodes initialNodes = clusterService.state().nodes();
        final DiscoveryNode localNode = initialNodes.getLocalNode();
        // make sure we have a master
        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(
            DiscoveryNodes.builder(initialNodes).masterNodeId(localNode.getId())));
        nodeJoinController = new NodeJoinController(clusterService, new NoopAllocationService(Settings.EMPTY),
            new ElectMasterService(Settings.EMPTY, Version.CURRENT),
            new DiscoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            Settings.EMPTY);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    public void testSimpleJoinAccumulation() throws InterruptedException, ExecutionException {
        List<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(clusterService.localNode());

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
        // remove current master flag
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterService.state().nodes()).masterNodeId(null);
        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodes));
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
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterService.state().nodes()).masterNodeId(null);
        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodes));
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
            public void onFailure(Throwable t) {
                logger.error("unexpected error from waitToBeElectedAsMaster", t);
                electionFuture.markAsFailed(t);
            }

            @Override
            protected void doRun() throws Exception {
                nodeJoinController.waitToBeElectedAsMaster(requiredJoins, TimeValue.timeValueHours(30), new NodeJoinController.ElectionCallback() {
                    @Override
                    public void onElectedAsMaster(ClusterState state) {
                        assertThat("callback called with elected as master, but state disagrees", state.nodes().isLocalNodeElectedMaster(), equalTo(true));
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
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterService.state().nodes()).masterNodeId(null);
        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodes));
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
            public void onFailure(Throwable t) {
                logger.error("unexpected error from waitToBeElectedAsMaster", t);
                electionFuture.markAsFailed(t);
            }

            @Override
            protected void doRun() throws Exception {
                nodeJoinController.waitToBeElectedAsMaster(requiredJoins, TimeValue.timeValueHours(30), new NodeJoinController.ElectionCallback() {
                    @Override
                    public void onElectedAsMaster(ClusterState state) {
                        assertThat("callback called with elected as master, but state disagrees", state.nodes().isLocalNodeElectedMaster(), equalTo(true));
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
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterService.state().nodes()).masterNodeId(null);
        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodes));
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
        ClusterState state = clusterService.state();
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(state.nodes());
        final DiscoveryNode other_node = new DiscoveryNode("other_node", DummyTransportAddress.INSTANCE,
            emptyMap(), emptySet(), Version.CURRENT);
        nodesBuilder.put(other_node);
        setState(clusterService, ClusterState.builder(state).nodes(nodesBuilder));

        state = clusterService.state();
        joinNode(other_node);
        assertTrue("failed to publish a new state upon existing join", clusterService.state() != state);
    }

    public void testNormalConcurrentJoins() throws InterruptedException {
        Thread[] threads = new Thread[3 + randomInt(5)];
        ArrayList<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(clusterService.localNode());
        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
        final List<Throwable> backgroundExceptions = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threads.length; i++) {
            final DiscoveryNode node = newNode(i);
            final int iterations = rarely() ? randomIntBetween(1, 4) : 1;
            nodes.add(node);
            threads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Throwable t) {
                    logger.error("unexpected error in join thread", t);
                    backgroundExceptions.add(t);
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
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterService.state().nodes()).masterNodeId(null);
        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodesBuilder));

        nodeJoinController.startElectionContext();

        Thread[] threads = new Thread[3 + randomInt(5)];
        final int requiredJoins = randomInt(threads.length);
        ArrayList<DiscoveryNode> nodes = new ArrayList<>();
        nodes.add(clusterService.localNode());
        final CyclicBarrier barrier = new CyclicBarrier(threads.length + 1);
        final List<Throwable> backgroundExceptions = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threads.length; i++) {
            final DiscoveryNode node = newNode(i, true);
            final int iterations = rarely() ? randomIntBetween(1, 4) : 1;
            nodes.add(node);
            threads[i] = new Thread(new AbstractRunnable() {
                @Override
                public void onFailure(Throwable t) {
                    logger.error("unexpected error in join thread", t);
                    backgroundExceptions.add(t);
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


    static class NoopAllocationService extends AllocationService {

        public NoopAllocationService(Settings settings) {
            super(settings, null, null, null, null);
        }

        @Override
        public RoutingAllocation.Result applyStartedShards(ClusterState clusterState, List<? extends ShardRouting> startedShards,
                                                           boolean withReroute) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), clusterState.metaData());
        }

        @Override
        public RoutingAllocation.Result applyFailedShards(ClusterState clusterState,
                                                          List<FailedRerouteAllocation.FailedShard> failedShards) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), clusterState.metaData());
        }

        @Override
        protected RoutingAllocation.Result reroute(ClusterState clusterState, String reason, boolean debug) {
            return new RoutingAllocation.Result(false, clusterState.routingTable(), clusterState.metaData());
        }
    }

    protected void assertNodesInCurrentState(List<DiscoveryNode> expectedNodes) {
        final ClusterState state = clusterService.state();
        logger.info("assert for [{}] in:\n{}", expectedNodes, state.prettyPrint());
        DiscoveryNodes discoveryNodes = state.nodes();
        for (DiscoveryNode node : expectedNodes) {
            assertThat("missing " + node + "\n" + discoveryNodes.prettyPrint(), discoveryNodes.get(node.getId()), equalTo(node));
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

    final static AtomicInteger joinId = new AtomicInteger();

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
            public void onFailure(Throwable t) {
                logger.error("unexpected error for {}", t, future);
                future.markAsFailed(t);
            }
        });
        return future;
    }

    /**
     * creates an object clone of node, so it will be a different object instance
     */
    private DiscoveryNode cloneNode(DiscoveryNode node) {
        return new DiscoveryNode(node.getName(), node.getId(), node.getHostName(), node.getHostAddress(), node.getAddress(),
            node.getAttributes(), node.getRoles(), node.getVersion());
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
        return new DiscoveryNode(prefix + i, i + "", new LocalTransportAddress("test_" + i), emptyMap(), roles, Version.CURRENT);
    }
}
