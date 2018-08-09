package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestLogging("org.elasticsearch.discovery.zen:TRACE,org.elasticsearch.cluster.service:TRACE,org.elasticsearch.cluster.coordination:TRACE")
public class NodeJoinTests extends ESTestCase {

    private static ThreadPool threadPool;

    private MasterService masterService;
    private Coordinator coordinator;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("NodeJoinControllerTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        masterService.close();
    }

    private static ClusterState initialState(boolean withMaster, DiscoveryNode localNode, long term, long version,
                                             VotingConfiguration config) {
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder()
                .add(localNode)
                .localNodeId(localNode.getId())
                .masterNodeId(withMaster ? localNode.getId() : null))
            .term(term)
            .version(version)
            .lastAcceptedConfiguration(config)
            .lastCommittedConfiguration(config)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
        return initialClusterState;
    }

    private void setupMasterServiceAndNodeJoinController(long term, ClusterState initialState) {
        if (masterService != null || coordinator != null) {
            throw new IllegalStateException("method setupMasterServiceAndNodeJoinController can only be called once");
        }
        masterService = ClusterServiceUtils.createMasterService(threadPool, initialState);
        TransportService transportService = mock(TransportService.class);
        when(transportService.getLocalNode()).thenReturn(initialState.nodes().getLocalNode());
        coordinator = new Coordinator(Settings.EMPTY,
            transportService,
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            masterService,
            () -> new CoordinationStateTests.InMemoryPersistedState(term, initialState));
        coordinator.start();
        coordinator.startInitialJoin();
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

    private DiscoveryNode cloneNode(DiscoveryNode node) {
        return new DiscoveryNode(node.getName(), node.getId(), node.getEphemeralId(), node.getHostName(), node.getHostAddress(),
            node.getAddress(), node.getAttributes(), node.getRoles(), node.getVersion());
    }

    private void joinNode(final JoinRequest joinRequest) throws InterruptedException, ExecutionException {
        joinNodeAsync(joinRequest).get();
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

    private SimpleFuture joinNodeAsync(final JoinRequest joinRequest) {
        final SimpleFuture future = new SimpleFuture("join of " + joinRequest + "]");
        logger.debug("starting {}", future);
        // clone the node before submitting to simulate an incoming join, which is guaranteed to have a new
        // disco node object serialized off the network
        coordinator.handleJoinRequest(joinRequest, new MembershipAction.JoinCallback() {
            @Override
            public void onSuccess() {
                logger.debug("{} completed", future);
                future.markAsDone();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> new ParameterizedMessage("unexpected error for {}", future), e);
                future.markAsFailed(e);
            }
        });
        return future;
    }

    public void testSuccessfulJoinAccumulation() {
        List<DiscoveryNode> nodes = IntStream.rangeClosed(1, randomIntBetween(2, 5))
            .mapToObj(nodeId -> newNode(nodeId, true)).collect(Collectors.toList());

        VotingConfiguration votingConfiguration = new VotingConfiguration(
            randomSubsetOf(randomIntBetween(1, nodes.size()), nodes).stream().map(DiscoveryNode::getId).collect(Collectors.toSet()));

        DiscoveryNode localNode = nodes.get(0);
        setupMasterServiceAndNodeJoinController(1, initialState(false, localNode, 1, 1, votingConfiguration));

        // we need at least a quorum of voting nodes with a correct term

        List<Thread> threads = randomSubsetOf(nodes).stream().map(node -> new Thread(() -> {
            JoinRequest joinRequest = new JoinRequest(node, Optional.of(new Join(node, localNode, 2, 1, 1)));
            try {
                joinNode(joinRequest);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        })).collect(Collectors.toList());

        threads.forEach(Thread::start);
        threads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });


//        Thread[] threads = new Thread[3 + randomInt(5)];
//        ArrayList<DiscoveryNode> nodes = new ArrayList<>();
//        nodes.add(discoveryState(masterService).nodes().getLocalNode());
//        final CyclicBarrier barrier = new CyclicBarrier(threads.length);
//        final List<Throwable> backgroundExceptions = new CopyOnWriteArrayList<>();
//        for (int i = 0; i < threads.length; i++) {
//            final DiscoveryNode node = newNode(i);
//            final int iterations = rarely() ? randomIntBetween(1, 4) : 1;
//            nodes.add(node);
//            threads[i] = new Thread(new AbstractRunnable() {
//                @Override
//                public void onFailure(Exception e) {
//                    logger.error("unexpected error in join thread", e);
//                    backgroundExceptions.add(e);
//                }
//
//                @Override
//                protected void doRun() throws Exception {
//                    barrier.await();
//                    for (int i = 0; i < iterations; i++) {
//                        logger.debug("{} joining", node);
//                        joinNode(node);
//                    }
//                }
//            }, "t_" + i);
//            threads[i].start();
//        }
//
//        logger.info("--> waiting for joins to complete");
//        for (Thread thread : threads) {
//            thread.join();
//        }

        assertTrue(MasterServiceTests.discoveryState(masterService).nodes().isLocalNodeElectedMaster());

//        nodeJoinController.startElectionContext();
//        ArrayList<Future<Void>> pendingJoins = new ArrayList<>();
//        for (int i = randomInt(5); i > 0; i--) {
//            DiscoveryNode node = newNode(nodeId++);
//            nodes.add(node);
//            pendingJoins.add(joinNodeAsync(node));
//        }
//        nodeJoinController.stopElectionContext("test");
//        boolean hadSyncJoin = false;
//        for (int i = randomInt(5); i > 0; i--) {
//            DiscoveryNode node = newNode(nodeId++);
//            nodes.add(node);
//            joinNode(node);
//            hadSyncJoin = true;
//        }
//        if (hadSyncJoin) {
//            for (Future<Void> joinFuture : pendingJoins) {
//                assertThat(joinFuture.isDone(), equalTo(true));
//            }
//        }
//        for (Future<Void> joinFuture : pendingJoins) {
//            joinFuture.get();
//        }
    }
}
