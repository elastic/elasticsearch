/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTests;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class NodeJoinTests extends ESTestCase {

    private static ThreadPool threadPool;

    private MasterService masterService;
    private Coordinator coordinator;
    private DeterministicTaskQueue deterministicTaskQueue;
    private Transport transport;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(NodeJoinTests.getTestClass().getName());
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @After
    public void tearDown() throws Exception {
        masterService.stop();
        coordinator.stop();
        if (deterministicTaskQueue != null) {
            deterministicTaskQueue.runAllRunnableTasks();
        }
        masterService.close();
        coordinator.close();
        super.tearDown();
    }

    private static ClusterState initialState(DiscoveryNode localNode, long term, long version, VotingConfiguration config) {
        return ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .term(term)
                            .lastAcceptedConfiguration(config)
                            .lastCommittedConfiguration(config)
                            .build()
                    )
            )
            .version(version)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
    }

    private void setupFakeMasterServiceAndCoordinator(long term, ClusterState initialState, NodeHealthService nodeHealthService) {
        deterministicTaskQueue = new DeterministicTaskQueue();
        final ThreadPool fakeThreadPool = deterministicTaskQueue.getThreadPool();
        FakeThreadPoolMasterService fakeMasterService = new FakeThreadPoolMasterService(
            "test_node",
            fakeThreadPool,
            deterministicTaskQueue::scheduleNow
        );
        setupMasterServiceAndCoordinator(term, initialState, fakeMasterService, fakeThreadPool, Randomness.get(), nodeHealthService);
        fakeMasterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
            ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
            coordinator.handlePublishRequest(new PublishRequest(clusterStatePublicationEvent.getNewState()));
            publishListener.onResponse(null);
        });
        fakeMasterService.start();
    }

    private void setupRealMasterServiceAndCoordinator(long term, ClusterState initialState) {
        final Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test_node").build();
        MasterService masterService = new MasterService(
            settings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            new TaskManager(settings, threadPool, Set.of())
        );
        AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialState);
        masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
            ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
            clusterStateRef.set(clusterStatePublicationEvent.getNewState());
            publishListener.onResponse(null);
        });
        setupMasterServiceAndCoordinator(
            term,
            initialState,
            masterService,
            threadPool,
            new Random(Randomness.get().nextLong()),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        masterService.setClusterStateSupplier(clusterStateRef::get);
        masterService.start();
    }

    private void setupMasterServiceAndCoordinator(
        long term,
        ClusterState initialState,
        MasterService masterService,
        ThreadPool threadPool,
        Random random,
        NodeHealthService nodeHealthService
    ) {
        if (this.masterService != null || coordinator != null) {
            throw new IllegalStateException("method setupMasterServiceAndCoordinator can only be called once");
        }
        this.masterService = masterService;
        CapturingTransport capturingTransport = new CapturingTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode destination) {
                if (action.equals(HANDSHAKE_ACTION_NAME)) {
                    handleResponse(
                        requestId,
                        new TransportService.HandshakeResponse(
                            destination.getVersion(),
                            Build.current().hash(),
                            destination,
                            initialState.getClusterName()
                        )
                    );
                } else if (action.equals(JoinValidationService.JOIN_VALIDATE_ACTION_NAME)
                    || action.equals(JoinHelper.JOIN_PING_ACTION_NAME)) {
                        handleResponse(requestId, new TransportResponse.Empty());
                    } else {
                        super.onSendRequest(requestId, action, request, destination);
                    }
            }
        };
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TransportService transportService = capturingTransport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> initialState.nodes().getLocalNode(),
            clusterSettings,
            Collections.emptySet()
        );
        coordinator = new Coordinator(
            "test_node",
            Settings.EMPTY,
            clusterSettings,
            transportService,
            null,
            writableRegistry(),
            ESAllocationTestCase.createAllocationService(Settings.EMPTY),
            masterService,
            () -> new InMemoryPersistedState(term, initialState),
            r -> emptyList(),
            new NoOpClusterApplier(),
            Collections.emptyList(),
            random,
            (s, p, r) -> {},
            ElectionStrategy.DEFAULT_INSTANCE,
            nodeHealthService,
            new NoneCircuitBreakerService(),
            new Reconfigurator(Settings.EMPTY, clusterSettings),
            LeaderHeartbeatService.NO_OP,
            StatefulPreVoteCollector::new
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        transport = capturingTransport;
        coordinator.start();
        coordinator.startInitialJoin();
    }

    protected DiscoveryNode newNode(int i) {
        return newNode(i, randomBoolean());
    }

    protected DiscoveryNode newNode(int i, boolean master) {
        final Set<DiscoveryNodeRole> roles;
        if (master) {
            roles = Set.of(DiscoveryNodeRole.MASTER_ROLE);
        } else {
            roles = Set.of();
        }
        final String prefix = master ? "master_" : "data_";
        return DiscoveryNodeUtils.builder(i + "").name(prefix + i).roles(roles).build();
    }

    private Future<Void> joinNodeAsync(final JoinRequest joinRequest) {
        final PlainActionFuture<Void> future = new PlainActionFuture<>() {
            private final String description = "join of " + joinRequest + "]";

            @Override
            public String toString() {
                return "future [" + description + "]";
            }
        };

        logger.debug("starting {}", future);
        // clone the node before submitting to simulate an incoming join, which is guaranteed to have a new
        // disco node object serialized off the network
        try {
            final RequestHandlerRegistry<JoinRequest> joinHandler = transport.getRequestHandlers().getHandler(JoinHelper.JOIN_ACTION_NAME);
            final ActionListener<TransportResponse> listener = new ActionListener<>() {

                @Override
                public void onResponse(TransportResponse transportResponse) {
                    logger.debug("{} completed", future);
                    future.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error(() -> format("unexpected error for %s", future), e);
                    future.onFailure(e);
                }
            };

            joinHandler.processMessageReceived(joinRequest, new TestTransportChannel(listener));
        } catch (Exception e) {
            logger.error(() -> format("unexpected error for %s", future), e);
            future.onFailure(e);
        }
        return future;
    }

    private void joinNode(final JoinRequest joinRequest) {
        FutureUtils.get(joinNodeAsync(joinRequest));
    }

    private void joinNodeAndRun(final JoinRequest joinRequest) {
        Future<Void> fut = joinNodeAsync(joinRequest);
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(fut.isDone());
        FutureUtils.get(fut);
    }

    public void testJoinWithHigherTermElectsLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        TransportVersion version1 = TransportVersionUtils.randomVersion();
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(randomFrom(node0, node1))),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        assertNull(coordinator.getStateForMasterService().nodes().getMasterNodeId());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        Future<Void> fut = joinNodeAsync(
            new JoinRequest(node1, version1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion)))
        );
        assertEquals(Coordinator.Mode.LEADER, coordinator.getMode());
        assertNull(coordinator.getStateForMasterService().nodes().getMasterNodeId());
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(fut.isDone());
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(coordinator.getStateForMasterService().nodes().isLocalNodeElectedMaster());
    }

    public void testJoinWithHigherTermButBetterStateGetsRejected() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        TransportVersion version1 = TransportVersionUtils.randomVersion();
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node1)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long higherVersion = initialVersion + randomLongBetween(1, 10);
        expectThrows(
            CoordinationStateRejectedException.class,
            () -> joinNodeAndRun(
                new JoinRequest(node1, version1, newTerm, Optional.of(new Join(node1, node0, newTerm, initialTerm, higherVersion)))
            )
        );
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testJoinWithHigherTermButBetterStateStillElectsMasterThroughSelfJoin() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long higherVersion = initialVersion + randomLongBetween(1, 10);
        joinNodeAndRun(
            new JoinRequest(
                node1,
                TransportVersion.current(),
                newTerm,
                Optional.of(new Join(node1, node0, newTerm, initialTerm, higherVersion))
            )
        );
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinElectedLeader() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        joinNodeAndRun(
            new JoinRequest(
                node0,
                TransportVersion.current(),
                newTerm,
                Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))
            )
        );
        assertTrue(isLocalNodeElectedMaster());
        assertFalse(clusterStateHasNode(node1));
        joinNodeAndRun(
            new JoinRequest(
                node1,
                TransportVersion.current(),
                newTerm,
                Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion))
            )
        );
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
    }

    public void testJoinElectedLeaderWithHigherTerm() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        long newTerm = initialTerm + randomLongBetween(1, 10);

        joinNodeAndRun(
            new JoinRequest(
                node0,
                TransportVersion.current(),
                newTerm,
                Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))
            )
        );
        assertTrue(isLocalNodeElectedMaster());

        long newerTerm = newTerm + randomLongBetween(1, 10);
        joinNodeAndRun(new JoinRequest(node1, TransportVersion.current(), newerTerm, Optional.empty()));
        assertThat(coordinator.getCurrentTerm(), greaterThanOrEqualTo(newerTerm));
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinAccumulation() {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        DiscoveryNode node2 = newNode(2, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node2)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        assertFalse(isLocalNodeElectedMaster());
        long newTerm = initialTerm + randomLongBetween(1, 10);
        Future<Void> futNode0 = joinNodeAsync(
            new JoinRequest(
                node0,
                TransportVersion.current(),
                newTerm,
                Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion))
            )
        );
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(futNode0.isDone());
        assertFalse(isLocalNodeElectedMaster());
        Future<Void> futNode1 = joinNodeAsync(
            new JoinRequest(
                node1,
                TransportVersion.current(),
                newTerm,
                Optional.of(new Join(node1, node0, newTerm, initialTerm, initialVersion))
            )
        );
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(futNode1.isDone());
        assertFalse(isLocalNodeElectedMaster());
        joinNodeAndRun(
            new JoinRequest(
                node2,
                TransportVersion.current(),
                newTerm,
                Optional.of(new Join(node2, node0, newTerm, initialTerm, initialVersion))
            )
        );
        assertTrue(isLocalNodeElectedMaster());
        assertTrue(clusterStateHasNode(node1));
        assertTrue(clusterStateHasNode(node2));
        FutureUtils.get(futNode0);
        FutureUtils.get(futNode1);
    }

    public void testJoinFollowerWithHigherTerm() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        long newTerm = initialTerm + randomLongBetween(1, 10);
        handleStartJoinFrom(node1, newTerm);
        handleFollowerCheckFrom(node1, newTerm);
        long newerTerm = newTerm + randomLongBetween(1, 10);
        joinNodeAndRun(
            new JoinRequest(
                node1,
                TransportVersion.current(),
                newerTerm,
                Optional.of(new Join(node1, node0, newerTerm, initialTerm, initialVersion))
            )
        );
        assertTrue(isLocalNodeElectedMaster());
    }

    public void testJoinUpdateVotingConfigExclusion() throws Exception {
        DiscoveryNode initialNode = newNode(0, true);
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);

        CoordinationMetadata.VotingConfigExclusion votingConfigExclusion = new CoordinationMetadata.VotingConfigExclusion(
            CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER,
            "knownNodeName"
        );

        setupFakeMasterServiceAndCoordinator(
            initialTerm,
            buildStateWithVotingConfigExclusion(initialNode, initialTerm, initialVersion, votingConfigExclusion),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );

        DiscoveryNode knownJoiningNode = DiscoveryNodeUtils.builder("newNodeId")
            .name("knownNodeName")
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
            .build();
        long newTerm = initialTerm + randomLongBetween(1, 10);
        long newerTerm = newTerm + randomLongBetween(1, 10);

        joinNodeAndRun(
            new JoinRequest(
                knownJoiningNode,
                TransportVersion.current(),
                initialTerm,
                Optional.of(new Join(knownJoiningNode, initialNode, newerTerm, initialTerm, initialVersion))
            )
        );

        assertTrue(MasterServiceTests.discoveryState(masterService).getVotingConfigExclusions().stream().anyMatch(exclusion -> {
            return "knownNodeName".equals(exclusion.getNodeName()) && "newNodeId".equals(exclusion.getNodeId());
        }));
    }

    private ClusterState buildStateWithVotingConfigExclusion(
        DiscoveryNode initialNode,
        long initialTerm,
        long initialVersion,
        CoordinationMetadata.VotingConfigExclusion votingConfigExclusion
    ) {
        ClusterState initialState = initialState(
            initialNode,
            initialTerm,
            initialVersion,
            new VotingConfiguration(Collections.singleton(initialNode.getId()))
        );
        Metadata newMetadata = Metadata.builder(initialState.metadata())
            .coordinationMetadata(
                CoordinationMetadata.builder(initialState.coordinationMetadata()).addVotingConfigExclusion(votingConfigExclusion).build()
            )
            .build();

        return ClusterState.builder(initialState).metadata(newMetadata).build();
    }

    private void handleStartJoinFrom(DiscoveryNode node, long term) throws Exception {
        final RequestHandlerRegistry<StartJoinRequest> startJoinHandler = transport.getRequestHandlers()
            .getHandler(JoinHelper.START_JOIN_ACTION_NAME);
        startJoinHandler.processMessageReceived(new StartJoinRequest(node, term), new TestTransportChannel(new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse transportResponse) {

            }

            @Override
            public void onFailure(Exception e) {
                fail();
            }
        }));
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(isLocalNodeElectedMaster());
        assertThat(coordinator.getMode(), equalTo(Coordinator.Mode.CANDIDATE));
    }

    private void handleFollowerCheckFrom(DiscoveryNode node, long term) throws Exception {
        final RequestHandlerRegistry<FollowersChecker.FollowerCheckRequest> followerCheckHandler = transport.getRequestHandlers()
            .getHandler(FollowersChecker.FOLLOWER_CHECK_ACTION_NAME);
        final TestTransportChannel channel = new TestTransportChannel(new ActionListener<>() {
            @Override
            public void onResponse(TransportResponse transportResponse) {

            }

            @Override
            public void onFailure(Exception e) {
                fail();
            }
        });
        followerCheckHandler.processMessageReceived(new FollowersChecker.FollowerCheckRequest(term, node), channel);
        // Will throw exception if failed
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(isLocalNodeElectedMaster());
        assertThat(coordinator.getMode(), equalTo(Coordinator.Mode.FOLLOWER));
    }

    public void testJoinFollowerFails() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        TransportVersion version1 = TransportVersionUtils.randomVersion();
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node0)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        long newTerm = initialTerm + randomLongBetween(1, 10);
        handleStartJoinFrom(node1, newTerm);
        handleFollowerCheckFrom(node1, newTerm);
        assertThat(
            expectThrows(
                CoordinationStateRejectedException.class,
                () -> joinNodeAndRun(new JoinRequest(node1, version1, newTerm, Optional.empty()))
            ).getMessage(),
            containsString("join target is a follower")
        );
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testBecomeFollowerFailsPendingJoin() throws Exception {
        DiscoveryNode node0 = newNode(0, true);
        DiscoveryNode node1 = newNode(1, true);
        TransportVersion version0 = TransportVersionUtils.randomVersion();
        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupFakeMasterServiceAndCoordinator(
            initialTerm,
            initialState(node0, initialTerm, initialVersion, VotingConfiguration.of(node1)),
            () -> new StatusInfo(HEALTHY, "healthy-info")
        );
        long newTerm = initialTerm + randomLongBetween(1, 10);
        Future<Void> fut = joinNodeAsync(
            new JoinRequest(node0, version0, newTerm, Optional.of(new Join(node0, node0, newTerm, initialTerm, initialVersion)))
        );
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(fut.isDone());
        assertFalse(isLocalNodeElectedMaster());
        handleFollowerCheckFrom(node1, newTerm);
        assertFalse(isLocalNodeElectedMaster());
        assertThat(
            expectThrows(CoordinationStateRejectedException.class, () -> FutureUtils.get(fut)).getMessage(),
            containsString("became follower")
        );
        assertFalse(isLocalNodeElectedMaster());
    }

    public void testConcurrentJoining() {
        List<DiscoveryNode> masterNodes = IntStream.rangeClosed(1, randomIntBetween(2, 5))
            .mapToObj(nodeId -> newNode(nodeId, true))
            .toList();
        List<DiscoveryNode> otherNodes = IntStream.rangeClosed(masterNodes.size() + 1, masterNodes.size() + 1 + randomIntBetween(0, 5))
            .mapToObj(nodeId -> newNode(nodeId, false))
            .toList();
        List<DiscoveryNode> allNodes = Stream.concat(masterNodes.stream(), otherNodes.stream()).toList();

        DiscoveryNode localNode = masterNodes.get(0);
        VotingConfiguration votingConfiguration = new VotingConfiguration(
            randomValueOtherThan(singletonList(localNode), () -> randomSubsetOf(randomIntBetween(1, masterNodes.size()), masterNodes))
                .stream()
                .map(DiscoveryNode::getId)
                .collect(Collectors.toSet())
        );

        logger.info("Voting configuration: {}", votingConfiguration);

        long initialTerm = randomLongBetween(1, 10);
        long initialVersion = randomLongBetween(1, 10);
        setupRealMasterServiceAndCoordinator(initialTerm, initialState(localNode, initialTerm, initialVersion, votingConfiguration));
        long newTerm = initialTerm + randomLongBetween(1, 10);

        // we need at least a quorum of voting nodes with a correct term and worse state
        List<DiscoveryNode> successfulNodes;
        do {
            successfulNodes = randomSubsetOf(allNodes);
        } while (votingConfiguration.hasQuorum(successfulNodes.stream().map(DiscoveryNode::getId).toList()) == false);

        logger.info("Successful voting nodes: {}", successfulNodes);

        List<JoinRequest> correctJoinRequests = successfulNodes.stream()
            .map(
                node -> new JoinRequest(
                    node,
                    TransportVersion.current(),
                    newTerm,
                    Optional.of(new Join(node, localNode, newTerm, initialTerm, initialVersion))
                )
            )
            .toList();

        List<DiscoveryNode> possiblyUnsuccessfulNodes = new ArrayList<>(allNodes);
        possiblyUnsuccessfulNodes.removeAll(successfulNodes);

        logger.info("Possibly unsuccessful voting nodes: {}", possiblyUnsuccessfulNodes);

        List<JoinRequest> possiblyFailingJoinRequests = possiblyUnsuccessfulNodes.stream().map(node -> {
            if (randomBoolean()) {
                // a correct request
                return new JoinRequest(
                    node,
                    TransportVersion.current(),
                    newTerm,
                    Optional.of(new Join(node, localNode, newTerm, initialTerm, initialVersion))
                );
            } else if (randomBoolean()) {
                // term too low
                return new JoinRequest(
                    node,
                    TransportVersion.current(),
                    newTerm,
                    Optional.of(new Join(node, localNode, randomLongBetween(0, initialTerm), initialTerm, initialVersion))
                );
            } else {
                // better state
                return new JoinRequest(
                    node,
                    TransportVersion.current(),
                    newTerm,
                    Optional.of(new Join(node, localNode, newTerm, initialTerm, initialVersion + randomLongBetween(1, 10)))
                );
            }
        }).collect(Collectors.toCollection(ArrayList::new));

        // duplicate some requests, which will be unsuccessful
        possiblyFailingJoinRequests.addAll(randomSubsetOf(possiblyFailingJoinRequests));

        final CyclicBarrier barrier = new CyclicBarrier(correctJoinRequests.size() + possiblyFailingJoinRequests.size() + 1);

        final AtomicBoolean stopAsserting = new AtomicBoolean();
        final Thread assertionThread = new Thread(() -> {
            safeAwait(barrier);
            while (stopAsserting.get() == false) {
                coordinator.invariant();
            }
        }, "assert invariants");

        final List<Thread> joinThreads = Stream.concat(correctJoinRequests.stream().map(joinRequest -> new Thread(() -> {
            safeAwait(barrier);
            joinNode(joinRequest);
        }, "process " + joinRequest)), possiblyFailingJoinRequests.stream().map(joinRequest -> new Thread(() -> {
            safeAwait(barrier);
            try {
                joinNode(joinRequest);
            } catch (CoordinationStateRejectedException e) {
                // ignore - these requests are expected to fail
            }
        }, "process " + joinRequest))).toList();

        assertionThread.start();
        joinThreads.forEach(Thread::start);
        joinThreads.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        stopAsserting.set(true);
        try {
            assertionThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertTrue(MasterServiceTests.discoveryState(masterService).nodes().isLocalNodeElectedMaster());
        for (DiscoveryNode successfulNode : successfulNodes) {
            assertTrue(successfulNode + " joined cluster", clusterStateHasNode(successfulNode));
            assertFalse(successfulNode + " voted for master", coordinator.missingJoinVoteFrom(successfulNode));
        }
    }

    private boolean isLocalNodeElectedMaster() {
        return MasterServiceTests.discoveryState(masterService).nodes().isLocalNodeElectedMaster();
    }

    private boolean clusterStateHasNode(DiscoveryNode node) {
        return node.equals(MasterServiceTests.discoveryState(masterService).nodes().get(node.getId()));
    }
}
