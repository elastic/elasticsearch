/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesTransportRequest;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PublishClusterStateActionTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY);

    protected ThreadPool threadPool;
    protected Map<String, MockNode> nodes = new HashMap<>();

    public static class MockNode implements PublishClusterStateAction.IncomingClusterStateListener {
        public final DiscoveryNode discoveryNode;
        public final MockTransportService service;
        public MockPublishAction action;
        public final ClusterStateListener listener;
        private final PendingClusterStatesQueue pendingStatesQueue;

        public volatile ClusterState clusterState;

        private final Logger logger;

        public MockNode(DiscoveryNode discoveryNode, MockTransportService service,
                        @Nullable ClusterStateListener listener, Logger logger) {
            this.discoveryNode = discoveryNode;
            this.service = service;
            this.listener = listener;
            this.logger = logger;
            this.clusterState = ClusterState.builder(CLUSTER_NAME).nodes(DiscoveryNodes.builder()
                .add(discoveryNode).localNodeId(discoveryNode.getId()).build()).build();
            this.pendingStatesQueue = new PendingClusterStatesQueue(logger, 25);
        }

        public MockNode setAsMaster() {
            this.clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .masterNodeId(discoveryNode.getId())).build();
            return this;
        }

        public MockNode resetMasterId() {
            this.clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .masterNodeId(null)).build();
            return this;
        }


        public void connectTo(DiscoveryNode node) {
            service.connectToNode(node);
        }

        @Override
        public void onIncomingClusterState(ClusterState incomingState) {
            ZenDiscovery.validateIncomingState(logger, incomingState, clusterState);
            pendingStatesQueue.addPending(incomingState);
        }

        public void onClusterStateCommitted(String stateUUID, ActionListener<Void> processedListener) {
            final ClusterState state = pendingStatesQueue.markAsCommitted(stateUUID,
                new PendingClusterStatesQueue.StateProcessedListener() {
                    @Override
                    public void onNewClusterStateProcessed() {
                        processedListener.onResponse(null);
                    }

                    @Override
                    public void onNewClusterStateFailed(Exception e) {
                        processedListener.onFailure(e);
                    }
                });
            if (state != null) {
                ClusterState newClusterState = pendingStatesQueue.getNextClusterStateToProcess();
                logger.debug("[{}] received version [{}], uuid [{}]",
                    discoveryNode.getName(), newClusterState.version(), newClusterState.stateUUID());
                if (listener != null) {
                    ClusterChangedEvent event = new ClusterChangedEvent("", newClusterState, clusterState);
                    listener.clusterChanged(event);
                }
                if (clusterState.nodes().getMasterNode() == null || newClusterState.supersedes(clusterState)) {
                    clusterState = newClusterState;
                }
                pendingStatesQueue.markAsProcessed(newClusterState);
            }
        }

        public DiscoveryNodes nodes() {
            return clusterState.nodes();
        }

    }

    public MockNode createMockNode(final String name) throws Exception {
        return createMockNode(name, Settings.EMPTY, null);
    }

    public MockNode createMockNode(String name, final Settings basSettings, @Nullable ClusterStateListener listener) throws Exception {
        return createMockNode(name, basSettings, listener, threadPool, logger, nodes);
    }

    public static MockNode createMockNode(String name, final Settings basSettings, @Nullable ClusterStateListener listener,
                                          ThreadPool threadPool, Logger logger, Map<String, MockNode> nodes) throws Exception {
        final Settings settings = Settings.builder()
                .put("name", name)
                .put(TransportSettings.TRACE_LOG_INCLUDE_SETTING.getKey(), "").put(
                     TransportSettings.TRACE_LOG_EXCLUDE_SETTING.getKey(), "NOTHING")
                .put(basSettings)
                .build();

        MockTransportService service = buildTransportService(settings, threadPool);
        DiscoveryNode discoveryNode = service.getLocalDiscoNode();
        MockNode node = new MockNode(discoveryNode, service, listener, logger);
        node.action = buildPublishClusterStateAction(settings, service, node);
        final CountDownLatch latch = new CountDownLatch(nodes.size() * 2);
        TransportConnectionListener waitForConnection = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                latch.countDown();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                fail("disconnect should not be called " + node);
            }
        };
        node.service.addConnectionListener(waitForConnection);
        for (MockNode curNode : nodes.values()) {
            curNode.service.addConnectionListener(waitForConnection);
            curNode.connectTo(node.discoveryNode);
            node.connectTo(curNode.discoveryNode);
        }
        assertThat("failed to wait for all nodes to connect", latch.await(5, TimeUnit.SECONDS), equalTo(true));
        for (MockNode curNode : nodes.values()) {
            curNode.service.removeConnectionListener(waitForConnection);
        }
        node.service.removeConnectionListener(waitForConnection);
        if (nodes.put(name, node) != null) {
            fail("Node with the name " + name + " already exist");
        }
        return node;
    }

    public MockTransportService service(String name) {
        MockNode node = nodes.get(name);
        if (node != null) {
            return node.service;
        }
        return null;
    }

    public PublishClusterStateAction action(String name) {
        MockNode node = nodes.get(name);
        if (node != null) {
            return node.action;
        }
        return null;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        for (MockNode curNode : nodes.values()) {
            curNode.service.close();
        }
        terminate(threadPool);
    }

    private static MockTransportService buildTransportService(Settings settings, ThreadPool threadPool) {
        MockTransportService transportService = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null);
        transportService.start();
        transportService.acceptIncomingRequests();
        return transportService;
    }

    private static MockPublishAction buildPublishClusterStateAction(
            Settings settings,
            MockTransportService transportService,
            PublishClusterStateAction.IncomingClusterStateListener listener
    ) {
        DiscoverySettings discoverySettings =
                new DiscoverySettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        return new MockPublishAction(
                transportService,
                namedWriteableRegistry,
                listener,
                discoverySettings);
    }

    public void testSimpleClusterStatePublishing() throws Exception {
        MockNode nodeA = createMockNode("nodeA").setAsMaster();
        MockNode nodeB = createMockNode("nodeB");

        // Initial cluster state
        ClusterState clusterState = nodeA.clusterState;

        // cluster state update - add nodeB
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder(clusterState.nodes()).add(nodeB.discoveryNode).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeB.clusterState, clusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder()
            .addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
        assertThat(nodeB.clusterState.blocks().global().size(), equalTo(1));

        // cluster state update - remove block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
        assertTrue(nodeB.clusterState.wasReadFromDiff());

        // Adding new node - this node should get full cluster state while nodeB should still be getting diffs

        MockNode nodeC = createMockNode("nodeC");

        // cluster state update 3 - register node C
        previousClusterState = clusterState;
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).add(nodeC.discoveryNode).build();
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
        // First state
        assertSameStateFromFull(nodeC.clusterState, clusterState);

        // cluster state update 4 - update settings
        previousClusterState = clusterState;
        Metadata metadata = Metadata.builder(clusterState.metadata())
            .transientSettings(Settings.builder().put("foo", "bar").build()).build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
        assertThat(nodeB.clusterState.blocks().global().size(), equalTo(0));
        assertSameStateFromDiff(nodeC.clusterState, clusterState);
        assertThat(nodeC.clusterState.blocks().global().size(), equalTo(0));

        // cluster state update - skipping one version change - should request full cluster state
        previousClusterState = ClusterState.builder(clusterState).incrementVersion().build();
        clusterState = ClusterState.builder(clusterState).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeB.clusterState, clusterState);
        assertSameStateFromFull(nodeC.clusterState, clusterState);
        assertFalse(nodeC.clusterState.wasReadFromDiff());

        // node A steps down from being master
        nodeA.resetMasterId();
        nodeB.resetMasterId();
        nodeC.resetMasterId();

        // node B becomes the master and sends a version of the cluster state that goes back
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes)
                .add(nodeA.discoveryNode)
                .add(nodeB.discoveryNode)
                .add(nodeC.discoveryNode)
                .masterNodeId(nodeB.discoveryNode.getId())
                .localNodeId(nodeB.discoveryNode.getId())
                .build();
        previousClusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateAndWait(nodeB.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeA.clusterState, clusterState);
        assertSameStateFromFull(nodeC.clusterState, clusterState);
    }

    public void testUnexpectedDiffPublishing() throws Exception {
        MockNode nodeA = createMockNode("nodeA", Settings.EMPTY, event -> {
            fail("Shouldn't send cluster state to myself");
        }).setAsMaster();

        MockNode nodeB = createMockNode("nodeB");

        // Initial cluster state with both states - the second node still shouldn't
        // get diff even though it's present in the previous cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder(nodeA.nodes()).add(nodeB.discoveryNode).build();
        ClusterState previousClusterState = ClusterState.builder(CLUSTER_NAME).nodes(discoveryNodes).build();
        ClusterState clusterState = ClusterState.builder(previousClusterState).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeB.clusterState, clusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder()
            .addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
    }

    public void testDisablingDiffPublishing() throws Exception {
        Settings noDiffPublishingSettings = Settings.builder().put(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.getKey(), false).build();

        MockNode nodeA = createMockNode("nodeA", noDiffPublishingSettings, new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                fail("Shouldn't send cluster state to myself");
            }
        });

        MockNode nodeB = createMockNode("nodeB", noDiffPublishingSettings, new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                assertFalse(event.state().wasReadFromDiff());
            }
        });

        // Initial cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(nodeA.discoveryNode).localNodeId(nodeA.discoveryNode.getId()).masterNodeId(nodeA.discoveryNode.getId()).build();
        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME).nodes(discoveryNodes).build();

        // cluster state update - add nodeB
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).add(nodeB.discoveryNode).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder()
            .addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);

        assertWarnings(
            "[discovery.zen.publish_diff.enable] setting was deprecated in Elasticsearch and will be removed in a future release! " +
                "See the breaking changes documentation for the next major version.");
    }


    /**
     * Test not waiting on publishing works correctly (i.e., publishing times out)
     */
    public void testSimultaneousClusterStatePublishing() throws Exception {
        int numberOfNodes = randomIntBetween(2, 10);
        int numberOfIterations = scaledRandomIntBetween(5, 50);
        Settings settings = Settings.builder().put(DiscoverySettings.PUBLISH_DIFF_ENABLE_SETTING.getKey(), randomBoolean()).build();
        MockNode master = createMockNode("node0", settings, new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                assertProperMetadataForVersion(event.state().metadata(), event.state().version());
            }
        }).setAsMaster();
        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder(master.nodes());
        for (int i = 1; i < numberOfNodes; i++) {
            final String name = "node" + i;
            final MockNode node = createMockNode(name, settings, new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    assertProperMetadataForVersion(event.state().metadata(), event.state().version());
                }
            });
            discoveryNodesBuilder.add(node.discoveryNode);
        }

        AssertingAckListener[] listeners = new AssertingAckListener[numberOfIterations];
        DiscoveryNodes discoveryNodes = discoveryNodesBuilder.build();
        Metadata metadata = Metadata.EMPTY_METADATA;
        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME).metadata(metadata).build();
        ClusterState previousState;
        for (int i = 0; i < numberOfIterations; i++) {
            previousState = clusterState;
            metadata = buildMetadataForVersion(metadata, i + 1);
            clusterState = ClusterState.builder(clusterState).incrementVersion().metadata(metadata).nodes(discoveryNodes).build();
            listeners[i] = publishState(master.action, clusterState, previousState);
        }

        for (int i = 0; i < numberOfIterations; i++) {
            listeners[i].await(1, TimeUnit.SECONDS);
        }

        // set the master cs
        master.clusterState = clusterState;

        for (MockNode node : nodes.values()) {
            assertSameState(node.clusterState, clusterState);
            assertThat(node.clusterState.nodes().getLocalNode(), equalTo(node.discoveryNode));
        }

        assertWarnings(
            "[discovery.zen.publish_diff.enable] setting was deprecated in Elasticsearch and will be removed in a future release! " +
                "See the breaking changes documentation for the next major version.");
    }

    public void testSerializationFailureDuringDiffPublishing() throws Exception {
        MockNode nodeA = createMockNode("nodeA", Settings.EMPTY, new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                fail("Shouldn't send cluster state to myself");
            }
        }).setAsMaster();

        MockNode nodeB = createMockNode("nodeB");

        // Initial cluster state with both states - the second node still shouldn't get
        // diff even though it's present in the previous cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder(nodeA.nodes()).add(nodeB.discoveryNode).build();
        ClusterState previousClusterState = ClusterState.builder(CLUSTER_NAME).nodes(discoveryNodes).build();
        ClusterState clusterState = ClusterState.builder(previousClusterState).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeB.clusterState, clusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder()
            .addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();

        ClusterState unserializableClusterState = new ClusterState(clusterState.version(), clusterState.stateUUID(),
                                                                   clusterState) {
            @Override
            public Diff<ClusterState> diff(ClusterState previousState) {
                return new Diff<ClusterState>() {
                    @Override
                    public ClusterState apply(ClusterState part) {
                        fail("this diff shouldn't be applied");
                        return part;
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        throw new IOException("Simulated failure of diff serialization");
                    }
                };
            }
        };
        try {
            publishStateAndWait(nodeA.action, unserializableClusterState, previousClusterState);
            fail("cluster state published despite of diff errors");
        } catch (FailedToCommitClusterStateException e) {
            assertThat(e.getCause(), notNullValue());
            assertThat(e.getCause().getMessage(), containsString("failed to serialize"));
        }
    }


    public void testFailToPublishWithLessThanMinMasterNodes() throws Exception {
        final int masterNodes = randomIntBetween(1, 10);

        MockNode master = createMockNode("master");
        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder().add(master.discoveryNode);
        for (int i = 1; i < masterNodes; i++) {
            discoveryNodesBuilder.add(createMockNode("node" + i).discoveryNode);
        }
        final int dataNodes = randomIntBetween(0, 5);
        final Settings dataSettings = Settings.builder().put(nonMasterNode()).build();
        for (int i = 0; i < dataNodes; i++) {
            discoveryNodesBuilder.add(createMockNode("data_" + i, dataSettings, null).discoveryNode);
        }
        discoveryNodesBuilder.localNodeId(master.discoveryNode.getId()).masterNodeId(master.discoveryNode.getId());
        DiscoveryNodes discoveryNodes = discoveryNodesBuilder.build();
        Metadata metadata = Metadata.EMPTY_METADATA;
        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME).metadata(metadata).nodes(discoveryNodes).build();
        ClusterState previousState = master.clusterState;
        try {
            publishState(master.action, clusterState, previousState, masterNodes + randomIntBetween(1, 5));
            fail("cluster state publishing didn't fail despite of not having enough nodes");
        } catch (FailedToCommitClusterStateException expected) {
            logger.debug("failed to publish as expected", expected);
        }
    }

    public void testPublishingWithSendingErrors() throws Exception {
        int goodNodes = randomIntBetween(2, 5);
        int errorNodes = randomIntBetween(1, 5);
        int timeOutNodes = randomBoolean() ? 0 : randomIntBetween(1, 5); // adding timeout nodes will force timeout errors
        final int numberOfMasterNodes = goodNodes + errorNodes + timeOutNodes + 1; // master
        final boolean expectingToCommit = randomBoolean();
        Settings.Builder settings = Settings.builder();
        // make sure we have a reasonable timeout if we expect to timeout, o.w. one that will make the test "hang"
        settings.put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), expectingToCommit == false && timeOutNodes > 0 ? "100ms" : "1h")
                .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "5ms"); // test is about committing

        MockNode master = createMockNode("master", settings.build(), null);

        // randomize things a bit
        int[] nodeTypes = new int[goodNodes + errorNodes + timeOutNodes];
        for (int i = 0; i < goodNodes; i++) {
            nodeTypes[i] = 0;
        }
        for (int i = goodNodes; i < goodNodes + errorNodes; i++) {
            nodeTypes[i] = 1;
        }
        for (int i = goodNodes + errorNodes; i < nodeTypes.length; i++) {
            nodeTypes[i] = 2;
        }
        Collections.shuffle(Arrays.asList(nodeTypes), random());

        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder().add(master.discoveryNode);
        for (int i = 0; i < nodeTypes.length; i++) {
            final MockNode mockNode = createMockNode("node" + i);
            discoveryNodesBuilder.add(mockNode.discoveryNode);
            switch (nodeTypes[i]) {
                case 1:
                    mockNode.action.errorOnSend.set(true);
                    break;
                case 2:
                    mockNode.action.timeoutOnSend.set(true);
                    break;
            }
        }
        final int dataNodes = randomIntBetween(0, 3); // data nodes don't matter
        for (int i = 0; i < dataNodes; i++) {
            final MockNode mockNode = createMockNode("data_" + i,
                Settings.builder().put(nonMasterNode()).build(), null);
            discoveryNodesBuilder.add(mockNode.discoveryNode);
            if (randomBoolean()) {
                // we really don't care - just chaos monkey
                mockNode.action.errorOnCommit.set(randomBoolean());
                mockNode.action.errorOnSend.set(randomBoolean());
                mockNode.action.timeoutOnCommit.set(randomBoolean());
                mockNode.action.timeoutOnSend.set(randomBoolean());
            }
        }

        final int minMasterNodes;
        final String expectedBehavior;
        if (expectingToCommit) {
            minMasterNodes = randomIntBetween(0, goodNodes + 1); // count master
            expectedBehavior = "succeed";
        } else {
            minMasterNodes = randomIntBetween(goodNodes + 2, numberOfMasterNodes); // +2 because of master
            expectedBehavior = timeOutNodes > 0 ? "timeout" : "fail";
        }
        logger.info("--> expecting commit to {}. good nodes [{}], errors [{}], timeouts [{}]. min_master_nodes [{}]",
                expectedBehavior, goodNodes + 1, errorNodes, timeOutNodes, minMasterNodes);

        discoveryNodesBuilder.localNodeId(master.discoveryNode.getId()).masterNodeId(master.discoveryNode.getId());
        DiscoveryNodes discoveryNodes = discoveryNodesBuilder.build();
        Metadata metadata = Metadata.EMPTY_METADATA;
        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME).metadata(metadata).nodes(discoveryNodes).build();
        ClusterState previousState = master.clusterState;
        try {
            publishState(master.action, clusterState, previousState, minMasterNodes);
            if (expectingToCommit == false) {
                fail("cluster state publishing didn't fail despite of not have enough nodes");
            }
        } catch (FailedToCommitClusterStateException exception) {
            logger.debug("failed to publish as expected", exception);
            if (expectingToCommit) {
                throw exception;
            }
            assertThat(exception.getMessage(), containsString(timeOutNodes > 0 ? "timed out" : "failed"));
        }

        assertWarnings(
            "[discovery.zen.publish_timeout] setting was deprecated in Elasticsearch and will be removed in a future release! " +
                "See the breaking changes documentation for the next major version.",
            "[discovery.zen.commit_timeout] setting was deprecated in Elasticsearch and will be removed in a future release! " +
                "See the breaking changes documentation for the next major version.");
    }

    public void testOutOfOrderCommitMessages() throws Throwable {
        MockNode node = createMockNode("node").setAsMaster();
        final CapturingTransportChannel channel = new CapturingTransportChannel();

        List<ClusterState> states = new ArrayList<>();
        final int numOfStates = scaledRandomIntBetween(3, 25);
        for (int i = 1; i <= numOfStates; i++) {
            states.add(ClusterState.builder(node.clusterState).version(i).stateUUID(ClusterState.UNKNOWN_UUID).build());
        }

        final ClusterState finalState = states.get(numOfStates - 1);

        logger.info("--> publishing states");
        for (ClusterState state : states) {
            node.action.handleIncomingClusterStateRequest(
                new BytesTransportRequest(PublishClusterStateAction.serializeFullClusterState(state, Version.CURRENT), Version.CURRENT),
                channel);
            assertThat(channel.response.get(), equalTo((TransportResponse) TransportResponse.Empty.INSTANCE));
            assertThat(channel.error.get(), nullValue());
            channel.clear();

        }

        logger.info("--> committing states");

        long largestVersionSeen = Long.MIN_VALUE;
        Randomness.shuffle(states);
        for (ClusterState state : states) {
            node.action.handleCommitRequest(new PublishClusterStateAction.CommitClusterStateRequest(state.stateUUID()), channel);
            if (largestVersionSeen < state.getVersion()) {
                assertThat(channel.response.get(), equalTo((TransportResponse) TransportResponse.Empty.INSTANCE));
                if (channel.error.get() != null) {
                    throw channel.error.get();
                }
                largestVersionSeen = state.getVersion();
            } else {
                // older cluster states will be rejected
                assertNotNull(channel.error.get());
                assertThat(channel.error.get(), instanceOf(IllegalStateException.class));
            }
            channel.clear();
        }

        //now check the last state held
        assertSameState(node.clusterState, finalState);
    }

    /**
     * Tests that cluster is committed or times out. It should never be the case that we fail
     * an update due to a commit timeout, but it ends up being committed anyway
     */
    public void testTimeoutOrCommit() throws Exception {
        Settings settings = Settings.builder()
            // short but so we will sometime commit sometime timeout
            .put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), "1ms").build();

        MockNode master = createMockNode("master", settings, null);
        MockNode node = createMockNode("node", settings, null);
        ClusterState state = ClusterState.builder(master.clusterState)
                .nodes(DiscoveryNodes.builder(master.clusterState.nodes())
                    .add(node.discoveryNode).masterNodeId(master.discoveryNode.getId())).build();

        for (int i = 0; i < 10; i++) {
            state = ClusterState.builder(state).incrementVersion().build();
            logger.debug("--> publishing version [{}], UUID [{}]", state.version(), state.stateUUID());
            boolean success;
            try {
                publishState(master.action, state, master.clusterState, 2).await(1, TimeUnit.HOURS);
                success = true;
            } catch (FailedToCommitClusterStateException OK) {
                success = false;
            }
            logger.debug("--> publishing [{}], verifying...", success ? "succeeded" : "failed");

            if (success) {
                assertSameState(node.clusterState, state);
            } else {
                assertThat(node.clusterState.stateUUID(), not(equalTo(state.stateUUID())));
            }
        }

        assertWarnings(
            "[discovery.zen.commit_timeout] setting was deprecated in Elasticsearch and will be removed in a future release! " +
                "See the breaking changes documentation for the next major version.");
    }

    private void assertPublishClusterStateStats(String description, MockNode node, long expectedFull, long expectedIncompatibleDiffs,
                                                long expectedCompatibleDiffs) {
        PublishClusterStateStats stats = node.action.stats();
        assertThat(description + ": full cluster states", stats.getFullClusterStateReceivedCount(), equalTo(expectedFull));
        assertThat(description + ": incompatible cluster state diffs", stats.getIncompatibleClusterStateDiffReceivedCount(),
            equalTo(expectedIncompatibleDiffs));
        assertThat(description + ": compatible cluster state diffs", stats.getCompatibleClusterStateDiffReceivedCount(),
            equalTo(expectedCompatibleDiffs));
    }

    public void testPublishClusterStateStats() throws Exception {
        MockNode nodeA = createMockNode("nodeA").setAsMaster();
        MockNode nodeB = createMockNode("nodeB");

        assertPublishClusterStateStats("nodeA: initial state", nodeA, 0, 0, 0);
        assertPublishClusterStateStats("nodeB: initial state", nodeB, 0, 0, 0);

        // Initial cluster state
        ClusterState clusterState = nodeA.clusterState;

        // cluster state update - add nodeB
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder(clusterState.nodes()).add(nodeB.discoveryNode).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);

        // Sent as a full cluster state update
        assertPublishClusterStateStats("nodeA: after full update", nodeA, 0, 0, 0);
        assertPublishClusterStateStats("nodeB: after full update", nodeB, 1, 0, 0);

        // Increment cluster state version
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);

        // Sent, successfully, as a cluster state diff
        assertPublishClusterStateStats("nodeA: after successful diff update", nodeA, 0, 0, 0);
        assertPublishClusterStateStats("nodeB: after successful diff update", nodeB, 1, 0, 1);

        // Increment cluster state version twice
        previousClusterState = ClusterState.builder(clusterState).incrementVersion().build();
        clusterState = ClusterState.builder(previousClusterState).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);

        // Sent, unsuccessfully, as a diff and then retried as a full update
        assertPublishClusterStateStats("nodeA: after unsuccessful diff update", nodeA, 0, 0, 0);
        assertPublishClusterStateStats("nodeB: after unsuccessful diff update", nodeB, 2, 1, 1);

        // node A steps down from being master
        nodeA.resetMasterId();
        nodeB.resetMasterId();

        // node B becomes the master and sends a version of the cluster state that goes back
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes)
            .add(nodeA.discoveryNode)
            .add(nodeB.discoveryNode)
            .masterNodeId(nodeB.discoveryNode.getId())
            .localNodeId(nodeB.discoveryNode.getId())
            .build();
        previousClusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateAndWait(nodeB.action, clusterState, previousClusterState);

        // Sent, unsuccessfully, as a diff, and then retried as a full update
        assertPublishClusterStateStats("nodeA: B became master", nodeA, 1, 1, 0);
        assertPublishClusterStateStats("nodeB: B became master", nodeB, 2, 1, 1);
    }

    private Metadata buildMetadataForVersion(Metadata metadata, long version) {
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.builder(metadata.indices());
        indices.put("test" + version, IndexMetadata.builder("test" + version)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards((int) version).numberOfReplicas(0).build());
        return Metadata.builder(metadata)
                .transientSettings(Settings.builder().put("test", version).build())
                .indices(indices.build())
                .build();
    }

    private void assertProperMetadataForVersion(Metadata metadata, long version) {
        for (long i = 1; i <= version; i++) {
            assertThat(metadata.index("test" + i), notNullValue());
            assertThat(metadata.index("test" + i).getNumberOfShards(), equalTo((int) i));
        }
        assertThat(metadata.index("test" + (version + 1)), nullValue());
        assertThat(metadata.transientSettings().get("test"), equalTo(Long.toString(version)));
    }

    public void publishStateAndWait(PublishClusterStateAction action, ClusterState state,
                                    ClusterState previousState) throws InterruptedException {
        publishState(action, state, previousState).await(1, TimeUnit.SECONDS);
    }

    public AssertingAckListener publishState(PublishClusterStateAction action, ClusterState state,
                                             ClusterState previousState) throws InterruptedException {
        final int minimumMasterNodes = randomIntBetween(-1, state.nodes().getMasterNodes().size());
        return publishState(action, state, previousState, minimumMasterNodes);
    }

    public AssertingAckListener publishState(PublishClusterStateAction action, ClusterState state,
                                             ClusterState previousState, int minMasterNodes) throws InterruptedException {
        AssertingAckListener assertingAckListener = new AssertingAckListener(state.nodes().getSize() - 1);
        ClusterStatePublicationEvent clusterStatePublicationEvent
            = new ClusterStatePublicationEvent("test update", previousState, state, 0L, 0L);
        action.publish(clusterStatePublicationEvent, minMasterNodes, assertingAckListener);
        return assertingAckListener;
    }

    public static class AssertingAckListener implements Discovery.AckListener {
        private final List<Tuple<DiscoveryNode, Throwable>> errors = new CopyOnWriteArrayList<>();
        private final Set<DiscoveryNode> successfulAcks = Collections.synchronizedSet(new HashSet<>());
        private final CountDownLatch countDown;
        private final CountDownLatch commitCountDown;

        public AssertingAckListener(int nodeCount) {
            countDown = new CountDownLatch(nodeCount);
            commitCountDown = new CountDownLatch(1);
        }

        @Override
        public void onCommit(TimeValue commitTime) {
            commitCountDown.countDown();
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            if (e != null) {
                errors.add(new Tuple<>(node, e));
            } else {
                successfulAcks.add(node);
            }
            countDown.countDown();
        }

        public Set<DiscoveryNode> await(long timeout, TimeUnit unit) throws InterruptedException {
            assertThat(awaitErrors(timeout, unit), emptyIterable());
            assertTrue(commitCountDown.await(timeout, unit));
            return new HashSet<>(successfulAcks);
        }

        public List<Tuple<DiscoveryNode, Throwable>> awaitErrors(long timeout, TimeUnit unit) throws InterruptedException {
            countDown.await(timeout, unit);
            return errors;
        }

    }

    void assertSameState(ClusterState actual, ClusterState expected) {
        assertThat(actual, notNullValue());
        final String reason = "\n--> actual ClusterState: " + actual + "\n" +
                                "--> expected ClusterState:" + expected;
        assertThat("unequal UUIDs" + reason, actual.stateUUID(), equalTo(expected.stateUUID()));
        assertThat("unequal versions" + reason, actual.version(), equalTo(expected.version()));
    }

    void assertSameStateFromDiff(ClusterState actual, ClusterState expected) {
        assertSameState(actual, expected);
        assertTrue(actual.wasReadFromDiff());
    }

    void assertSameStateFromFull(ClusterState actual, ClusterState expected) {
        assertSameState(actual, expected);
        assertFalse(actual.wasReadFromDiff());
    }

    public static class MockPublishAction extends PublishClusterStateAction {

        AtomicBoolean timeoutOnSend = new AtomicBoolean();
        AtomicBoolean errorOnSend = new AtomicBoolean();
        AtomicBoolean timeoutOnCommit = new AtomicBoolean();
        AtomicBoolean errorOnCommit = new AtomicBoolean();

        public MockPublishAction(TransportService transportService, NamedWriteableRegistry namedWriteableRegistry,
                                 IncomingClusterStateListener listener, DiscoverySettings discoverySettings) {
            super(transportService, namedWriteableRegistry, listener, discoverySettings);
        }

        @Override
        protected void handleIncomingClusterStateRequest(BytesTransportRequest request, TransportChannel channel) throws IOException {
            if (errorOnSend.get()) {
                throw new ElasticsearchException("forced error on incoming cluster state");
            }
            if (timeoutOnSend.get()) {
                return;
            }
            super.handleIncomingClusterStateRequest(request, channel);
        }

        @Override
        protected void handleCommitRequest(PublishClusterStateAction.CommitClusterStateRequest request, TransportChannel channel) {
            if (errorOnCommit.get()) {
                throw new ElasticsearchException("forced error on incoming commit");
            }
            if (timeoutOnCommit.get()) {
                return;
            }
            super.handleCommitRequest(request, channel);
        }
    }

    static class CapturingTransportChannel implements TransportChannel {

        AtomicReference<TransportResponse> response = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();

        public void clear() {
            response.set(null);
            error.set(null);
        }

        @Override
        public String getProfileName() {
            return "_noop_";
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            this.response.set(response);
            assertThat(error.get(), nullValue());
        }

        @Override
        public void sendResponse(Exception exception) throws IOException {
            this.error.set(exception);
            assertThat(response.get(), nullValue());
        }

        @Override
        public String getChannelType() {
            return "capturing";
        }
    }
}
