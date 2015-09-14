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

package org.elasticsearch.discovery.zen.publish;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.transport.local.LocalTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.*;

@TestLogging("discovery.zen.publish:TRACE")
public class PublishClusterStateActionTests extends ESTestCase {

    protected ThreadPool threadPool;
    protected Map<String, MockNode> nodes = new HashMap<>();

    public static class MockNode implements PublishClusterStateAction.NewPendingClusterStateListener, DiscoveryNodesProvider {
        public final DiscoveryNode discoveryNode;
        public final MockTransportService service;
        public MockPublishAction action;
        public final ClusterStateListener listener;

        public volatile ClusterState clusterState;

        private final ESLogger logger;

        public MockNode(DiscoveryNode discoveryNode, MockTransportService service, @Nullable ClusterStateListener listener, ESLogger logger) {
            this.discoveryNode = discoveryNode;
            this.service = service;
            this.listener = listener;
            this.logger = logger;
            this.clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(DiscoveryNodes.builder().put(discoveryNode).localNodeId(discoveryNode.id()).build()).build();
        }

        public MockNode setAsMaster() {
            this.clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).masterNodeId(discoveryNode.id())).build();
            return this;
        }

        public MockNode resetMasterId() {
            this.clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).masterNodeId(null)).build();
            return this;
        }


        public void connectTo(DiscoveryNode node) {
            service.connectToNode(node);
        }

        @Override
        public void onNewClusterState(String reason) {
            ClusterState newClusterState = action.pendingStatesQueue().getNextClusterStateToProcess();
            logger.debug("[{}] received version [{}], uuid [{}]", discoveryNode.name(), newClusterState.version(), newClusterState.stateUUID());
            if (listener != null) {
                ClusterChangedEvent event = new ClusterChangedEvent("", newClusterState, clusterState);
                listener.clusterChanged(event);
            }
            if (clusterState.nodes().masterNode() == null || newClusterState.supersedes(clusterState)) {
                clusterState = newClusterState;
            }
            action.pendingStatesQueue().markAsProcessed(newClusterState);
        }

        @Override
        public DiscoveryNodes nodes() {
            return clusterState.nodes();
        }

        @Override
        public NodeService nodeService() {
            assert false;
            throw new UnsupportedOperationException("Shouldn't be here");
        }
    }

    public MockNode createMockNode(final String name) throws Exception {
        return createMockNode(name, Settings.EMPTY, Version.CURRENT);
    }

    public MockNode createMockNode(String name, Settings settings) throws Exception {
        return createMockNode(name, settings, Version.CURRENT);
    }

    public MockNode createMockNode(final String name, Settings settings, Version version) throws Exception {
        return createMockNode(name, settings, version, null);
    }

    public MockNode createMockNode(String name, Settings settings, Version version, @Nullable ClusterStateListener listener) throws Exception {
        settings = Settings.builder()
                .put("name", name)
                .put(TransportService.SETTING_TRACE_LOG_INCLUDE, "", TransportService.SETTING_TRACE_LOG_EXCLUDE, "NOTHING")
                .put(settings)
                .build();

        MockTransportService service = buildTransportService(settings, version);
        DiscoveryNode discoveryNode = new DiscoveryNode(name, name, service.boundAddress().publishAddress(),
                settings.getByPrefix("node.").getAsMap(), version);
        MockNode node = new MockNode(discoveryNode, service, listener, logger);
        node.action = buildPublishClusterStateAction(settings, service, node, node);
        final CountDownLatch latch = new CountDownLatch(nodes.size() * 2 + 1);
        TransportConnectionListener waitForConnection = new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node) {
                latch.countDown();
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node) {
                fail("disconnect should not be called " + node);
            }
        };
        node.service.addConnectionListener(waitForConnection);
        for (MockNode curNode : nodes.values()) {
            curNode.service.addConnectionListener(waitForConnection);
            curNode.connectTo(node.discoveryNode);
            node.connectTo(curNode.discoveryNode);
        }
        node.connectTo(node.discoveryNode);
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
        threadPool = new ThreadPool(getClass().getName());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        for (MockNode curNode : nodes.values()) {
            curNode.action.close();
            curNode.service.close();
        }
        terminate(threadPool);
    }

    protected MockTransportService buildTransportService(Settings settings, Version version) {
        MockTransportService transportService = new MockTransportService(settings, new LocalTransport(settings, threadPool, version, new NamedWriteableRegistry()), threadPool);
        transportService.start();
        return transportService;
    }

    protected MockPublishAction buildPublishClusterStateAction(Settings settings, MockTransportService transportService, DiscoveryNodesProvider nodesProvider,
                                                               PublishClusterStateAction.NewPendingClusterStateListener listener) {
        DiscoverySettings discoverySettings = new DiscoverySettings(settings, new NodeSettingsService(settings));
        return new MockPublishAction(settings, transportService, nodesProvider, listener, discoverySettings, ClusterName.DEFAULT);
    }

    @Test
    public void testSimpleClusterStatePublishing() throws Exception {
        MockNode nodeA = createMockNode("nodeA", Settings.EMPTY, Version.CURRENT).setAsMaster();
        MockNode nodeB = createMockNode("nodeB", Settings.EMPTY, Version.CURRENT);

        // Initial cluster state
        ClusterState clusterState = nodeA.clusterState;

        // cluster state update - add nodeB
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder(clusterState.nodes()).put(nodeB.discoveryNode).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeB.clusterState, clusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder().addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
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

        MockNode nodeC = createMockNode("nodeC", Settings.EMPTY, Version.CURRENT);

        // cluster state update 3 - register node C
        previousClusterState = clusterState;
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).put(nodeC.discoveryNode).build();
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
        // First state
        assertSameStateFromFull(nodeC.clusterState, clusterState);

        // cluster state update 4 - update settings
        previousClusterState = clusterState;
        MetaData metaData = MetaData.builder(clusterState.metaData()).transientSettings(Settings.settingsBuilder().put("foo", "bar").build()).build();
        clusterState = ClusterState.builder(clusterState).metaData(metaData).incrementVersion().build();
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
                .put(nodeA.discoveryNode)
                .put(nodeB.discoveryNode)
                .put(nodeC.discoveryNode)
                .masterNodeId(nodeB.discoveryNode.id())
                .localNodeId(nodeB.discoveryNode.id())
                .build();
        previousClusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateAndWait(nodeB.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeA.clusterState, clusterState);
        assertSameStateFromFull(nodeC.clusterState, clusterState);
    }

    @Test
    public void testUnexpectedDiffPublishing() throws Exception {

        MockNode nodeA = createMockNode("nodeA", Settings.EMPTY, Version.CURRENT, new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                fail("Shouldn't send cluster state to myself");
            }
        }).setAsMaster();

        MockNode nodeB = createMockNode("nodeB", Settings.EMPTY, Version.CURRENT);

        // Initial cluster state with both states - the second node still shouldn't get diff even though it's present in the previous cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder(nodeA.nodes()).put(nodeB.discoveryNode).build();
        ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();
        ClusterState clusterState = ClusterState.builder(previousClusterState).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeB.clusterState, clusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder().addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
    }

    @Test
    public void testDisablingDiffPublishing() throws Exception {
        Settings noDiffPublishingSettings = Settings.builder().put(DiscoverySettings.PUBLISH_DIFF_ENABLE, false).build();

        MockNode nodeA = createMockNode("nodeA", noDiffPublishingSettings, Version.CURRENT, new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                fail("Shouldn't send cluster state to myself");
            }
        });

        MockNode nodeB = createMockNode("nodeB", noDiffPublishingSettings, Version.CURRENT, new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                assertFalse(event.state().wasReadFromDiff());
            }
        });

        // Initial cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().put(nodeA.discoveryNode).localNodeId(nodeA.discoveryNode.id()).masterNodeId(nodeA.discoveryNode.id()).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();

        // cluster state update - add nodeB
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).put(nodeB.discoveryNode).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder().addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
    }


    /**
     * Test not waiting on publishing works correctly (i.e., publishing times out)
     */
    @Test
    public void testSimultaneousClusterStatePublishing() throws Exception {
        int numberOfNodes = randomIntBetween(2, 10);
        int numberOfIterations = scaledRandomIntBetween(5, 50);
        Settings settings = Settings.builder().put(DiscoverySettings.PUBLISH_DIFF_ENABLE, randomBoolean()).build();
        MockNode master = createMockNode("node0", settings, Version.CURRENT, new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                assertProperMetaDataForVersion(event.state().metaData(), event.state().version());
            }
        }).setAsMaster();
        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder(master.nodes());
        for (int i = 1; i < numberOfNodes; i++) {
            final String name = "node" + i;
            final MockNode node = createMockNode(name, settings, Version.CURRENT, new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    assertProperMetaDataForVersion(event.state().metaData(), event.state().version());
                }
            });
            discoveryNodesBuilder.put(node.discoveryNode);
        }

        AssertingAckListener[] listeners = new AssertingAckListener[numberOfIterations];
        DiscoveryNodes discoveryNodes = discoveryNodesBuilder.build();
        MetaData metaData = MetaData.EMPTY_META_DATA;
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        ClusterState previousState;
        for (int i = 0; i < numberOfIterations; i++) {
            previousState = clusterState;
            metaData = buildMetaDataForVersion(metaData, i + 1);
            clusterState = ClusterState.builder(clusterState).incrementVersion().metaData(metaData).nodes(discoveryNodes).build();
            listeners[i] = publishState(master.action, clusterState, previousState);
        }

        for (int i = 0; i < numberOfIterations; i++) {
            listeners[i].await(1, TimeUnit.SECONDS);
        }

        // set the master cs
        master.clusterState = clusterState;

        for (MockNode node : nodes.values()) {
            assertSameState(node.clusterState, clusterState);
            assertThat(node.clusterState.nodes().localNode(), equalTo(node.discoveryNode));
        }
    }

    @Test
    public void testSerializationFailureDuringDiffPublishing() throws Exception {

        MockNode nodeA = createMockNode("nodeA", Settings.EMPTY, Version.CURRENT, new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                fail("Shouldn't send cluster state to myself");
            }
        }).setAsMaster();

        MockNode nodeB = createMockNode("nodeB", Settings.EMPTY, Version.CURRENT);

        // Initial cluster state with both states - the second node still shouldn't get diff even though it's present in the previous cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder(nodeA.nodes()).put(nodeB.discoveryNode).build();
        ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();
        ClusterState clusterState = ClusterState.builder(previousClusterState).incrementVersion().build();
        publishStateAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeB.clusterState, clusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder().addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();

        ClusterState unserializableClusterState = new ClusterState(clusterState.version(), clusterState.stateUUID(), clusterState) {
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
        } catch (Discovery.FailedToCommitClusterStateException e) {
            assertThat(e.getCause(), notNullValue());
            assertThat(e.getCause().getMessage(), containsString("failed to serialize"));
        }
    }


    public void testFailToPublishWithLessThanMinMasterNodes() throws Exception {
        final int masterNodes = randomIntBetween(1, 10);

        MockNode master = createMockNode("master");
        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder().put(master.discoveryNode);
        for (int i = 1; i < masterNodes; i++) {
            discoveryNodesBuilder.put(createMockNode("node" + i).discoveryNode);
        }
        final int dataNodes = randomIntBetween(0, 5);
        final Settings dataSettings = Settings.builder().put("node.master", false).build();
        for (int i = 0; i < dataNodes; i++) {
            discoveryNodesBuilder.put(createMockNode("data_" + i, dataSettings).discoveryNode);
        }
        discoveryNodesBuilder.localNodeId(master.discoveryNode.id()).masterNodeId(master.discoveryNode.id());
        DiscoveryNodes discoveryNodes = discoveryNodesBuilder.build();
        MetaData metaData = MetaData.EMPTY_META_DATA;
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).nodes(discoveryNodes).build();
        ClusterState previousState = master.clusterState;
        try {
            publishState(master.action, clusterState, previousState, masterNodes + randomIntBetween(1, 5));
            fail("cluster state publishing didn't fail despite of not having enough nodes");
        } catch (Discovery.FailedToCommitClusterStateException expected) {
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
        settings.put(DiscoverySettings.COMMIT_TIMEOUT, expectingToCommit == false && timeOutNodes > 0 ? "100ms" : "1h")
                .put(DiscoverySettings.PUBLISH_TIMEOUT, "5ms"); // test is about committing

        MockNode master = createMockNode("master", settings.build());

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

        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder().put(master.discoveryNode);
        for (int i = 0; i < nodeTypes.length; i++) {
            final MockNode mockNode = createMockNode("node" + i);
            discoveryNodesBuilder.put(mockNode.discoveryNode);
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
            final MockNode mockNode = createMockNode("data_" + i, Settings.builder().put("node.master", false).build());
            discoveryNodesBuilder.put(mockNode.discoveryNode);
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

        discoveryNodesBuilder.localNodeId(master.discoveryNode.id()).masterNodeId(master.discoveryNode.id());
        DiscoveryNodes discoveryNodes = discoveryNodesBuilder.build();
        MetaData metaData = MetaData.EMPTY_META_DATA;
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).nodes(discoveryNodes).build();
        ClusterState previousState = master.clusterState;
        try {
            publishState(master.action, clusterState, previousState, minMasterNodes);
            if (expectingToCommit == false) {
                fail("cluster state publishing didn't fail despite of not have enough nodes");
            }
        } catch (Discovery.FailedToCommitClusterStateException exception) {
            logger.debug("failed to publish as expected", exception);
            if (expectingToCommit) {
                throw exception;
            }
            assertThat(exception.getMessage(), containsString(timeOutNodes > 0 ? "timed out" : "failed"));
        }
    }

    public void testIncomingClusterStateValidation() throws Exception {
        MockNode node = createMockNode("node");

        logger.info("--> testing acceptances of any master when having no master");
        ClusterState state = ClusterState.builder(node.clusterState)
                .nodes(DiscoveryNodes.builder(node.nodes()).masterNodeId(randomAsciiOfLength(10))).incrementVersion().build();
        node.action.validateIncomingState(state, null);

        // now set a master node
        node.clusterState = ClusterState.builder(node.clusterState).nodes(DiscoveryNodes.builder(node.nodes()).masterNodeId("master")).build();
        logger.info("--> testing rejection of another master");
        try {
            node.action.validateIncomingState(state, node.clusterState);
            fail("node accepted state from another master");
        } catch (IllegalStateException OK) {
        }

        logger.info("--> test state from the current master is accepted");
        node.action.validateIncomingState(ClusterState.builder(node.clusterState)
                .nodes(DiscoveryNodes.builder(node.nodes()).masterNodeId("master")).build(), node.clusterState);


        logger.info("--> testing rejection of another cluster name");
        try {
            node.action.validateIncomingState(ClusterState.builder(new ClusterName(randomAsciiOfLength(10))).nodes(node.nodes()).build(), node.clusterState);
            fail("node accepted state with another cluster name");
        } catch (IllegalStateException OK) {
        }

        logger.info("--> testing rejection of a cluster state with wrong local node");
        try {
            state = ClusterState.builder(node.clusterState)
                    .nodes(DiscoveryNodes.builder(node.nodes()).localNodeId("_non_existing_").build())
                    .incrementVersion().build();
            node.action.validateIncomingState(state, node.clusterState);
            fail("node accepted state with non-existence local node");
        } catch (IllegalStateException OK) {
        }

        try {
            MockNode otherNode = createMockNode("otherNode");
            state = ClusterState.builder(node.clusterState).nodes(
                    DiscoveryNodes.builder(node.nodes()).put(otherNode.discoveryNode).localNodeId(otherNode.discoveryNode.id()).build()
            ).incrementVersion().build();
            node.action.validateIncomingState(state, node.clusterState);
            fail("node accepted state with existent but wrong local node");
        } catch (IllegalStateException OK) {
        }

        logger.info("--> testing acceptance of an old cluster state");
        state = node.clusterState;
        node.clusterState = ClusterState.builder(node.clusterState).incrementVersion().build();
        node.action.validateIncomingState(state, node.clusterState);

        // an older version from a *new* master is also OK!
        ClusterState previousState = ClusterState.builder(node.clusterState).incrementVersion().build();
        state = ClusterState.builder(node.clusterState)
                .nodes(DiscoveryNodes.builder(node.clusterState.nodes()).masterNodeId("_new_master_").build())
                .build();
        // remove the master of the node (but still have a previous cluster state with it)!
        node.resetMasterId();

        node.action.validateIncomingState(state, previousState);
    }

    public void testInterleavedPublishCommit() throws Throwable {
        MockNode node = createMockNode("node").setAsMaster();
        final CapturingTransportChannel channel = new CapturingTransportChannel();

        List<ClusterState> states = new ArrayList<>();
        final int numOfStates = scaledRandomIntBetween(3, 10);
        for (int i = 1; i <= numOfStates; i++) {
            states.add(ClusterState.builder(node.clusterState).version(i).stateUUID(ClusterState.UNKNOWN_UUID).build());
        }

        final ClusterState finalState = states.get(numOfStates - 1);
        Collections.shuffle(states, random());

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

        Collections.shuffle(states, random());
        for (ClusterState state : states) {
            node.action.handleCommitRequest(new PublishClusterStateAction.CommitClusterStateRequest(state.stateUUID()), channel);
            assertThat(channel.response.get(), equalTo((TransportResponse) TransportResponse.Empty.INSTANCE));
            if (channel.error.get() != null) {
                throw channel.error.get();
            }
        }
        channel.clear();

        //now check the last state held
        assertSameState(node.clusterState, finalState);
    }

    /**
     * Tests that cluster is committed or times out. It should never be the case that we fail
     * an update due to a commit timeout, but it ends up being committed anyway
     */
    public void testTimeoutOrCommit() throws Exception {
        Settings settings = Settings.builder()
                .put(DiscoverySettings.COMMIT_TIMEOUT, "1ms").build(); // short but so we will sometime commit sometime timeout

        MockNode master = createMockNode("master", settings);
        MockNode node = createMockNode("node", settings);
        ClusterState state = ClusterState.builder(master.clusterState)
                .nodes(DiscoveryNodes.builder(master.clusterState.nodes()).put(node.discoveryNode).masterNodeId(master.discoveryNode.id())).build();

        for (int i = 0; i < 10; i++) {
            state = ClusterState.builder(state).incrementVersion().build();
            logger.debug("--> publishing version [{}], UUID [{}]", state.version(), state.stateUUID());
            boolean success;
            try {
                publishState(master.action, state, master.clusterState, 2).await(1, TimeUnit.HOURS);
                success = true;
            } catch (Discovery.FailedToCommitClusterStateException OK) {
                success = false;
            }
            logger.debug("--> publishing [{}], verifying...", success ? "succeeded" : "failed");

            if (success) {
                assertSameState(node.clusterState, state);
            } else {
                assertThat(node.clusterState.stateUUID(), not(equalTo(state.stateUUID())));
            }
        }
    }


    private MetaData buildMetaDataForVersion(MetaData metaData, long version) {
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.builder(metaData.indices());
        indices.put("test" + version, IndexMetaData.builder("test" + version).settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards((int) version).numberOfReplicas(0).build());
        return MetaData.builder(metaData)
                .transientSettings(Settings.builder().put("test", version).build())
                .indices(indices.build())
                .build();
    }

    private void assertProperMetaDataForVersion(MetaData metaData, long version) {
        for (long i = 1; i <= version; i++) {
            assertThat(metaData.index("test" + i), notNullValue());
            assertThat(metaData.index("test" + i).numberOfShards(), equalTo((int) i));
        }
        assertThat(metaData.index("test" + (version + 1)), nullValue());
        assertThat(metaData.transientSettings().get("test"), equalTo(Long.toString(version)));
    }

    public void publishStateAndWait(PublishClusterStateAction action, ClusterState state, ClusterState previousState) throws InterruptedException {
        publishState(action, state, previousState).await(1, TimeUnit.SECONDS);
    }

    public AssertingAckListener publishState(PublishClusterStateAction action, ClusterState state, ClusterState previousState) throws InterruptedException {
        final int minimumMasterNodes = randomIntBetween(-1, state.nodes().getMasterNodes().size());
        return publishState(action, state, previousState, minimumMasterNodes);
    }

    public AssertingAckListener publishState(PublishClusterStateAction action, ClusterState state, ClusterState previousState, int minMasterNodes) throws InterruptedException {
        AssertingAckListener assertingAckListener = new AssertingAckListener(state.nodes().getSize() - 1);
        ClusterChangedEvent changedEvent = new ClusterChangedEvent("test update", state, previousState);
        action.publish(changedEvent, minMasterNodes, assertingAckListener);
        return assertingAckListener;
    }

    public static class AssertingAckListener implements Discovery.AckListener {
        private final List<Tuple<DiscoveryNode, Throwable>> errors = new CopyOnWriteArrayList<>();
        private final AtomicBoolean timeoutOccurred = new AtomicBoolean();
        private final CountDownLatch countDown;

        public AssertingAckListener(int nodeCount) {
            countDown = new CountDownLatch(nodeCount);
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Throwable t) {
            if (t != null) {
                errors.add(new Tuple<>(node, t));
            }
            countDown.countDown();
        }

        @Override
        public void onTimeout() {
            timeoutOccurred.set(true);
            // Fast forward the counter - no reason to wait here
            long currentCount = countDown.getCount();
            for (long i = 0; i < currentCount; i++) {
                countDown.countDown();
            }
        }

        public void await(long timeout, TimeUnit unit) throws InterruptedException {
            assertThat(awaitErrors(timeout, unit), emptyIterable());
        }

        public List<Tuple<DiscoveryNode, Throwable>> awaitErrors(long timeout, TimeUnit unit) throws InterruptedException {
            countDown.await(timeout, unit);
            assertFalse(timeoutOccurred.get());
            return errors;
        }

    }

    void assertSameState(ClusterState actual, ClusterState expected) {
        assertThat(actual, notNullValue());
        final String reason = "\n--> actual ClusterState: " + actual.prettyPrint() + "\n--> expected ClusterState:" + expected.prettyPrint();
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

    static class MockPublishAction extends PublishClusterStateAction {

        AtomicBoolean timeoutOnSend = new AtomicBoolean();
        AtomicBoolean errorOnSend = new AtomicBoolean();
        AtomicBoolean timeoutOnCommit = new AtomicBoolean();
        AtomicBoolean errorOnCommit = new AtomicBoolean();

        public MockPublishAction(Settings settings, TransportService transportService, DiscoveryNodesProvider nodesProvider, NewPendingClusterStateListener listener, DiscoverySettings discoverySettings, ClusterName clusterName) {
            super(settings, transportService, nodesProvider, listener, discoverySettings, clusterName);
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
        public String action() {
            return "_noop_";
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
        public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
            this.response.set(response);
            assertThat(error.get(), nullValue());
        }

        @Override
        public void sendResponse(Throwable error) throws IOException {
            this.error.set(error);
            assertThat(response.get(), nullValue());
        }
    }
}
