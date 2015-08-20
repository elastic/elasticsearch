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

package org.elasticsearch.cluster;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
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
import org.elasticsearch.discovery.zen.publish.PublishClusterStateAction;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.local.LocalTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.Matchers.*;

public class PublishClusterStateActionTests extends ESTestCase {

    protected ThreadPool threadPool;
    protected Map<String, MockNode> nodes = newHashMap();

    public static class MockNode implements PublishClusterStateAction.NewClusterStateListener {
        public final DiscoveryNode discoveryNode;
        public final MockTransportService service;
        public PublishClusterStateAction action;
        public final MockDiscoveryNodesProvider nodesProvider;
        public final ClusterStateListener listener;

        public volatile ClusterState clusterState;

        private final ESLogger logger;

        public MockNode(DiscoveryNode discoveryNode, MockTransportService service, MockDiscoveryNodesProvider nodesProvider, @Nullable ClusterStateListener listener, ESLogger logger) {
            this.discoveryNode = discoveryNode;
            this.service = service;
            this.nodesProvider = nodesProvider;
            this.listener = listener;
            this.logger = logger;
            this.clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(DiscoveryNodes.builder().put(discoveryNode).localNodeId(discoveryNode.id()).build()).build();
        }

        public void connectTo(DiscoveryNode node) {
            service.connectToNode(node);
            nodesProvider.addNode(node);
        }

        @Override
        public void onNewClusterState(ClusterState newClusterState, NewStateProcessed newStateProcessed) {
            logger.debug("[{}] received version [{}], uuid [{}]", discoveryNode.name(), newClusterState.version(), newClusterState.stateUUID());
            if (listener != null) {
                ClusterChangedEvent event = new ClusterChangedEvent("", newClusterState, clusterState);
                listener.clusterChanged(event);
            }
            clusterState = newClusterState;
            newStateProcessed.onNewClusterStateProcessed();
        }
    }

    public MockNode createMockNode(final String name, Settings settings, Version version) throws Exception {
        return createMockNode(name, settings, version, null);
    }

    public MockNode createMockNode(String name, Settings settings, Version version, @Nullable ClusterStateListener listener) throws Exception {
        MockTransportService service = buildTransportService(
                Settings.builder().put(settings).put("name", name, TransportService.SETTING_TRACE_LOG_INCLUDE, "", TransportService.SETTING_TRACE_LOG_EXCLUDE, "NOTHING").build(),
                version
        );
        DiscoveryNode discoveryNode = new DiscoveryNode(name, name, service.boundAddress().publishAddress(), ImmutableMap.<String, String>of(), version);
        MockDiscoveryNodesProvider nodesProvider = new MockDiscoveryNodesProvider(discoveryNode);
        MockNode node = new MockNode(discoveryNode, service, nodesProvider, listener, logger);
        nodesProvider.addNode(discoveryNode);
        node.action = buildPublishClusterStateAction(settings, service, nodesProvider, node);
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

    protected PublishClusterStateAction buildPublishClusterStateAction(Settings settings, MockTransportService transportService, MockDiscoveryNodesProvider nodesProvider,
                                                                       PublishClusterStateAction.NewClusterStateListener listener) {
        DiscoverySettings discoverySettings = new DiscoverySettings(settings, new NodeSettingsService(settings));
        return new PublishClusterStateAction(settings, transportService, nodesProvider, listener, discoverySettings, ClusterName.DEFAULT);
    }


    static class MockDiscoveryNodesProvider implements DiscoveryNodesProvider {

        private DiscoveryNodes discoveryNodes = DiscoveryNodes.EMPTY_NODES;

        public MockDiscoveryNodesProvider(DiscoveryNode localNode) {
            discoveryNodes = DiscoveryNodes.builder().put(localNode).localNodeId(localNode.id()).build();
        }

        public void addNode(DiscoveryNode node) {
            discoveryNodes = DiscoveryNodes.builder(discoveryNodes).put(node).build();
        }

        @Override
        public DiscoveryNodes nodes() {
            return discoveryNodes;
        }

        @Override
        public NodeService nodeService() {
            assert false;
            throw new UnsupportedOperationException("Shouldn't be here");
        }
    }


    @Test
    @TestLogging("cluster:DEBUG,discovery.zen.publish:DEBUG")
    public void testSimpleClusterStatePublishing() throws Exception {
        MockNode nodeA = createMockNode("nodeA", Settings.EMPTY, Version.CURRENT);
        MockNode nodeB = createMockNode("nodeB", Settings.EMPTY, Version.CURRENT);

        // Initial cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().put(nodeA.discoveryNode).localNodeId(nodeA.discoveryNode.id()).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();

        // cluster state update - add nodeB
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).put(nodeB.discoveryNode).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeB.clusterState, clusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder().addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
        assertThat(nodeB.clusterState.blocks().global().size(), equalTo(1));

        // cluster state update - remove block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).incrementVersion().build();
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
        assertTrue(nodeB.clusterState.wasReadFromDiff());

        // Adding new node - this node should get full cluster state while nodeB should still be getting diffs

        MockNode nodeC = createMockNode("nodeC", Settings.EMPTY, Version.CURRENT);

        // cluster state update 3 - register node C
        previousClusterState = clusterState;
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).put(nodeC.discoveryNode).build();
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
        // First state
        assertSameStateFromFull(nodeC.clusterState, clusterState);

        // cluster state update 4 - update settings
        previousClusterState = clusterState;
        MetaData metaData = MetaData.builder(clusterState.metaData()).transientSettings(Settings.settingsBuilder().put("foo", "bar").build()).build();
        clusterState = ClusterState.builder(clusterState).metaData(metaData).incrementVersion().build();
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
        assertThat(nodeB.clusterState.blocks().global().size(), equalTo(0));
        assertSameStateFromDiff(nodeC.clusterState, clusterState);
        assertThat(nodeC.clusterState.blocks().global().size(), equalTo(0));

        // cluster state update - skipping one version change - should request full cluster state
        previousClusterState = ClusterState.builder(clusterState).incrementVersion().build();
        clusterState = ClusterState.builder(clusterState).incrementVersion().build();
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeB.clusterState, clusterState);
        assertSameStateFromFull(nodeC.clusterState, clusterState);
        assertFalse(nodeC.clusterState.wasReadFromDiff());

        // node B becomes the master and sends a version of the cluster state that goes back
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes)
                .put(nodeA.discoveryNode)
                .put(nodeB.discoveryNode)
                .put(nodeC.discoveryNode)
                .build();
        previousClusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateDiffAndWait(nodeB.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeA.clusterState, clusterState);
        assertSameStateFromFull(nodeC.clusterState, clusterState);
    }

    @Test
    @TestLogging("cluster:DEBUG,discovery.zen.publish:DEBUG")
    public void testUnexpectedDiffPublishing() throws Exception {

        MockNode nodeA = createMockNode("nodeA", Settings.EMPTY, Version.CURRENT, new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                fail("Shouldn't send cluster state to myself");
            }
        });

        MockNode nodeB = createMockNode("nodeB", Settings.EMPTY, Version.CURRENT);

        // Initial cluster state with both states - the second node still shouldn't get diff even though it's present in the previous cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().put(nodeA.discoveryNode).put(nodeB.discoveryNode).localNodeId(nodeA.discoveryNode.id()).build();
        ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();
        ClusterState clusterState = ClusterState.builder(previousClusterState).incrementVersion().build();
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromFull(nodeB.clusterState, clusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder().addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);
        assertSameStateFromDiff(nodeB.clusterState, clusterState);
    }

    @Test
    @TestLogging("cluster:DEBUG,discovery.zen.publish:DEBUG")
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
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().put(nodeA.discoveryNode).localNodeId(nodeA.discoveryNode.id()).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();

        // cluster state update - add nodeB
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).put(nodeB.discoveryNode).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder().addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);
    }


    @Test
    @TestLogging("cluster:DEBUG,discovery.zen.publish:DEBUG")
    /**
     * Test concurrent publishing works correctly (although not strictly required, it's a good testamne
     */
    public void testSimultaneousClusterStatePublishing() throws Exception {
        int numberOfNodes = randomIntBetween(2, 10);
        int numberOfIterations = randomIntBetween(50, 200);
        Settings settings = Settings.builder().put(DiscoverySettings.PUBLISH_DIFF_ENABLE, randomBoolean()).build();
        MockNode[] nodes = new MockNode[numberOfNodes];
        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < nodes.length; i++) {
            final String name = "node" + i;
            nodes[i] = createMockNode(name, settings, Version.CURRENT, new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    assertProperMetaDataForVersion(event.state().metaData(), event.state().version());
                }
            });
            discoveryNodesBuilder.put(nodes[i].discoveryNode);
        }

        AssertingAckListener[] listeners = new AssertingAckListener[numberOfIterations];
        discoveryNodesBuilder.localNodeId(nodes[0].discoveryNode.id());
        DiscoveryNodes discoveryNodes = discoveryNodesBuilder.build();
        MetaData metaData = MetaData.EMPTY_META_DATA;
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        ClusterState previousState;
        for (int i = 0; i < numberOfIterations; i++) {
            previousState = clusterState;
            metaData = buildMetaDataForVersion(metaData, i + 1);
            clusterState = ClusterState.builder(clusterState).incrementVersion().metaData(metaData).nodes(discoveryNodes).build();
            listeners[i] = publishStateDiff(nodes[0].action, clusterState, previousState);
        }

        for (int i = 0; i < numberOfIterations; i++) {
            listeners[i].await(1, TimeUnit.SECONDS);
        }

        // fake node[0] - it is the master
        nodes[0].clusterState = clusterState;

        for (MockNode node : nodes) {
            assertThat(node.discoveryNode + " misses a cluster state", node.clusterState, notNullValue());
            assertThat(node.discoveryNode + " unexpected cluster state: " + node.clusterState, node.clusterState.version(), equalTo(clusterState.version()));
            assertThat(node.clusterState.nodes().localNode(), equalTo(node.discoveryNode));
        }
    }

    @Test
    @TestLogging("cluster:DEBUG,discovery.zen.publish:DEBUG")
    public void testSerializationFailureDuringDiffPublishing() throws Exception {

        MockNode nodeA = createMockNode("nodeA", Settings.EMPTY, Version.CURRENT, new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                fail("Shouldn't send cluster state to myself");
            }
        });

        MockNode nodeB = createMockNode("nodeB", Settings.EMPTY, Version.CURRENT);

        // Initial cluster state with both states - the second node still shouldn't get diff even though it's present in the previous cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().put(nodeA.discoveryNode).put(nodeB.discoveryNode).localNodeId(nodeA.discoveryNode.id()).build();
        ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();
        ClusterState clusterState = ClusterState.builder(previousClusterState).incrementVersion().build();
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);
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
        List<Tuple<DiscoveryNode, Throwable>> errors = publishStateDiff(nodeA.action, unserializableClusterState, previousClusterState).awaitErrors(1, TimeUnit.SECONDS);
        assertThat(errors.size(), equalTo(1));
        assertThat(errors.get(0).v2().getMessage(), containsString("Simulated failure of diff serialization"));
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

    public void publishStateDiffAndWait(PublishClusterStateAction action, ClusterState state, ClusterState previousState) throws InterruptedException {
        publishStateDiff(action, state, previousState).await(1, TimeUnit.SECONDS);
    }

    public AssertingAckListener publishStateDiff(PublishClusterStateAction action, ClusterState state, ClusterState previousState) throws InterruptedException {
        AssertingAckListener assertingAckListener = new AssertingAckListener(state.nodes().getSize() - 1);
        ClusterChangedEvent changedEvent = new ClusterChangedEvent("test update", state, previousState);
        int requiredNodes = randomIntBetween(-1, state.nodes().getSize() - 1);
        action.publish(changedEvent, requiredNodes, assertingAckListener);
        return assertingAckListener;
    }

    public static class AssertingAckListener implements Discovery.AckListener {
        private final List<Tuple<DiscoveryNode, Throwable>> errors = new CopyOnWriteArrayList<>();
        private final AtomicBoolean timeoutOccured = new AtomicBoolean();
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
            timeoutOccured.set(true);
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
            assertFalse(timeoutOccured.get());
            return errors;
        }

    }

    public static class DelegatingClusterState extends ClusterState {

        public DelegatingClusterState(ClusterState clusterState) {
            super(clusterState.version(), clusterState.stateUUID(), clusterState);
        }


    }


    void assertSameState(ClusterState actual, ClusterState expected) {
        assertThat(actual, notNullValue());
        assertThat("\n--> actual ClusterState: " + actual.prettyPrint() + "\n--> expected ClusterState:" + expected.prettyPrint(), actual.stateUUID(), equalTo(expected.stateUUID()));
    }

    void assertSameStateFromDiff(ClusterState actual, ClusterState expected) {
        assertSameState(actual, expected);
        assertTrue(actual.wasReadFromDiff());
    }

    void assertSameStateFromFull(ClusterState actual, ClusterState expected) {
        assertSameState(actual, expected);
        assertFalse(actual.wasReadFromDiff());
    }
}
