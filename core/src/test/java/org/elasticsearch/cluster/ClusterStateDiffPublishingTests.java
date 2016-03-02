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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStateDiffPublishingTests extends ESTestCase {

    protected ThreadPool threadPool;
    protected Map<String, MockNode> nodes = newHashMap();

    public static class MockNode {
        public final DiscoveryNode discoveryNode;
        public final MockTransportService service;
        public final PublishClusterStateAction action;
        public final MockDiscoveryNodesProvider nodesProvider;

        public MockNode(DiscoveryNode discoveryNode, MockTransportService service, PublishClusterStateAction action, MockDiscoveryNodesProvider nodesProvider) {
            this.discoveryNode = discoveryNode;
            this.service = service;
            this.action = action;
            this.nodesProvider = nodesProvider;
        }

        public void connectTo(DiscoveryNode node) {
            service.connectToNode(node);
            nodesProvider.addNode(node);
        }
    }

    public MockNode createMockNode(final String name, Settings settings, Version version) throws Exception {
        return createMockNode(name, settings, version, new PublishClusterStateAction.NewClusterStateListener() {
            @Override
            public void onNewClusterState(ClusterState clusterState, NewStateProcessed newStateProcessed) {
                logger.debug("Node [{}] onNewClusterState version [{}], uuid [{}]", name, clusterState.version(), clusterState.stateUUID());
                newStateProcessed.onNewClusterStateProcessed();
            }
        });
    }

    public MockNode createMockNode(String name, Settings settings, Version version, PublishClusterStateAction.NewClusterStateListener listener) throws Exception {
        MockTransportService service = buildTransportService(
                Settings.builder().put(settings).put("name", name, TransportService.SETTING_TRACE_LOG_INCLUDE, "", TransportService.SETTING_TRACE_LOG_EXCLUDE, "NOTHING").build(),
                version
        );
        DiscoveryNode discoveryNode = new DiscoveryNode(name, name, service.boundAddress().publishAddress(), ImmutableMap.<String, String>of(), version);
        MockDiscoveryNodesProvider nodesProvider = new MockDiscoveryNodesProvider(discoveryNode);
        PublishClusterStateAction action = buildPublishClusterStateAction(settings, service, nodesProvider, listener);
        MockNode node = new MockNode(discoveryNode, service, action, nodesProvider);
        nodesProvider.addNode(discoveryNode);
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
        MockTransportService transportService = MockTransportService.local(settings, version, threadPool);
        transportService.start();
        transportService.acceptIncomingRequests();
        return transportService;
    }

    protected PublishClusterStateAction buildPublishClusterStateAction(Settings settings, MockTransportService transportService, MockDiscoveryNodesProvider nodesProvider,
                                                                       PublishClusterStateAction.NewClusterStateListener listener) {
        DiscoverySettings discoverySettings = new DiscoverySettings(settings, new NodeSettingsService(settings));
        return new PublishClusterStateAction(settings, transportService, nodesProvider, listener, discoverySettings);
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
        MockNewClusterStateListener mockListenerA = new MockNewClusterStateListener();
        MockNode nodeA = createMockNode("nodeA", Settings.EMPTY, Version.CURRENT, mockListenerA);

        MockNewClusterStateListener mockListenerB = new MockNewClusterStateListener();
        MockNode nodeB = createMockNode("nodeB", Settings.EMPTY, Version.CURRENT, mockListenerB);

        // Initial cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().put(nodeA.discoveryNode).localNodeId(nodeA.discoveryNode.id()).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();

        // cluster state update - add nodeB
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).put(nodeB.discoveryNode).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        mockListenerB.add(new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertFalse(clusterState.wasReadFromDiff());
            }
        });
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder().addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
        mockListenerB.add(new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertTrue(clusterState.wasReadFromDiff());
                assertThat(clusterState.blocks().global().size(), equalTo(1));
            }
        });
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);

        // cluster state update - remove block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).incrementVersion().build();
        mockListenerB.add(new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertTrue(clusterState.wasReadFromDiff());
                assertThat(clusterState.blocks().global().size(), equalTo(0));
            }
        });
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);

        // Adding new node - this node should get full cluster state while nodeB should still be getting diffs

        MockNewClusterStateListener mockListenerC = new MockNewClusterStateListener();
        MockNode nodeC = createMockNode("nodeC", Settings.EMPTY, Version.CURRENT, mockListenerC);

        // cluster state update 3 - register node C
        previousClusterState = clusterState;
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).put(nodeC.discoveryNode).build();
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        mockListenerB.add(new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertTrue(clusterState.wasReadFromDiff());
                assertThat(clusterState.blocks().global().size(), equalTo(0));
            }
        });
        mockListenerC.add(new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                // First state
                assertFalse(clusterState.wasReadFromDiff());
            }
        });
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);

        // cluster state update 4 - update settings
        previousClusterState = clusterState;
        MetaData metaData = MetaData.builder(clusterState.metaData()).transientSettings(Settings.settingsBuilder().put("foo", "bar").build()).build();
        clusterState = ClusterState.builder(clusterState).metaData(metaData).incrementVersion().build();
        NewClusterStateExpectation expectation = new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertTrue(clusterState.wasReadFromDiff());
                assertThat(clusterState.blocks().global().size(), equalTo(0));
            }
        };
        mockListenerB.add(expectation);
        mockListenerC.add(expectation);
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);

        // cluster state update - skipping one version change - should request full cluster state
        previousClusterState = ClusterState.builder(clusterState).incrementVersion().build();
        clusterState = ClusterState.builder(clusterState).incrementVersion().build();
        expectation = new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertFalse(clusterState.wasReadFromDiff());
            }
        };
        mockListenerB.add(expectation);
        mockListenerC.add(expectation);
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);

        // cluster state update - skipping one version change - should request full cluster state
        previousClusterState = ClusterState.builder(clusterState).incrementVersion().build();
        clusterState = ClusterState.builder(clusterState).incrementVersion().build();
        expectation = new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertFalse(clusterState.wasReadFromDiff());
            }
        };
        mockListenerB.add(expectation);
        mockListenerC.add(expectation);
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);

        // node B becomes the master and sends a version of the cluster state that goes back
        discoveryNodes = DiscoveryNodes.builder(discoveryNodes)
                .put(nodeA.discoveryNode)
                .put(nodeB.discoveryNode)
                .put(nodeC.discoveryNode)
                .build();
        previousClusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        clusterState = ClusterState.builder(clusterState).nodes(discoveryNodes).incrementVersion().build();
        expectation = new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertFalse(clusterState.wasReadFromDiff());
            }
        };
        mockListenerA.add(expectation);
        mockListenerC.add(expectation);
        publishStateDiffAndWait(nodeB.action, clusterState, previousClusterState);
    }

    @Test
    @TestLogging("cluster:DEBUG,discovery.zen.publish:DEBUG")
    public void testUnexpectedDiffPublishing() throws Exception {

        MockNode nodeA = createMockNode("nodeA", Settings.EMPTY, Version.CURRENT, new PublishClusterStateAction.NewClusterStateListener() {
            @Override
            public void onNewClusterState(ClusterState clusterState, NewStateProcessed newStateProcessed) {
                fail("Shouldn't send cluster state to myself");
            }
        });

        MockNewClusterStateListener mockListenerB = new MockNewClusterStateListener();
        MockNode nodeB = createMockNode("nodeB", Settings.EMPTY, Version.CURRENT, mockListenerB);

        // Initial cluster state with both states - the second node still shouldn't get diff even though it's present in the previous cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().put(nodeA.discoveryNode).put(nodeB.discoveryNode).localNodeId(nodeA.discoveryNode.id()).build();
        ClusterState previousClusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        ClusterState clusterState = ClusterState.builder(previousClusterState).incrementVersion().build();
        mockListenerB.add(new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertFalse(clusterState.wasReadFromDiff());
            }
        });
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder().addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
        mockListenerB.add(new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertTrue(clusterState.wasReadFromDiff());
            }
        });
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);
    }

    @Test
    @TestLogging("cluster:DEBUG,discovery.zen.publish:DEBUG")
    public void testDisablingDiffPublishing() throws Exception {
        Settings noDiffPublishingSettings = Settings.builder().put(DiscoverySettings.PUBLISH_DIFF_ENABLE, false).build();

        MockNode nodeA = createMockNode("nodeA", noDiffPublishingSettings, Version.CURRENT, new PublishClusterStateAction.NewClusterStateListener() {
            @Override
            public void onNewClusterState(ClusterState clusterState, NewStateProcessed newStateProcessed) {
                fail("Shouldn't send cluster state to myself");
            }
        });

        MockNode nodeB = createMockNode("nodeB", noDiffPublishingSettings, Version.CURRENT, new PublishClusterStateAction.NewClusterStateListener() {
            @Override
            public void onNewClusterState(ClusterState clusterState, NewStateProcessed newStateProcessed) {
                logger.debug("Got cluster state update, version [{}], guid [{}], from diff [{}]", clusterState.version(), clusterState.stateUUID(), clusterState.wasReadFromDiff());
                assertFalse(clusterState.wasReadFromDiff());
                newStateProcessed.onNewClusterStateProcessed();
            }
        });

        // Initial cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().put(nodeA.discoveryNode).localNodeId(nodeA.discoveryNode.id()).build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();

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
    public void testSimultaneousClusterStatePublishing() throws Exception {
        int numberOfNodes = randomIntBetween(2, 10);
        int numberOfIterations = randomIntBetween(50, 200);
        Settings settings = Settings.builder().put(DiscoverySettings.PUBLISH_TIMEOUT, "100ms").put(DiscoverySettings.PUBLISH_DIFF_ENABLE, true).build();
        MockNode[] nodes = new MockNode[numberOfNodes];
        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < nodes.length; i++) {
            final String name = "node" + i;
            nodes[i] = createMockNode(name, settings, Version.CURRENT, new PublishClusterStateAction.NewClusterStateListener() {
                @Override
                public synchronized void onNewClusterState(ClusterState clusterState, NewStateProcessed newStateProcessed) {
                    assertProperMetaDataForVersion(clusterState.metaData(), clusterState.version());
                    if (randomInt(10) < 2) {
                        // Cause timeouts from time to time
                        try {
                            Thread.sleep(randomInt(110));
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    newStateProcessed.onNewClusterStateProcessed();
                }
            });
            discoveryNodesBuilder.put(nodes[i].discoveryNode);
        }

        AssertingAckListener[] listeners = new AssertingAckListener[numberOfIterations];
        DiscoveryNodes discoveryNodes = discoveryNodesBuilder.build();
        MetaData metaData = MetaData.EMPTY_META_DATA;
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metaData(metaData).build();
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
    }

    @Test
    @TestLogging("cluster:DEBUG,discovery.zen.publish:DEBUG")
    public void testSerializationFailureDuringDiffPublishing() throws Exception {

        MockNode nodeA = createMockNode("nodeA", Settings.EMPTY, Version.CURRENT, new PublishClusterStateAction.NewClusterStateListener() {
            @Override
            public void onNewClusterState(ClusterState clusterState, NewStateProcessed newStateProcessed) {
                fail("Shouldn't send cluster state to myself");
            }
        });

        MockNewClusterStateListener mockListenerB = new MockNewClusterStateListener();
        MockNode nodeB = createMockNode("nodeB", Settings.EMPTY, Version.CURRENT, mockListenerB);

        // Initial cluster state with both states - the second node still shouldn't get diff even though it's present in the previous cluster state
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().put(nodeA.discoveryNode).put(nodeB.discoveryNode).localNodeId(nodeA.discoveryNode.id()).build();
        ClusterState previousClusterState = ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).build();
        ClusterState clusterState = ClusterState.builder(previousClusterState).incrementVersion().build();
        mockListenerB.add(new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertFalse(clusterState.wasReadFromDiff());
            }
        });
        publishStateDiffAndWait(nodeA.action, clusterState, previousClusterState);

        // cluster state update - add block
        previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).blocks(ClusterBlocks.builder().addGlobalBlock(MetaData.CLUSTER_READ_ONLY_BLOCK)).incrementVersion().build();
        mockListenerB.add(new NewClusterStateExpectation() {
            @Override
            public void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed) {
                assertTrue(clusterState.wasReadFromDiff());
            }
        });

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
            assertThat(metaData.index("test" + i).getNumberOfShards(), equalTo((int) i));
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
        action.publish(changedEvent, assertingAckListener);
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

    public interface NewClusterStateExpectation {
        void check(ClusterState clusterState, PublishClusterStateAction.NewClusterStateListener.NewStateProcessed newStateProcessed);
    }

    public static class MockNewClusterStateListener implements PublishClusterStateAction.NewClusterStateListener {
        CopyOnWriteArrayList<NewClusterStateExpectation> expectations = new CopyOnWriteArrayList();

        @Override
        public void onNewClusterState(ClusterState clusterState, NewStateProcessed newStateProcessed) {
            final NewClusterStateExpectation expectation;
            try {
                expectation = expectations.remove(0);
            } catch (ArrayIndexOutOfBoundsException ex) {
                fail("Unexpected cluster state update " + clusterState.prettyPrint());
                return;
            }
            expectation.check(clusterState, newStateProcessed);
            newStateProcessed.onNewClusterStateProcessed();
        }

        public void add(NewClusterStateExpectation expectation) {
            expectations.add(expectation);
        }
    }

    public static class DelegatingClusterState extends ClusterState {

        public DelegatingClusterState(ClusterState clusterState) {
            super(clusterState.version(), clusterState.stateUUID(), clusterState);
        }


    }

}
