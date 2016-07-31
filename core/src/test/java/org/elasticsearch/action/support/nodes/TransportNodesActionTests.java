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

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeActionTests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.mockito.Mockito.mock;

public class TransportNodesActionTests extends ESTestCase {

    private static ThreadPool THREAD_POOL;

    private ClusterService clusterService;
    private CapturingTransport transport;
    private TransportService transportService;

    public void testRequestIsSentToEachNode() throws Exception {
        TransportNodesAction action = getTestTransportNodesAction();
        TestNodesRequest request = new TestNodesRequest();
        PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        int numNodes = clusterService.state().getNodes().getSize();
        // check a request was sent to the right number of nodes
        assertEquals(numNodes, capturedRequests.size());
    }

    public void testNodesSelectors() {
        TransportNodesAction action = getTestTransportNodesAction();
        int numSelectors = randomIntBetween(1, 5);
        Set<String> nodeSelectors = new HashSet<>();
        for (int i = 0; i < numSelectors; i++) {
            nodeSelectors.add(randomFrom(NodeSelector.values()).selector);
        }
        int numNodeIds = randomIntBetween(0, 3);
        String[] nodeIds = clusterService.state().nodes().getNodes().keys().toArray(String.class);
        for (int i = 0; i < numNodeIds; i++) {
            String nodeId = randomFrom(nodeIds);
            nodeSelectors.add(nodeId);
        }
        String[] finalNodesIds = nodeSelectors.toArray(new String[nodeSelectors.size()]);
        TestNodesRequest request = new TestNodesRequest(finalNodesIds);
        action.new AsyncAction(null, request, new PlainActionFuture<>()).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        assertEquals(clusterService.state().nodes().resolveNodes(finalNodesIds).length, capturedRequests.size());
    }

    public void testNewResponseNullArray() {
        TransportNodesAction action = getTestTransportNodesAction();
        expectThrows(NullPointerException.class, () -> action.newResponse(new TestNodesRequest(), null));
    }

    public void testNewResponse() {
        TestTransportNodesAction action = getTestTransportNodesAction();
        TestNodesRequest request = new TestNodesRequest();
        List<TestNodeResponse> expectedNodeResponses = mockList(TestNodeResponse.class, randomIntBetween(0, 2));
        expectedNodeResponses.add(new TestNodeResponse());
        List<BaseNodeResponse> nodeResponses = new ArrayList<>(expectedNodeResponses);
        // This should be ignored:
        nodeResponses.add(new OtherNodeResponse());
        List<FailedNodeException> failures = mockList(FailedNodeException.class, randomIntBetween(0, 2));

        List<Object> allResponses = new ArrayList<>(expectedNodeResponses);
        allResponses.addAll(failures);

        Collections.shuffle(allResponses, random());

        AtomicReferenceArray<?> atomicArray = new AtomicReferenceArray<>(allResponses.toArray());

        TestNodesResponse response = action.newResponse(request, atomicArray);

        assertSame(request, response.request);
        // note: I shuffled the overall list, so it's not possible to guarantee that it's in the right order
        assertTrue(expectedNodeResponses.containsAll(response.getNodes()));
        assertTrue(failures.containsAll(response.failures()));
    }

    public void testCustomResolving() throws Exception {
        TransportNodesAction action = getDataNodesOnlyTransportNodesAction(transportService);
        TestNodesRequest request = new TestNodesRequest(randomBoolean() ? null : generateRandomStringArray(10, 5, false, true));
        PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        // check requests were only sent to data nodes
        for (String nodeTarget : capturedRequests.keySet()) {
            assertTrue(clusterService.state().nodes().get(nodeTarget).isDataNode());
        }
        assertEquals(clusterService.state().nodes().getDataNodes().size(), capturedRequests.size());
    }

    private <T> List<T> mockList(Class<T> clazz, int size) {
        List<T> failures = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            failures.add(mock(clazz));
        }
        return failures;
    }

    private enum NodeSelector {
        LOCAL("_local"), ELECTED_MASTER("_master"), MASTER_ELIGIBLE("master:true"), DATA("data:true"), CUSTOM_ATTRIBUTE("attr:value");

        private final String selector;

        NodeSelector(String selector) {
            this.selector = selector;
        }
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool(TransportBroadcastByNodeActionTests.class.getSimpleName());
    }

    @AfterClass
    public static void destroyThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        THREAD_POOL = null;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        transportService = new TransportService(clusterService.getSettings(), transport, THREAD_POOL);
        transportService.start();
        transportService.acceptIncomingRequests();
        int numNodes = randomIntBetween(3, 10);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            Set<DiscoveryNode.Role> roles = new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values())));
            if (frequently()) {
                attributes.put("custom", randomBoolean() ? "match" : randomAsciiOfLengthBetween(3, 5));
            }
            final DiscoveryNode node = newNode(i, attributes, roles);
            discoBuilder = discoBuilder.add(node);
            discoveryNodes.add(node);
        }
        discoBuilder.localNodeId(randomFrom(discoveryNodes).getId());
        discoBuilder.masterNodeId(randomFrom(discoveryNodes).getId());
        ClusterState.Builder stateBuilder = ClusterState.builder(clusterService.getClusterName());
        stateBuilder.nodes(discoBuilder);
        ClusterState clusterState = stateBuilder.build();
        setState(clusterService, clusterState);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transport.close();
    }

    public TestTransportNodesAction getTestTransportNodesAction() {
        return new TestTransportNodesAction(
                Settings.EMPTY,
                THREAD_POOL,
                clusterService,
                transportService,
                new ActionFilters(Collections.emptySet()),
                TestNodesRequest::new,
                TestNodeRequest::new,
                ThreadPool.Names.SAME
        );
    }

    public DataNodesOnlyTransportNodesAction getDataNodesOnlyTransportNodesAction(TransportService transportService) {
        return new DataNodesOnlyTransportNodesAction(
            Settings.EMPTY,
            THREAD_POOL,
            clusterService,
            transportService,
            new ActionFilters(Collections.emptySet()),
            TestNodesRequest::new,
            TestNodeRequest::new,
            ThreadPool.Names.SAME
        );
    }

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes, Set<DiscoveryNode.Role> roles) {
        String node = "node_" + nodeId;
        return new DiscoveryNode(node, node, LocalTransportAddress.buildUnique(), attributes, roles, Version.CURRENT);
    }

    private static class TestTransportNodesAction
        extends TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse> {

        TestTransportNodesAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService
                transportService, ActionFilters actionFilters, Supplier<TestNodesRequest> request,
                                 Supplier<TestNodeRequest> nodeRequest, String nodeExecutor) {
            super(settings, "indices:admin/test", threadPool, clusterService, transportService, actionFilters,
                    null, request, nodeRequest, nodeExecutor, TestNodeResponse.class);
        }

        @Override
        protected TestNodesResponse newResponse(TestNodesRequest request,
                                                List<TestNodeResponse> responses, List<FailedNodeException> failures) {
            return new TestNodesResponse(clusterService.getClusterName(), request, responses, failures);
        }

        @Override
        protected TestNodeRequest newNodeRequest(String nodeId, TestNodesRequest request) {
            return new TestNodeRequest();
        }

        @Override
        protected TestNodeResponse newNodeResponse() {
            return new TestNodeResponse();
        }

        @Override
        protected TestNodeResponse nodeOperation(TestNodeRequest request) {
            return new TestNodeResponse();
        }

        @Override
        protected boolean accumulateExceptions() {
            return false;
        }
    }

    private static class DataNodesOnlyTransportNodesAction
        extends TestTransportNodesAction {

        DataNodesOnlyTransportNodesAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService
            transportService, ActionFilters actionFilters, Supplier<TestNodesRequest> request,
                                          Supplier<TestNodeRequest> nodeRequest, String nodeExecutor) {
            super(settings, threadPool, clusterService, transportService, actionFilters, request, nodeRequest, nodeExecutor);
        }

        @Override
        protected void resolveRequest(TestNodesRequest request, ClusterState clusterState) {
            request.setConcreteNodes(clusterState.nodes().getDataNodes().values().toArray(DiscoveryNode.class));
        }
    }

    private static class TestNodesRequest extends BaseNodesRequest<TestNodesRequest> {
        TestNodesRequest(String... nodesIds) {
            super(nodesIds);
        }
    }

    private static class TestNodesResponse extends BaseNodesResponse<TestNodeResponse> {

        private final TestNodesRequest request;

        TestNodesResponse(ClusterName clusterName, TestNodesRequest request, List<TestNodeResponse> nodeResponses,
                          List<FailedNodeException> failures) {
            super(clusterName, nodeResponses, failures);
            this.request = request;
        }

        @Override
        protected List<TestNodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readStreamableList(TestNodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<TestNodeResponse> nodes) throws IOException {
            out.writeStreamableList(nodes);
        }
    }

    private static class TestNodeRequest extends BaseNodeRequest { }

    private static class TestNodeResponse extends BaseNodeResponse { }

    private static class OtherNodeResponse extends BaseNodeResponse { }

}
