/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.NodeResponseTracker;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeActionTests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.mockito.Mockito.mock;

public class TransportNodesActionTests extends ESTestCase {

    private static ThreadPool THREAD_POOL;

    private ClusterService clusterService;
    private CapturingTransport transport;
    private TransportService transportService;

    public void testRequestIsSentToEachNode() throws Exception {
        TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse> action = getTestTransportNodesAction();
        TestNodesRequest request = new TestNodesRequest();
        PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        int numNodes = clusterService.state().getNodes().getSize();
        // check a request was sent to the right number of nodes
        assertEquals(numNodes, capturedRequests.size());
    }

    public void testNodesSelectors() {
        TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse> action = getTestTransportNodesAction();
        int numSelectors = randomIntBetween(1, 5);
        Set<String> nodeSelectors = new HashSet<>();
        for (int i = 0; i < numSelectors; i++) {
            nodeSelectors.add(randomFrom(NodeSelector.values()).selector);
        }
        int numNodeIds = randomIntBetween(0, 3);
        String[] nodeIds = clusterService.state().nodes().getNodes().keySet().toArray(new String[0]);
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

    public void testNewResponseNullArray() throws Exception {
        TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse> action = getTestTransportNodesAction();
        final PlainActionFuture<TestNodesResponse> future = new PlainActionFuture<>();
        action.newResponse(new Task(1, "test", "test", "", null, emptyMap()), new TestNodesRequest(), null, future);
        expectThrows(NullPointerException.class, future::actionGet);
    }

    public void testNewResponse() throws Exception {
        TestTransportNodesAction action = getTestTransportNodesAction();
        TestNodesRequest request = new TestNodesRequest();
        List<TestNodeResponse> expectedNodeResponses = mockList(TestNodeResponse::new, randomIntBetween(0, 2));
        expectedNodeResponses.add(new TestNodeResponse());
        List<FailedNodeException> failures = mockList(
            () -> new FailedNodeException(
                randomAlphaOfLength(8),
                randomAlphaOfLength(8),
                new IllegalStateException(randomAlphaOfLength(8))
            ),
            randomIntBetween(0, 2)
        );

        List<Object> allResponses = new ArrayList<>(expectedNodeResponses);
        allResponses.addAll(failures);

        Collections.shuffle(allResponses, random());

        NodeResponseTracker nodeResponseCollector = new NodeResponseTracker(allResponses);

        final PlainActionFuture<TestNodesResponse> future = new PlainActionFuture<>();
        action.newResponse(new Task(1, "test", "test", "", null, emptyMap()), request, nodeResponseCollector, future);
        TestNodesResponse response = future.actionGet();

        assertSame(request, response.request);
        // note: I shuffled the overall list, so it's not possible to guarantee that it's in the right order
        assertTrue(expectedNodeResponses.containsAll(response.getNodes()));
        assertTrue(failures.containsAll(response.failures()));
    }

    public void testCustomResolving() throws Exception {
        TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse> action =
            getDataNodesOnlyTransportNodesAction(transportService);
        TestNodesRequest request = new TestNodesRequest(randomBoolean() ? null : generateRandomStringArray(10, 5, false, true));
        PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        // check requests were only sent to data nodes
        for (String nodeTarget : capturedRequests.keySet()) {
            assertTrue(clusterService.state().nodes().get(nodeTarget).canContainData());
        }
        assertEquals(clusterService.state().nodes().getDataNodes().size(), capturedRequests.size());
    }

    public void testTaskCancellation() {
        TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse> action = getTestTransportNodesAction();
        List<String> nodeIds = new ArrayList<>();
        for (DiscoveryNode node : clusterService.state().nodes()) {
            nodeIds.add(node.getId());
        }

        TestNodesRequest request = new TestNodesRequest(nodeIds.toArray(new String[0]));
        PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        CancellableTask cancellableTask = new CancellableTask(randomLong(), "transport", "action", "", null, emptyMap());
        TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse>.AsyncAction asyncAction =
            action.new AsyncAction(cancellableTask, request, listener);
        asyncAction.start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        int cancelAt = randomIntBetween(0, Math.max(0, capturedRequests.values().size() - 2));
        int requestCount = 0;
        for (List<CapturingTransport.CapturedRequest> requests : capturedRequests.values()) {
            if (requestCount == cancelAt) {
                TaskCancelHelper.cancel(cancellableTask, "simulated");
            }
            for (CapturingTransport.CapturedRequest capturedRequest : requests) {
                if (randomBoolean()) {
                    transport.handleResponse(capturedRequest.requestId(), new TestNodeResponse(capturedRequest.node()));
                } else {
                    transport.handleRemoteError(capturedRequest.requestId(), new TaskCancelledException("simulated"));
                }
            }
            requestCount++;
        }

        assertTrue(listener.isDone());
        assertTrue(asyncAction.getNodeResponseTracker().responsesDiscarded());
        expectThrows(ExecutionException.class, TaskCancelledException.class, listener::get);
    }

    private <T> List<T> mockList(Supplier<T> supplier, int size) {
        List<T> failures = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            failures.add(supplier.get());
        }
        return failures;
    }

    private enum NodeSelector {
        LOCAL("_local"),
        ELECTED_MASTER("_master"),
        MASTER_ELIGIBLE("master:true"),
        DATA("data:true"),
        CUSTOM_ATTRIBUTE("attr:value");

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
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            THREAD_POOL,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        int numNodes = randomIntBetween(3, 10);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.roles()));
            if (frequently()) {
                attributes.put("custom", randomBoolean() ? "match" : randomAlphaOfLengthBetween(3, 5));
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
            THREAD_POOL,
            clusterService,
            transportService,
            new ActionFilters(Collections.emptySet()),
            TestNodesRequest::new,
            TestNodeRequest::new,
            ThreadPool.Names.SAME
        );
    }

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        String node = "node_" + nodeId;
        return new DiscoveryNode(node, node, buildNewFakeTransportAddress(), attributes, roles, Version.CURRENT);
    }

    private static class TestTransportNodesAction extends TransportNodesAction<
        TestNodesRequest,
        TestNodesResponse,
        TestNodeRequest,
        TestNodeResponse> {

        TestTransportNodesAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            Writeable.Reader<TestNodesRequest> request,
            Writeable.Reader<TestNodeRequest> nodeRequest,
            String nodeExecutor
        ) {
            super(
                "indices:admin/test",
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                request,
                nodeRequest,
                nodeExecutor,
                TestNodeResponse.class
            );
        }

        @Override
        protected TestNodesResponse newResponse(
            TestNodesRequest request,
            List<TestNodeResponse> responses,
            List<FailedNodeException> failures
        ) {
            return new TestNodesResponse(clusterService.getClusterName(), request, responses, failures);
        }

        @Override
        protected TestNodeRequest newNodeRequest(TestNodesRequest request) {
            return new TestNodeRequest();
        }

        @Override
        protected TestNodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
            return new TestNodeResponse(in);
        }

        @Override
        protected TestNodeResponse nodeOperation(TestNodeRequest request, Task task) {
            return new TestNodeResponse();
        }

    }

    private static class DataNodesOnlyTransportNodesAction extends TestTransportNodesAction {

        DataNodesOnlyTransportNodesAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            Writeable.Reader<TestNodesRequest> request,
            Writeable.Reader<TestNodeRequest> nodeRequest,
            String nodeExecutor
        ) {
            super(threadPool, clusterService, transportService, actionFilters, request, nodeRequest, nodeExecutor);
        }

        @Override
        protected void resolveRequest(TestNodesRequest request, ClusterState clusterState) {
            request.setConcreteNodes(clusterState.nodes().getDataNodes().values().toArray(DiscoveryNode[]::new));
        }
    }

    private static class TestNodesRequest extends BaseNodesRequest<TestNodesRequest> {
        TestNodesRequest(StreamInput in) throws IOException {
            super(in);
        }

        TestNodesRequest(String... nodesIds) {
            super(nodesIds);
        }
    }

    private static class TestNodesResponse extends BaseNodesResponse<TestNodeResponse> {

        private final TestNodesRequest request;

        TestNodesResponse(
            ClusterName clusterName,
            TestNodesRequest request,
            List<TestNodeResponse> nodeResponses,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodeResponses, failures);
            this.request = request;
        }

        @Override
        protected List<TestNodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(TestNodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<TestNodeResponse> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    private static class TestNodeRequest extends TransportRequest {
        TestNodeRequest() {}

        TestNodeRequest(StreamInput in) throws IOException {
            super(in);
        }
    }

    private static class TestNodeResponse extends BaseNodeResponse {
        TestNodeResponse() {
            this(mock(DiscoveryNode.class));
        }

        TestNodeResponse(DiscoveryNode node) {
            super(node);
        }

        protected TestNodeResponse(StreamInput in) throws IOException {
            super(in);
        }
    }

    private static class OtherNodeResponse extends BaseNodeResponse {
        OtherNodeResponse() {
            super(mock(DiscoveryNode.class));
        }

        protected OtherNodeResponse(StreamInput in) throws IOException {
            super(in);
        }
    }

}
