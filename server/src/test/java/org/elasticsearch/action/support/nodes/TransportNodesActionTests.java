/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ReachabilityChecker;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

public class TransportNodesActionTests extends ESTestCase {

    private static ThreadPool THREAD_POOL;

    private ClusterService clusterService;
    private CapturingTransport transport;
    private TransportService transportService;

    public void testRequestIsSentToEachNode() {
        TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse, Void> action =
            getTestTransportNodesAction();
        TestNodesRequest request = new TestNodesRequest();
        action.execute(null, request, new PlainActionFuture<>());
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        int numNodes = clusterService.state().getNodes().getSize();
        // check a request was sent to the right number of nodes
        assertEquals(numNodes, capturedRequests.size());
        assertTrue(capturedRequests.values().stream().flatMap(Collection::stream).noneMatch(cr -> cr.request().hasReferences()));
    }

    public void testNodesSelectors() {
        TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse, Void> action =
            getTestTransportNodesAction();
        int numSelectors = randomIntBetween(1, 5);
        Set<String> nodeSelectors = new HashSet<>();
        for (int i = 0; i < numSelectors; i++) {
            nodeSelectors.add(randomFrom("_local", "_master", "master:true", "data:true", "attr:value"));
        }
        int numNodeIds = randomIntBetween(0, 3);
        String[] nodeIds = clusterService.state().nodes().getNodes().keySet().toArray(new String[0]);
        for (int i = 0; i < numNodeIds; i++) {
            String nodeId = randomFrom(nodeIds);
            nodeSelectors.add(nodeId);
        }
        String[] finalNodesIds = nodeSelectors.toArray(String[]::new);
        TestNodesRequest request = new TestNodesRequest(finalNodesIds);
        action.execute(null, request, new PlainActionFuture<>());
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        assertEquals(clusterService.state().nodes().resolveNodes(finalNodesIds).length, capturedRequests.size());
    }

    public void testCustomResolving() {
        TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse, Void> action =
            getDataNodesOnlyTransportNodesAction(transportService);
        TestNodesRequest request = new TestNodesRequest(randomBoolean() ? null : generateRandomStringArray(10, 5, false, true));
        action.execute(null, request, new PlainActionFuture<>());
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        // check requests were only sent to data nodes
        for (String nodeTarget : capturedRequests.keySet()) {
            assertTrue(clusterService.state().nodes().get(nodeTarget).canContainData());
        }
        assertEquals(clusterService.state().nodes().getDataNodes().size(), capturedRequests.size());
    }

    public void testResponseAggregation() {
        final TestTransportNodesAction action = getTestTransportNodesAction();

        final PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        action.execute(null, new TestNodesRequest(), listener.delegateFailure((l, response) -> {
            assertTrue(response.getNodes().stream().allMatch(TestNodeResponse::hasReferences));
            assertTrue(response.hasReferences());
            l.onResponse(response);
        }));
        assertFalse(listener.isDone());

        final Set<String> failedNodeIds = new HashSet<>();
        final Set<DiscoveryNode> successfulNodes = new HashSet<>();

        for (CapturingTransport.CapturedRequest capturedRequest : transport.getCapturedRequestsAndClear()) {
            if (randomBoolean()) {
                successfulNodes.add(capturedRequest.node());
                final var response = new TestNodeResponse(capturedRequest.node());
                transport.handleResponse(capturedRequest.requestId(), response);
                response.decRef();
                assertFalse(response.hasReferences()); // response is copied (via the wire protocol) so this instance is released
            } else {
                failedNodeIds.add(capturedRequest.node().getId());
                if (randomBoolean()) {
                    transport.handleRemoteError(capturedRequest.requestId(), new ElasticsearchException("simulated"));
                } else {
                    transport.handleLocalError(capturedRequest.requestId(), new ElasticsearchException("simulated"));
                }
            }
        }

        final TestNodesResponse response = listener.actionGet(10, TimeUnit.SECONDS);

        final var allResponsesReleasedListener = new SubscribableListener<Void>();
        try (var listeners = new RefCountingListener(allResponsesReleasedListener)) {
            response.addCloseListener(listeners.acquire());
            for (final var nodeResponse : response.getNodes()) {
                nodeResponse.addCloseListener(listeners.acquire());
            }
        }
        safeAwait(allResponsesReleasedListener);
        assertTrue(response.getNodes().stream().noneMatch(TestNodeResponse::hasReferences));
        assertFalse(response.hasReferences());

        for (TestNodeResponse nodeResponse : response.getNodes()) {
            assertThat(successfulNodes, Matchers.hasItem(nodeResponse.getNode()));
        }
        assertEquals(successfulNodes.size(), response.getNodes().size());

        assertNotEquals(failedNodeIds.isEmpty(), response.hasFailures());
        for (FailedNodeException failure : response.failures()) {
            assertThat(failedNodeIds, Matchers.hasItem(failure.nodeId()));
            if (failure.getCause() instanceof ElasticsearchException elasticsearchException) {
                final var cause = elasticsearchException.unwrapCause();
                assertEquals("simulated", cause.getMessage());
            } else {
                throw new AssertionError("unexpected exception", failure);
            }
        }
        assertEquals(failedNodeIds.size(), response.failures().size());
    }

    public void testResponsesReleasedOnCancellation() {
        final TestTransportNodesAction action = getTestTransportNodesAction();

        final CancellableTask cancellableTask = new CancellableTask(randomLong(), "transport", "action", "", null, emptyMap());
        final PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        action.execute(cancellableTask, new TestNodesRequest(), listener.delegateResponse((l, e) -> {
            assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
            l.onFailure(e);
        }));

        final List<CapturingTransport.CapturedRequest> capturedRequests = new ArrayList<>(
            Arrays.asList(transport.getCapturedRequestsAndClear())
        );
        Randomness.shuffle(capturedRequests);

        final AtomicInteger liveResponseCount = new AtomicInteger();
        final Function<DiscoveryNode, TestNodeResponse> responseCreator = node -> {
            liveResponseCount.incrementAndGet();
            final var testNodeResponse = new TestNodeResponse(node);
            testNodeResponse.addCloseListener(ActionListener.running(liveResponseCount::decrementAndGet));
            return testNodeResponse;
        };

        final ObjLongConsumer<TestNodeResponse> responseSender = (response, requestId) -> {
            try {
                // transport.handleResponse may de/serialize the response, releasing it early, so send the response straight to the handler
                transport.getTransportResponseHandler(requestId).handleResponse(response);
            } finally {
                response.decRef();
            }
        };

        final ReachabilityChecker reachabilityChecker = new ReachabilityChecker();
        final Runnable nextRequestProcessor = () -> {
            var capturedRequest = capturedRequests.remove(0);
            if (randomBoolean()) {
                responseSender.accept(
                    reachabilityChecker.register(responseCreator.apply(capturedRequest.node())),
                    capturedRequest.requestId()
                );
            } else {
                // handleRemoteError may de/serialize the exception, releasing it early, so just use handleLocalError
                transport.handleLocalError(
                    capturedRequest.requestId(),
                    reachabilityChecker.register(new ElasticsearchException("simulated"))
                );
            }
        };

        assertThat(capturedRequests.size(), greaterThan(2));
        final var responsesBeforeCancellation = between(1, capturedRequests.size() - 2);
        for (int i = 0; i < responsesBeforeCancellation; i++) {
            nextRequestProcessor.run();
        }

        reachabilityChecker.checkReachable();
        TaskCancelHelper.cancel(cancellableTask, "simulated");

        // responses captured before cancellation are now unreachable
        reachabilityChecker.ensureUnreachable();
        assertEquals(0, liveResponseCount.get());

        while (capturedRequests.size() > 0) {
            // a response sent after cancellation is dropped immediately
            assertFalse(listener.isDone());
            nextRequestProcessor.run();
            reachabilityChecker.ensureUnreachable();
            assertEquals(0, liveResponseCount.get());
        }

        expectThrows(TaskCancelledException.class, () -> listener.actionGet(10, TimeUnit.SECONDS));
        assertTrue(cancellableTask.isCancelled()); // keep task alive
    }

    public void testActionContextReleasedOnCancellation() {
        final var reachabilityChecker = new ReachabilityChecker();
        final TransportNodesAction<TestNodesRequest, TestNodesResponse, TestNodeRequest, TestNodeResponse, Object> action =
            new TransportNodesAction<>(
                "indices:admin/test",
                clusterService,
                transportService,
                new ActionFilters(Collections.emptySet()),
                TestNodeRequest::new,
                THREAD_POOL.executor(ThreadPool.Names.GENERIC)
            ) {
                @Override
                protected TestNodesResponse newResponse(
                    TestNodesRequest request,
                    List<TestNodeResponse> testNodeResponses,
                    List<FailedNodeException> failures
                ) {
                    return fail(null, "should not be called");
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

                @Override
                protected Object createActionContext(Task task, TestNodesRequest request) {
                    return reachabilityChecker.register(new Object());
                }
            };

        final CancellableTask cancellableTask = new CancellableTask(randomLong(), "transport", "action", "", null, emptyMap());
        final PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        action.execute(cancellableTask, new TestNodesRequest(), listener);

        reachabilityChecker.checkReachable();
        TaskCancelHelper.cancel(cancellableTask, "simulated");
        reachabilityChecker.ensureUnreachable();

        for (CapturingTransport.CapturedRequest capturedRequest : transport.getCapturedRequestsAndClear()) {
            transport.handleLocalError(capturedRequest.requestId(), new ElasticsearchException("simulated"));
        }

        expectThrows(TaskCancelledException.class, () -> listener.actionGet(10, TimeUnit.SECONDS));
        assertTrue(cancellableTask.isCancelled()); // keep task alive
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool(TransportNodesActionTests.class.getSimpleName());
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
            clusterService,
            transportService,
            new ActionFilters(Collections.emptySet()),
            TestNodeRequest::new,
            THREAD_POOL.executor(ThreadPool.Names.GENERIC)
        );
    }

    public DataNodesOnlyTransportNodesAction getDataNodesOnlyTransportNodesAction(TransportService transportService) {
        return new DataNodesOnlyTransportNodesAction(
            clusterService,
            transportService,
            new ActionFilters(Collections.emptySet()),
            TestNodeRequest::new,
            THREAD_POOL.executor(ThreadPool.Names.GENERIC)
        );
    }

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        String node = "node_" + nodeId;
        return DiscoveryNodeUtils.builder(node).name(node).attributes(attributes).roles(roles).build();
    }

    private static class TestTransportNodesAction extends TransportNodesAction<
        TestNodesRequest,
        TestNodesResponse,
        TestNodeRequest,
        TestNodeResponse,
        Void> {

        TestTransportNodesAction(
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            Writeable.Reader<TestNodeRequest> nodeRequest,
            Executor nodeExecutor
        ) {
            super("indices:admin/test", clusterService, transportService, actionFilters, nodeRequest, nodeExecutor);
        }

        @Override
        protected TestNodesResponse newResponse(
            TestNodesRequest request,
            List<TestNodeResponse> responses,
            List<FailedNodeException> failures
        ) {
            return new TestNodesResponse(clusterService.getClusterName(), responses, failures);
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
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            Writeable.Reader<TestNodeRequest> nodeRequest,
            Executor nodeExecutor
        ) {
            super(clusterService, transportService, actionFilters, nodeRequest, nodeExecutor);
        }

        @Override
        protected DiscoveryNode[] resolveRequest(TestNodesRequest request, ClusterState clusterState) {
            return clusterState.nodes().getDataNodes().values().toArray(DiscoveryNode[]::new);
        }
    }

    private static class TestNodesRequest extends BaseNodesRequest {
        TestNodesRequest(String... nodesIds) {
            super(nodesIds);
        }
    }

    private static class TestNodesResponse extends BaseNodesResponse<TestNodeResponse> {

        private final SubscribableListener<Void> onClose = new SubscribableListener<>();
        private final RefCounted refCounted = LeakTracker.wrap(AbstractRefCounted.of(() -> onClose.onResponse(null)));

        TestNodesResponse(ClusterName clusterName, List<TestNodeResponse> nodeResponses, List<FailedNodeException> failures) {
            super(clusterName, nodeResponses, failures);
        }

        @Override
        protected List<TestNodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(TestNodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<TestNodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }

        @Override
        public void incRef() {
            refCounted.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refCounted.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refCounted.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refCounted.hasReferences();
        }

        void addCloseListener(ActionListener<Void> listener) {
            onClose.addListener(listener);
        }
    }

    private static class TestNodeRequest extends TransportRequest {
        private final RefCounted refCounted = AbstractRefCounted.of(() -> {});

        TestNodeRequest() {}

        TestNodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void incRef() {
            refCounted.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refCounted.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refCounted.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refCounted.hasReferences();
        }
    }

    private static class TestNodeResponse extends BaseNodeResponse {

        private final SubscribableListener<Void> onClose = new SubscribableListener<>();
        private final RefCounted refCounted = LeakTracker.wrap(AbstractRefCounted.of(() -> onClose.onResponse(null)));

        TestNodeResponse() {
            this(mock(DiscoveryNode.class));
        }

        TestNodeResponse(DiscoveryNode node) {
            super(node);
        }

        protected TestNodeResponse(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void incRef() {
            refCounted.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refCounted.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refCounted.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refCounted.hasReferences();
        }

        void addCloseListener(ActionListener<Void> listener) {
            onClose.addListener(listener);
        }
    }

}
