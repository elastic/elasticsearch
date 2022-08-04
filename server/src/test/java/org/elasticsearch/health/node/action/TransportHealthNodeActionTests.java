/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TransportHealthNodeActionTests extends ESTestCase {
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private CapturingTransport transport;
    private DiscoveryNode localNode;
    private DiscoveryNode remoteNode;
    private DiscoveryNode[] allNodes;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportHealthNodeActionTests");
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        localNode = new DiscoveryNode(
            "local_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        remoteNode = new DiscoveryNode(
            "remote_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        allNodes = new DiscoveryNode[] { localNode, remoteNode };
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public static class Request extends HealthNodeRequest<Request> {

        Request() {}

        Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }
    }

    static class Response extends ActionResponse {
        private long identity = randomLong();

        Response() {}

        Response(StreamInput in) throws IOException {
            super(in);
            identity = in.readLong();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return identity == response.identity;
        }

        @Override
        public int hashCode() {
            return Objects.hash(identity);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(identity);
        }
    }

    class Action extends TransportHealthNodeAction<Request, Response> {
        Action(String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
            this(actionName, transportService, clusterService, threadPool, ThreadPool.Names.SAME);
        }

        Action(
            String actionName,
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            String executor
        ) {
            super(
                actionName,
                transportService,
                clusterService,
                threadPool,
                new ActionFilters(new HashSet<>()),
                Request::new,
                Response::new,
                executor
            );
        }

        @Override
        protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
            // remove unneeded threading by wrapping listener with SAME to prevent super.doExecute from wrapping it with LISTENER
            super.doExecute(task, request, new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.SAME, listener, false));
        }

        @Override
        protected void healthOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
            listener.onResponse(new Response());
        }
    }

    class WaitForSignalAction extends Action {
        private final CountDownLatch countDownLatch;

        WaitForSignalAction(
            String actionName,
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            CountDownLatch countDownLatch
        ) {
            super(actionName, transportService, clusterService, threadPool, ThreadPool.Names.SAME);
            this.countDownLatch = countDownLatch;
        }

        @Override
        protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                fail("Something went wrong while waiting for the latch");
            }
            super.doExecute(task, request, listener);
        }
    }

    public void testLocalHealthNode() throws ExecutionException, InterruptedException {
        final boolean healthOperationFailure = randomBoolean();

        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        final Exception exception = new Exception();
        final Response response = new Response();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes));

        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected void healthOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
                if (healthOperationFailure) {
                    listener.onFailure(exception);
                } else {
                    listener.onResponse(response);
                }
            }
        }, null, request, listener);
        assertTrue(listener.isDone());

        if (healthOperationFailure) {
            try {
                listener.get();
                fail("Expected exception but returned proper result");
            } catch (ExecutionException ex) {
                assertThat(ex.getCause(), equalTo(exception));
            }
        } else {
            assertThat(listener.get(), equalTo(response));
        }
    }

    public void testHealthNodeNotAvailable() throws InterruptedException {
        Request request = new Request().healthNodeTimeout(TimeValue.timeValueSeconds(0));
        setState(clusterService, ClusterStateCreationUtils.state(localNode, null, allNodes));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool), null, request, listener);
        assertTrue(listener.isDone());
        try {
            listener.get();
            fail("NoHealthNodeSelectedException should be thrown");
        } catch (ExecutionException ex) {
            assertThat(ex.getCause(), instanceOf(HealthNodeNotDiscoveredException.class));
        }
    }

    public void testHealthNodeBecomesAvailable() throws ExecutionException, InterruptedException {
        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool), null, request, listener);
        assertFalse(listener.isDone());
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes));
        assertTrue(listener.isDone());
        listener.get();
    }

    public void testDelegateToHealthNode() throws ExecutionException, InterruptedException {
        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, remoteNode, allNodes));

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool), null, request, listener);

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertThat(capturedRequest.node(), equalTo(remoteNode));
        assertThat(capturedRequest.request(), equalTo(request));
        assertThat(capturedRequest.action(), equalTo("internal:testAction"));

        Response response = new Response();
        transport.handleResponse(capturedRequest.requestId(), response);
        assertTrue(listener.isDone());
        assertThat(listener.get(), equalTo(response));
    }

    public void testDelegateToFailingHealthNode() throws ExecutionException, InterruptedException {
        boolean failsWithConnectTransportException = randomBoolean();
        boolean selectSameHealthNode = failsWithConnectTransportException && randomBoolean();
        Request request = new Request().healthNodeTimeout(TimeValue.timeValueSeconds(failsWithConnectTransportException ? 60 : 0));
        ClusterState clusterState = ClusterStateCreationUtils.state(localNode, localNode, remoteNode, allNodes);
        setState(clusterService, clusterState);

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool), null, request, listener);

        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = capturedRequests[0];
        assertThat(capturedRequest.node(), equalTo(remoteNode));
        assertThat(capturedRequest.request(), equalTo(request));
        assertThat(capturedRequest.action(), equalTo("internal:testAction"));

        if (selectSameHealthNode) {
            transport.handleRemoteError(
                capturedRequest.requestId(),
                randomBoolean() ? new ConnectTransportException(remoteNode, "Fake error") : new NodeClosedException(remoteNode)
            );
            assertFalse(listener.isDone());
            // simulate health node deselection
            setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));

            // reassign the health node task to the same health node and bump the version of the cluster state
            setState(clusterService, ClusterState.builder(clusterState).version(clusterState.getVersion() + 1));
            assertFalse(listener.isDone());
            capturedRequests = transport.getCapturedRequestsAndClear();
            assertThat(capturedRequests.length, equalTo(1));
            capturedRequest = capturedRequests[0];
            assertThat(capturedRequest.node(), equalTo(remoteNode));
            assertThat(capturedRequest.request(), equalTo(request));
            assertThat(capturedRequest.action(), equalTo("internal:testAction"));
        } else if (failsWithConnectTransportException) {
            transport.handleRemoteError(capturedRequest.requestId(), new ConnectTransportException(remoteNode, "Fake error"));
            assertFalse(listener.isDone());
            setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes));
            assertTrue(listener.isDone());
            listener.get();
        } else {
            ElasticsearchException t = new ElasticsearchException("test");
            t.addHeader("header", "is here");
            transport.handleRemoteError(capturedRequest.requestId(), t);
            assertTrue(listener.isDone());
            try {
                listener.get();
                fail("Expected exception but returned proper result");
            } catch (ExecutionException ex) {
                final Throwable cause = ex.getCause().getCause();
                assertThat(cause, instanceOf(ElasticsearchException.class));
                final ElasticsearchException es = (ElasticsearchException) cause;
                assertThat(es.getMessage(), equalTo(t.getMessage()));
                assertThat(es.getHeader("header"), equalTo(t.getHeader("header")));
            }
        }
    }

    public void testHealthNodeFailoverAfterDeselection() throws ExecutionException, InterruptedException {
        Request request = new Request().healthNodeTimeout(TimeValue.timeValueHours(1));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        final Response response = new Response();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes));

        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected void healthOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
                // The other node has become the health node, simulate failures of this node while publishing cluster state through
                // ZenDiscovery
                listener.onFailure(new NotHealthNodeException("Simulate failure"));
                setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, remoteNode, allNodes));
            }
        }, null, request, listener);

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertThat(capturedRequest.node(), equalTo(remoteNode));
        assertThat(capturedRequest.request(), equalTo(request));
        assertThat(capturedRequest.action(), equalTo("internal:testAction"));

        transport.handleResponse(capturedRequest.requestId(), response);
        assertTrue(listener.isDone());
        assertThat(listener.get(), equalTo(response));
    }

    public void testTaskCancellation() {
        TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());

        Request request = new Request();
        final CancellableTask task = (CancellableTask) taskManager.register("type", "internal:testAction", request);

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);

        threadPool.executor(ThreadPool.Names.MANAGEMENT)
            .submit(
                () -> ActionTestUtils.execute(
                    new WaitForSignalAction("internal:testAction", transportService, clusterService, threadPool, countDownLatch),
                    task,
                    request,
                    listener
                )
            );

        taskManager.cancel(task, "", () -> {});
        assertThat(task.isCancelled(), equalTo(true));

        countDownLatch.countDown();

        expectThrows(TaskCancelledException.class, listener::actionGet);
    }

    public void testNewHealthNodePredicate() {
        {
            // No change in cluster state
            ClusterState initialState = ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes);
            assertThat(TransportHealthNodeAction.createNewHealthNodePredicate(initialState).test(initialState), equalTo(false));
        }
        {
            // No health node selected
            ClusterState initialState = ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes);
            ClusterState newState = ClusterStateCreationUtils.state(localNode, localNode, allNodes);
            assertThat(TransportHealthNodeAction.createNewHealthNodePredicate(initialState).test(newState), equalTo(false));
        }
        {
            // Same health node reselected
            ClusterState initialState = ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes);
            ClusterState newState = ClusterState.builder(initialState).version(initialState.getVersion() + 1).build();
            assertThat(TransportHealthNodeAction.createNewHealthNodePredicate(initialState).test(newState), equalTo(true));
        }
        {
            // New health node selected
            ClusterState initialState = ClusterStateCreationUtils.state(localNode, localNode, allNodes);
            ClusterState newState = ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes);
            assertThat(TransportHealthNodeAction.createNewHealthNodePredicate(initialState).test(newState), equalTo(true));
        }
        {
            // Health node changed
            ClusterState initialState = ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes);
            ClusterState newState = ClusterStateCreationUtils.state(localNode, localNode, remoteNode, allNodes);
            assertThat(TransportHealthNodeAction.createNewHealthNodePredicate(initialState).test(newState), equalTo(true));
        }
    }
}
