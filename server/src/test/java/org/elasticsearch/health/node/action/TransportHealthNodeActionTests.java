/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskManager;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class TransportHealthNodeActionTests extends ESTestCase {
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private CapturingTransport transport;
    private DiscoveryNode localNode;
    private DiscoveryNode remoteNode;
    private DiscoveryNode[] allNodes;
    private TaskManager taskManager;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportHealthNodeActionTests");
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
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
        localNode = DiscoveryNodeUtils.builder("local_node")
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
            .build();
        remoteNode = DiscoveryNodeUtils.builder("remote_node")
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
            .build();
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

    public static class Request extends HealthNodeRequest {

        Request() {}

        Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String getDescription() {
            return "Test request.";
        }
    }

    static class Response extends ActionResponse {
        private long identity = randomLong();

        Response() {}

        Response(StreamInput in) throws IOException {
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
            this(actionName, transportService, clusterService, threadPool, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        }

        Action(
            String actionName,
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            Executor executor
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
            super(actionName, transportService, clusterService, threadPool, EsExecutors.DIRECT_EXECUTOR_SERVICE);
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

    class HealthOperationWithExceptionAction extends Action {

        HealthOperationWithExceptionAction(
            String actionName,
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool
        ) {
            super(actionName, transportService, clusterService, threadPool);
        }

        @Override
        protected void healthOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
            throw new RuntimeException("Simulated");
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
        assertThat(transportService.getRequestHandler("internal:testAction").canTripCircuitBreaker(), is(false));

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
        Request request = new Request();
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

    public void testDelegateToHealthNodeWithoutParentTask() throws ExecutionException, InterruptedException {
        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, remoteNode, allNodes));

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool), null, request, listener);
        assertThat(transportService.getRequestHandler("internal:testAction").canTripCircuitBreaker(), is(false));

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

    public void testDelegateToHealthNodeWithParentTask() throws ExecutionException, InterruptedException {
        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, remoteNode, allNodes));

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        final CancellableTask task = (CancellableTask) taskManager.register("type", "internal:testAction", request);
        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool), task, request, listener);
        assertThat(transportService.getRequestHandler("internal:testAction").canTripCircuitBreaker(), is(false));

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

    public void testHealthNodeOperationWithException() throws InterruptedException {
        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, localNode, allNodes));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ActionTestUtils.execute(
            new HealthOperationWithExceptionAction("internal:testAction", transportService, clusterService, threadPool),
            null,
            request,
            listener
        );
        assertTrue(listener.isDone());
        assertThat(transportService.getRequestHandler("internal:testAction").canTripCircuitBreaker(), is(false));

        try {
            listener.get();
            fail("A simulated RuntimeException should be thrown");
        } catch (ExecutionException ex) {
            assertThat(ex.getCause().getMessage(), equalTo("Simulated"));
        }
    }

    public void testTaskCancellation() {
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
}
