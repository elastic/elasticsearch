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
package org.elasticsearch.action.support.master;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TransportMasterNodeActionTests extends ESTestCase {
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private CapturingTransport transport;
    private DiscoveryNode localNode;
    private DiscoveryNode remoteNode;
    private DiscoveryNode[] allNodes;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportMasterNodeActionTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(clusterService.getSettings(), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> clusterService.localNode(), null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        localNode = new DiscoveryNode("local_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
                Collections.singleton(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        remoteNode = new DiscoveryNode("remote_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
                Collections.singleton(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        allNodes = new DiscoveryNode[]{localNode, remoteNode};
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

    void assertListenerThrows(String msg, ActionFuture<?> listener, Class<?> klass) throws InterruptedException {
        try {
            listener.get();
            fail(msg);
        } catch (ExecutionException ex) {
            assertThat(ex.getCause(), instanceOf(klass));
        }
    }

    public static class Request extends MasterNodeRequest<Request> {
        Request() {}

        Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    class Response extends ActionResponse {
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

    class Action extends TransportMasterNodeAction<Request, Response> {
        Action(String actionName, TransportService transportService, ClusterService clusterService,
               ThreadPool threadPool) {
            super(actionName, transportService, clusterService, threadPool,
                    new ActionFilters(new HashSet<>()), Request::new, new IndexNameExpressionResolver());
        }

        @Override
        protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
            // remove unneeded threading by wrapping listener with SAME to prevent super.doExecute from wrapping it with LISTENER
            super.doExecute(task, request, new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.SAME, listener, false));
        }

        @Override
        protected String executor() {
            // very lightweight operation in memory, no need to fork to a thread
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            listener.onResponse(new Response()); // default implementation, overridden in specific tests
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return null; // default implementation, overridden in specific tests
        }
    }

    public void testLocalOperationWithoutBlocks() throws ExecutionException, InterruptedException {
        final boolean masterOperationFailure = randomBoolean();

        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        final Exception exception = new Exception();
        final Response response = new Response();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));

        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) {
                if (masterOperationFailure) {
                    listener.onFailure(exception);
                } else {
                    listener.onResponse(response);
                }
            }
        }, null, request, listener);
        assertTrue(listener.isDone());

        if (masterOperationFailure) {
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

    public void testLocalOperationWithBlocks() throws ExecutionException, InterruptedException {
        final boolean retryableBlock = randomBoolean();
        final boolean unblockBeforeTimeout = randomBoolean();

        Request request = new Request().masterNodeTimeout(TimeValue.timeValueSeconds(unblockBeforeTimeout ? 60 : 0));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        ClusterBlock block = new ClusterBlock(1, "", retryableBlock, true,
            false, randomFrom(RestStatus.values()), ClusterBlockLevel.ALL);
        ClusterState stateWithBlock = ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
                .blocks(ClusterBlocks.builder().addGlobalBlock(block)).build();
        setState(clusterService, stateWithBlock);

        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected ClusterBlockException checkBlock(Request request, ClusterState state) {
                Set<ClusterBlock> blocks = state.blocks().global();
                return blocks.isEmpty() ? null : new ClusterBlockException(blocks);
            }
        }, null, request, listener);

        if (retryableBlock && unblockBeforeTimeout) {
            assertFalse(listener.isDone());
            setState(clusterService, ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
                    .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build());
            assertTrue(listener.isDone());
            listener.get();
            return;
        }

        assertTrue(listener.isDone());
        if (retryableBlock) {
            try {
                listener.get();
                fail("Expected exception but returned proper result");
            } catch (ExecutionException ex) {
                assertThat(ex.getCause(), instanceOf(MasterNotDiscoveredException.class));
                assertThat(ex.getCause().getCause(), instanceOf(ClusterBlockException.class));
            }
        } else {
            assertListenerThrows("ClusterBlockException should be thrown", listener, ClusterBlockException.class);
        }
    }

    public void testCheckBlockThrowsException() throws InterruptedException {
        boolean throwExceptionOnRetry = randomBoolean();
        Request request = new Request().masterNodeTimeout(TimeValue.timeValueSeconds(60));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        ClusterBlock block = new ClusterBlock(1, "", true, true,
            false, randomFrom(RestStatus.values()), ClusterBlockLevel.ALL);
        ClusterState stateWithBlock = ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
            .blocks(ClusterBlocks.builder().addGlobalBlock(block)).build();
        setState(clusterService, stateWithBlock);

        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected ClusterBlockException checkBlock(Request request, ClusterState state) {
                Set<ClusterBlock> blocks = state.blocks().global();
                if (throwExceptionOnRetry == false || blocks.isEmpty()) {
                    throw new RuntimeException("checkBlock has thrown exception");
                }
                return new ClusterBlockException(blocks);

            }
        }, null, request, listener);

        if (throwExceptionOnRetry == false) {
            assertListenerThrows("checkBlock has thrown exception", listener, RuntimeException.class);
        } else {
            assertFalse(listener.isDone());
            setState(clusterService, ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build());
            assertListenerThrows("checkBlock has thrown exception", listener, RuntimeException.class);
        }
    }

    public void testForceLocalOperation() throws ExecutionException, InterruptedException {
        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, randomFrom(localNode, remoteNode, null), allNodes));

        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected boolean localExecute(Request request) {
                return true;
            }
        }, null, request, listener);

        assertTrue(listener.isDone());
        listener.get();
    }

    public void testMasterNotAvailable() throws ExecutionException, InterruptedException {
        Request request = new Request().masterNodeTimeout(TimeValue.timeValueSeconds(0));
        setState(clusterService, ClusterStateCreationUtils.state(localNode, null, allNodes));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool), null, request, listener);
        assertTrue(listener.isDone());
        assertListenerThrows("MasterNotDiscoveredException should be thrown", listener, MasterNotDiscoveredException.class);
    }

    public void testMasterBecomesAvailable() throws ExecutionException, InterruptedException {
        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, null, allNodes));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool), null, request, listener);
        assertFalse(listener.isDone());
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));
        assertTrue(listener.isDone());
        listener.get();
    }

    public void testDelegateToMaster() throws ExecutionException, InterruptedException {
        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool), null, request, listener);

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertTrue(capturedRequest.node.isMasterNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("internal:testAction"));

        Response response = new Response();
        transport.handleResponse(capturedRequest.requestId, response);
        assertTrue(listener.isDone());
        assertThat(listener.get(), equalTo(response));
    }

    public void testDelegateToFailingMaster() throws ExecutionException, InterruptedException {
        boolean failsWithConnectTransportException = randomBoolean();
        boolean rejoinSameMaster = failsWithConnectTransportException && randomBoolean();
        Request request = new Request().masterNodeTimeout(TimeValue.timeValueSeconds(failsWithConnectTransportException ? 60 : 0));
        DiscoveryNode masterNode = this.remoteNode;
        setState(clusterService, ClusterState.builder(ClusterStateCreationUtils.state(localNode, masterNode, allNodes))
            .version(randomIntBetween(0, 10))); // use a random base version so it can go down when simulating a restart.

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool), null, request, listener);

        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests.length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = capturedRequests[0];
        assertTrue(capturedRequest.node.isMasterNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("internal:testAction"));

        if (rejoinSameMaster) {
            transport.handleRemoteError(capturedRequest.requestId, new ConnectTransportException(masterNode, "Fake error"));
            assertFalse(listener.isDone());
            if (randomBoolean()) {
                // simulate master node removal
                final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
                nodesBuilder.masterNodeId(null);
                setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodesBuilder));
            }
            if (randomBoolean()) {
                // reset the same state to increment a version simulating a join of an existing node
                // simulating use being disconnected
                final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
                nodesBuilder.masterNodeId(masterNode.getId());
                setState(clusterService, ClusterState.builder(clusterService.state()).nodes(nodesBuilder));
            } else {
                // simulate master restart followed by a state recovery - this will reset the cluster state version
                final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterService.state().nodes());
                nodesBuilder.remove(masterNode);
                masterNode = new DiscoveryNode(masterNode.getId(), masterNode.getAddress(), masterNode.getVersion());
                nodesBuilder.add(masterNode);
                nodesBuilder.masterNodeId(masterNode.getId());
                final ClusterState.Builder builder = ClusterState.builder(clusterService.state()).nodes(nodesBuilder);
                setState(clusterService, builder.version(0));
            }
            assertFalse(listener.isDone());
            capturedRequests = transport.getCapturedRequestsAndClear();
            assertThat(capturedRequests.length, equalTo(1));
            capturedRequest = capturedRequests[0];
            assertTrue(capturedRequest.node.isMasterNode());
            assertThat(capturedRequest.request, equalTo(request));
            assertThat(capturedRequest.action, equalTo("internal:testAction"));
        } else if (failsWithConnectTransportException) {
            transport.handleRemoteError(capturedRequest.requestId, new ConnectTransportException(masterNode, "Fake error"));
            assertFalse(listener.isDone());
            setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));
            assertTrue(listener.isDone());
            listener.get();
        } else {
            ElasticsearchException t = new ElasticsearchException("test");
            t.addHeader("header", "is here");
            transport.handleRemoteError(capturedRequest.requestId, t);
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

    public void testMasterFailoverAfterStepDown() throws ExecutionException, InterruptedException {
        Request request = new Request().masterNodeTimeout(TimeValue.timeValueHours(1));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        final Response response = new Response();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));

        ActionTestUtils.execute(new Action( "internal:testAction", transportService, clusterService, threadPool) {
            @Override
            protected void masterOperation(Task task, Request request, ClusterState state,
                                           ActionListener<Response> listener) throws Exception {
                // The other node has become master, simulate failures of this node while publishing cluster state through ZenDiscovery
                setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));
                Exception failure = randomBoolean()
                        ? new FailedToCommitClusterStateException("Fake error")
                        : new NotMasterException("Fake error");
                listener.onFailure(failure);
            }
        }, null, request, listener);

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertTrue(capturedRequest.node.isMasterNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("internal:testAction"));

        transport.handleResponse(capturedRequest.requestId, response);
        assertTrue(listener.isDone());
        assertThat(listener.get(), equalTo(response));
    }
}
