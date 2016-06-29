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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.Discovery;
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

import java.util.Collections;
import java.util.HashSet;
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
        transportService = new TransportService(clusterService.getSettings(), transport, threadPool);
        transportService.start();
        transportService.acceptIncomingRequests();
        localNode = new DiscoveryNode("local_node", DummyTransportAddress.INSTANCE, Collections.emptyMap(),
                Collections.singleton(DiscoveryNode.Role.MASTER), Version.CURRENT);
        remoteNode = new DiscoveryNode("remote_node", DummyTransportAddress.INSTANCE, Collections.emptyMap(),
                Collections.singleton(DiscoveryNode.Role.MASTER), Version.CURRENT);
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
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    class Response extends ActionResponse {}

    class Action extends TransportMasterNodeAction<Request, Response> {
        Action(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
            super(settings, actionName, transportService, clusterService, threadPool,
                    new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY), Request::new);
        }

        @Override
        protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
            // remove unneeded threading by wrapping listener with SAME to prevent super.doExecute from wrapping it with LISTENER
            super.doExecute(task, request, new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.SAME, listener));
        }

        @Override
        protected String executor() {
            // very lightweight operation in memory, no need to fork to a thread
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
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

        final Throwable exception = new Throwable();
        final Response response = new Response();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));

        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool) {
            @Override
            protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
                if (masterOperationFailure) {
                    listener.onFailure(exception);
                } else {
                    listener.onResponse(response);
                }
            }
        }.execute(request, listener);
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
                randomFrom(RestStatus.values()), ClusterBlockLevel.ALL);
        ClusterState stateWithBlock = ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
                .blocks(ClusterBlocks.builder().addGlobalBlock(block)).build();
        setState(clusterService, stateWithBlock);

        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool) {
            @Override
            protected ClusterBlockException checkBlock(Request request, ClusterState state) {
                Set<ClusterBlock> blocks = state.blocks().global();
                return blocks.isEmpty() ? null : new ClusterBlockException(blocks);
            }
        }.execute(request, listener);

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

    public void testForceLocalOperation() throws ExecutionException, InterruptedException {
        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, randomFrom(null, localNode, remoteNode), allNodes));

        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool) {
            @Override
            protected boolean localExecute(Request request) {
                return true;
            }
        }.execute(request, listener);

        assertTrue(listener.isDone());
        listener.get();
    }

    public void testMasterNotAvailable() throws ExecutionException, InterruptedException {
        Request request = new Request().masterNodeTimeout(TimeValue.timeValueSeconds(0));
        setState(clusterService, ClusterStateCreationUtils.state(localNode, null, allNodes));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool).execute(request, listener);
        assertTrue(listener.isDone());
        assertListenerThrows("MasterNotDiscoveredException should be thrown", listener, MasterNotDiscoveredException.class);
    }

    public void testMasterBecomesAvailable() throws ExecutionException, InterruptedException {
        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, null, allNodes));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool).execute(request, listener);
        assertFalse(listener.isDone());
        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));
        assertTrue(listener.isDone());
        listener.get();
    }

    public void testDelegateToMaster() throws ExecutionException, InterruptedException {
        Request request = new Request();
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool).execute(request, listener);

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertTrue(capturedRequest.node.isMasterNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("testAction"));

        Response response = new Response();
        transport.handleResponse(capturedRequest.requestId, response);
        assertTrue(listener.isDone());
        assertThat(listener.get(), equalTo(response));
    }

    public void testDelegateToFailingMaster() throws ExecutionException, InterruptedException {
        boolean failsWithConnectTransportException = randomBoolean();
        Request request = new Request().masterNodeTimeout(TimeValue.timeValueSeconds(failsWithConnectTransportException ? 60 : 0));
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool).execute(request, listener);

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertTrue(capturedRequest.node.isMasterNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("testAction"));

        if (failsWithConnectTransportException) {
            transport.handleRemoteError(capturedRequest.requestId, new ConnectTransportException(remoteNode, "Fake error"));
            assertFalse(listener.isDone());
            setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));
            assertTrue(listener.isDone());
            listener.get();
        } else {
            Throwable t = new Throwable();
            transport.handleRemoteError(capturedRequest.requestId, t);
            assertTrue(listener.isDone());
            try {
                listener.get();
                fail("Expected exception but returned proper result");
            } catch (ExecutionException ex) {
                assertThat(ex.getCause().getCause(), equalTo(t));
            }
        }
    }

    public void testMasterFailoverAfterStepDown() throws ExecutionException, InterruptedException {
        Request request = new Request().masterNodeTimeout(TimeValue.timeValueHours(1));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        final Response response = new Response();

        setState(clusterService, ClusterStateCreationUtils.state(localNode, localNode, allNodes));

        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool) {
            @Override
            protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
                // The other node has become master, simulate failures of this node while publishing cluster state through ZenDiscovery
                setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));
                Throwable failure = randomBoolean()
                        ? new Discovery.FailedToCommitClusterStateException("Fake error")
                        : new NotMasterException("Fake error");
                listener.onFailure(failure);
            }
        }.execute(request, listener);

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertTrue(capturedRequest.node.isMasterNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("testAction"));

        transport.handleResponse(capturedRequest.requestId, response);
        assertTrue(listener.isDone());
        assertThat(listener.get(), equalTo(response));
    }
}
