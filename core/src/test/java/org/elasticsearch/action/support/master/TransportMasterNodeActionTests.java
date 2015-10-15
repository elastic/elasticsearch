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
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TransportMasterNodeActionTests extends ESTestCase {
    private static ThreadPool threadPool;

    private TestClusterService clusterService;
    private TransportService transportService;
    private CapturingTransport transport;
    private DiscoveryNode localNode;
    private DiscoveryNode remoteNode;
    private DiscoveryNode[] allNodes;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new ThreadPool("TransportMasterNodeActionTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = new TestClusterService(threadPool);
        transportService = new TransportService(transport, threadPool);
        transportService.start();
        localNode = new DiscoveryNode("local_node", DummyTransportAddress.INSTANCE, Version.CURRENT);
        remoteNode = new DiscoveryNode("remote_node", DummyTransportAddress.INSTANCE, Version.CURRENT);
        allNodes = new DiscoveryNode[] { localNode, remoteNode };
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    <T> void assertListenerThrows(String msg, ActionFuture<?> listener, Class<?> klass) throws InterruptedException {
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
        protected void doExecute(final Request request, ActionListener<Response> listener) {
            // remove unneeded threading by wrapping listener with SAME to prevent super.doExecute from wrapping it with LISTENER
            super.doExecute(request, new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.SAME, listener));
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

    @Test
    public void testLocalOperationWithoutBlocks() throws ExecutionException, InterruptedException {
        final boolean masterOperationFailure = randomBoolean();

        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        final Throwable exception = new Throwable();
        final Response response = new Response();

        clusterService.setState(ClusterStateCreationUtils.state(localNode, localNode, allNodes));

        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool) {
            @Override
            protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
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

    @Test
    public void testLocalOperationWithBlocks() throws ExecutionException, InterruptedException {
        final boolean retryableBlock = randomBoolean();
        final boolean unblockBeforeTimeout = randomBoolean();

        Request request = new Request().masterNodeTimeout(TimeValue.timeValueSeconds(unblockBeforeTimeout ? 60 : 0));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        ClusterBlock block = new ClusterBlock(1, "", retryableBlock, true,
                randomFrom(RestStatus.values()), ClusterBlockLevel.ALL);
        ClusterState stateWithBlock = ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
                .blocks(ClusterBlocks.builder().addGlobalBlock(block)).build();
        clusterService.setState(stateWithBlock);

        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool) {
            @Override
            protected ClusterBlockException checkBlock(Request request, ClusterState state) {
                Set<ClusterBlock> blocks = state.blocks().global();
                return blocks.isEmpty() ? null : new ClusterBlockException(blocks);
            }
        }.execute(request, listener);

        if (retryableBlock && unblockBeforeTimeout) {
            assertFalse(listener.isDone());
            clusterService.setState(ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
                    .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build());
            assertTrue(listener.isDone());
            listener.get();
            return;
        }

        assertTrue(listener.isDone());
        assertListenerThrows("ClusterBlockException should be thrown", listener, ClusterBlockException.class);
    }

    @Test
    public void testForceLocalOperation() throws ExecutionException, InterruptedException {
        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        clusterService.setState(ClusterStateCreationUtils.state(localNode, randomFrom(null, localNode, remoteNode), allNodes));

        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool) {
            @Override
            protected boolean localExecute(Request request) {
                return true;
            }
        }.execute(request, listener);

        assertTrue(listener.isDone());
        listener.get();
    }

    @Test
    public void testMasterNotAvailable() throws ExecutionException, InterruptedException {
        Request request = new Request().masterNodeTimeout(TimeValue.timeValueSeconds(0));
        clusterService.setState(ClusterStateCreationUtils.state(localNode, null, allNodes));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool).execute(request, listener);
        assertTrue(listener.isDone());
        assertListenerThrows("MasterNotDiscoveredException should be thrown", listener, MasterNotDiscoveredException.class);
    }

    @Test
    public void testMasterBecomesAvailable() throws ExecutionException, InterruptedException {
        Request request = new Request();
        clusterService.setState(ClusterStateCreationUtils.state(localNode, null, allNodes));
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool).execute(request, listener);
        assertFalse(listener.isDone());
        clusterService.setState(ClusterStateCreationUtils.state(localNode, localNode, allNodes));
        assertTrue(listener.isDone());
        listener.get();
    }

    @Test
    public void testDelegateToMaster() throws ExecutionException, InterruptedException {
        Request request = new Request();
        clusterService.setState(ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        final AtomicBoolean delegationToMaster = new AtomicBoolean();

        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool) {
            @Override
            protected void processBeforeDelegationToMaster(Request request, ClusterState state) {
                logger.debug("Delegation to master called");
                delegationToMaster.set(true);
            }
        }.execute(request, listener);

        assertTrue("processBeforeDelegationToMaster not called", delegationToMaster.get());
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

    @Test
    public void testDelegateToFailingMaster() throws ExecutionException, InterruptedException {
        boolean failsWithConnectTransportException = randomBoolean();
        Request request = new Request().masterNodeTimeout(TimeValue.timeValueSeconds(failsWithConnectTransportException ? 60 : 0));
        clusterService.setState(ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));

        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        final AtomicBoolean delegationToMaster = new AtomicBoolean();

        new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool) {
            @Override
            protected void processBeforeDelegationToMaster(Request request, ClusterState state) {
                logger.debug("Delegation to master called");
                delegationToMaster.set(true);
            }
        }.execute(request, listener);

        assertTrue("processBeforeDelegationToMaster not called", delegationToMaster.get());
        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertTrue(capturedRequest.node.isMasterNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("testAction"));

        if (failsWithConnectTransportException) {
            transport.handleResponse(capturedRequest.requestId, new ConnectTransportException(remoteNode, "Fake error"));
            assertFalse(listener.isDone());
            clusterService.setState(ClusterStateCreationUtils.state(localNode, localNode, allNodes));
            assertTrue(listener.isDone());
            listener.get();
        } else {
            Throwable t = new Throwable();
            transport.handleResponse(capturedRequest.requestId, t);
            assertTrue(listener.isDone());
            try {
                listener.get();
                fail("Expected exception but returned proper result");
            } catch (ExecutionException ex) {
                assertThat(ex.getCause().getCause(), equalTo(t));
            }
        }
    }
}