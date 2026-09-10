/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.update;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.action.DocWriteResponse.Result.NOT_FOUND;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;

// This test suite mocks out a fair amount of TransportUpdateAction behavior because it was ported from a
// more generic superclass (TransportInstancesSingleOperationActionTests) that only had a single subclass.
// It deserves more coverage than it currently has.
public class TransportUpdateActionTests extends ESTestCase {

    private static ThreadPool THREAD_POOL;

    private ProjectId projectId;

    private ClusterService clusterService;
    private ProjectResolver projectResolver;
    private CapturingTransport transport;
    private TransportService transportService;

    private static UpdateResponse RESPONSE = new UpdateResponse(new ShardId("index", "index_uuid", 0), "id", -2, 0, 0, NOT_FOUND);

    class TestTransportUpdateAction extends TransportUpdateAction {
        TestTransportUpdateAction() {
            this(new MyResolver());
        }

        private TestTransportUpdateAction(IndexNameExpressionResolver indexNameExpressionResolver) {
            super(
                THREAD_POOL,
                TransportUpdateActionTests.this.clusterService,
                TransportUpdateActionTests.this.projectResolver,
                TransportUpdateActionTests.this.transportService,
                mock(UpdateHelper.class),
                new ActionFilters(new HashSet<>()),
                indexNameExpressionResolver,
                mock(IndicesService.class),
                new AutoCreateIndex(
                    Settings.EMPTY,
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    indexNameExpressionResolver,
                    EmptySystemIndices.INSTANCE
                ),
                mock(NodeClient.class)
            );
        }

        @Override
        protected Executor executor(ShardId shardId) {
            return EsExecutors.DIRECT_EXECUTOR_SERVICE;
        }

        @Override
        protected void resolveRequest(ProjectState state, UpdateRequest request) {}
    }

    static class MyResolver extends IndexNameExpressionResolver {
        MyResolver() {
            super(new ThreadContext(Settings.EMPTY), EmptySystemIndices.INSTANCE, TestProjectResolvers.DEFAULT_PROJECT_ONLY);
        }

        @Override
        public String[] concreteIndexNames(ProjectMetadata project, IndicesRequest request) {
            return request.indices();
        }
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool(TransportUpdateActionTests.class.getSimpleName());
    }

    @Before
    public void initServices() throws Exception {
        projectId = randomProjectIdOrDefault();
        transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        projectResolver = TestProjectResolvers.singleProject(projectId);
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
    }

    @After
    public void closeServices() throws Exception {
        clusterService.close();
        transportService.close();
    }

    @AfterClass
    public static void destroyThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        THREAD_POOL = null;
    }

    public void testGlobalBlock() {
        UpdateRequest request = new UpdateRequest();
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        ClusterBlocks.Builder block = ClusterBlocks.builder()
            .addGlobalBlock(new ClusterBlock(1, "", false, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(
            clusterService,
            ClusterState.builder(clusterService.state()).putProjectMetadata(ProjectMetadata.builder(projectId)).blocks(block)
        );
        try {
            new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
            listener.get();
            fail("expected ClusterBlockException");
        } catch (Exception e) {
            if (ExceptionsHelper.unwrap(e, ClusterBlockException.class) == null) {
                logger.info("expected ClusterBlockException  but got ", e);
                fail("expected ClusterBlockException");
            }
        }
    }

    public void testBasicRequestWorks() throws InterruptedException, ExecutionException, TimeoutException {
        UpdateRequest request = new UpdateRequest("test", "id");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", randomBoolean(), ShardRoutingState.STARTED));
        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        transport.handleResponse(transport.capturedRequests()[0].requestId(), RESPONSE);
        listener.get();
    }

    public void testFailureWithoutRetry() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "id");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", randomBoolean(), ShardRoutingState.STARTED));

        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        long requestId = transport.capturedRequests()[0].requestId();
        transport.clear();
        // this should not trigger retry or anything and the listener should report exception immediately
        transport.handleRemoteError(
            requestId,
            new TransportException("a generic transport exception", new Exception("generic test exception"))
        );

        try {
            // result should return immediately
            assertTrue(listener.isDone());
            listener.get();
            fail("this should fail with a transport exception");
        } catch (ExecutionException t) {
            if (ExceptionsHelper.unwrap(t, TransportException.class) == null) {
                logger.info("expected TransportException  but got ", t);
                fail("expected and TransportException");
            }
        }
    }

    public void testSuccessAfterRetryWithClusterStateUpdate() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "id");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        boolean local = randomBoolean();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", local, ShardRoutingState.INITIALIZING));
        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        // this should fail because primary not initialized
        assertThat(transport.capturedRequests().length, equalTo(0));
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", local, ShardRoutingState.STARTED));
        // this time it should work
        assertThat(transport.capturedRequests().length, equalTo(1));
        transport.handleResponse(transport.capturedRequests()[0].requestId(), RESPONSE);
        listener.get();
    }

    public void testSuccessAfterRetryWithExceptionFromTransport() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "id");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        boolean local = randomBoolean();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", local, ShardRoutingState.STARTED));
        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        long requestId = transport.capturedRequests()[0].requestId();
        transport.clear();
        DiscoveryNode node = clusterService.state().getNodes().getLocalNode();
        transport.handleLocalError(requestId, new ConnectTransportException(node, "test exception"));
        // trigger cluster state observer
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", local, ShardRoutingState.STARTED));
        assertThat(transport.capturedRequests().length, equalTo(1));
        transport.handleResponse(transport.capturedRequests()[0].requestId(), RESPONSE);
        listener.get();
    }

    public void testRetryOfAnAlreadyTimedOutRequest() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "id").timeout(new TimeValue(0, TimeUnit.MILLISECONDS));
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", randomBoolean(), ShardRoutingState.STARTED));
        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        long requestId = transport.capturedRequests()[0].requestId();
        transport.clear();
        DiscoveryNode node = clusterService.state().getNodes().getLocalNode();
        transport.handleLocalError(requestId, new ConnectTransportException(node, "test exception"));

        // wait until the timeout was triggered and we actually tried to send for the second time
        assertBusy(() -> assertThat(transport.capturedRequests().length, equalTo(1)));

        // let it fail the second time too
        requestId = transport.capturedRequests()[0].requestId();
        transport.handleLocalError(requestId, new ConnectTransportException(node, "test exception"));
        try {
            // result should return immediately
            assertTrue(listener.isDone());
            listener.get();
            fail("this should fail with a transport exception");
        } catch (ExecutionException t) {
            if (ExceptionsHelper.unwrap(t, ConnectTransportException.class) == null) {
                logger.info("expected ConnectTransportException  but got ", t);
                fail("expected and ConnectTransportException");
            }
        }
    }

    public void testUnresolvableRequestDoesNotHang() throws InterruptedException, ExecutionException, TimeoutException {
        final var action = new TestTransportUpdateAction() {
            @Override
            protected void resolveRequest(ProjectState state, UpdateRequest request) {
                throw new IllegalStateException("request cannot be resolved");
            }
        };
        UpdateRequest request = new UpdateRequest().index("test");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", randomBoolean(), ShardRoutingState.STARTED));
        action.new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(0));
        try {
            listener.get();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrap(e, IllegalStateException.class) == null) {
                logger.info("expected IllegalStateException  but got ", e);
                fail("expected and IllegalStateException");
            }
        }
    }
}
