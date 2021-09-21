/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master.info;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.instanceOf;

public class TransportClusterInfoActionTests extends ESTestCase {
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private DiscoveryNode localNode;
    private DiscoveryNode[] allNodes;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportMasterNodeActionTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockTransport transport = new MockTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(clusterService.getSettings(), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> clusterService.localNode(), null, Collections.emptySet());
        transportService.start();
        transportService.acceptIncomingRequests();
        localNode = new DiscoveryNode("local_node", buildNewFakeTransportAddress(), Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        allNodes = new DiscoveryNode[]{localNode};
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

    static class Request extends ClusterInfoRequest<Request> {
        Request() { }

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
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(identity);
        }
    }

    static class Action extends TransportClusterInfoAction<Request, Response> {
        Action(String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
            super(actionName,
                transportService,
                clusterService,
                threadPool,
                new ActionFilters(new HashSet<>()),
                Request::new,
                TestIndexNameExpressionResolver.newInstance(),
                Response::new
            );
        }

        @Override
        protected void doMasterOperation(Task task,
                                         Request request,
                                         String[] concreteIndices,
                                         ClusterState state,
                                         ActionListener<Response> listener) {
            listener.onResponse(new Response());
        }
    }

    public void testGlobalBlocksAreCheckedBeforeResolvingIndices() throws Exception {
        final boolean unblockBeforeTimeout = randomBoolean();

        Request request = new Request().masterNodeTimeout(TimeValue.timeValueSeconds(unblockBeforeTimeout ? 60 : 0));
        String indexRequestName = "my-index";
        request.indices(indexRequestName);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        ClusterState stateWithBlockWithoutIndexMetadata =
            ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
                .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK))
                .build();
        setState(clusterService, stateWithBlockWithoutIndexMetadata);

        ActionTestUtils.execute(new Action("internal:testAction", transportService, clusterService, threadPool), null, request, listener);

        if (unblockBeforeTimeout) {
            assertFalse(listener.isDone());
            IndexMetadata.Builder indexMetadataBuilder =
                IndexMetadata.builder(indexRequestName)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(0);
            ClusterState clusterStateWithoutBlocks = ClusterState.builder(ClusterStateCreationUtils.state(localNode, localNode, allNodes))
                .metadata(Metadata.builder().put(indexMetadataBuilder).build())
                .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                .build();
            setState(clusterService, clusterStateWithoutBlocks);
            assertTrue(listener.isDone());
            listener.get();
        } else {
            ExecutionException ex = expectThrows(ExecutionException.class, listener::get);
            assertThat(ex.getCause(), instanceOf(MasterNotDiscoveredException.class));
            assertThat(ex.getCause().getCause(), instanceOf(ClusterBlockException.class));
        }
    }
}
