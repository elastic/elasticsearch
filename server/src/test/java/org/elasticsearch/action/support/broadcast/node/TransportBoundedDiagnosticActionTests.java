/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast.node;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.broadcast.node.BoundedDiagnosticRequestPermits.MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE;

public class TransportBoundedDiagnosticActionTests extends AbstractTransportBroadcastByNodeActionTestCase {

    private final ClusterSettings emptyClusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private BoundedDiagnosticRequestPermits boundedDiagnosticRequestPermits;

    class TestDiagnosticTransportBroadcastByNodeAction extends TransportBoundedDiagnosticAction<
        Request,
        Response,
        TransportBroadcastByNodeAction.EmptyResult> {
        private final Map<ShardRouting, Object> shards = new HashMap<>();

        TestDiagnosticTransportBroadcastByNodeAction(
            String postfix,
            TransportService transportService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Writeable.Reader<Request> request,
            String executor,
            BoundedDiagnosticRequestPermits boundedDiagnosticRequestPermits
        ) {
            super(
                "indices:admin/test" + postfix,
                TransportBoundedDiagnosticActionTests.this.clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                request,
                executor,
                boundedDiagnosticRequestPermits
            );
        }

        @Override
        protected EmptyResult readShardResult(StreamInput in) {
            return EmptyResult.readEmptyResultFrom(in);
        }

        @Override
        protected Response newResponse(
            Request request,
            int totalShards,
            int successfulShards,
            int failedShards,
            List<EmptyResult> emptyResults,
            List<DefaultShardOperationFailedException> shardFailures,
            ClusterState clusterState
        ) {
            return new Response(totalShards, successfulShards, failedShards, shardFailures);
        }

        @Override
        protected Request readRequestFrom(StreamInput in) throws IOException {
            return new Request(in);
        }

        @Override
        protected void shardOperation(Request request, ShardRouting shardRouting, Task task, ActionListener<EmptyResult> listener) {
            ActionListener.completeWith(listener, () -> {
                if (rarely()) {
                    shards.put(shardRouting, Boolean.TRUE);
                    return EmptyResult.INSTANCE;
                } else {
                    ElasticsearchException e = new ElasticsearchException("operation failed");
                    shards.put(shardRouting, e);
                    throw e;
                }
            });
        }

        @Override
        protected ShardsIterator shards(ClusterState clusterState, Request request, String[] concreteIndices) {
            return clusterState.routingTable().allShards(new String[] { TEST_INDEX });
        }

        @Override
        protected ClusterBlockException checkGlobalBlock(ClusterState state, Request request) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices) {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
        }

        public Map<ShardRouting, Object> getResults() {
            return shards;
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        boundedDiagnosticRequestPermits = new BoundedDiagnosticRequestPermits(
            Settings.builder().put(MAX_CONCURRENT_BOUNDED_DIAGNOSTIC_REQUESTS_PER_NODE.getKey(), 1).build(),
            emptyClusterSettings
        );
    }

    public void testReleasingAfterSuccess() {
        Request request = new Request(TEST_INDEX);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        TestDiagnosticTransportBroadcastByNodeAction boundedAction = new TestDiagnosticTransportBroadcastByNodeAction(
            "_successful",
            transportService,
            new ActionFilters(new HashSet<>()),
            new MyResolver(),
            Request::new,
            ThreadPool.Names.SAME,
            boundedDiagnosticRequestPermits
        );
        boundedAction.doExecute(null, request, listener);
        assertFalse(boundedDiagnosticRequestPermits.tryAcquire());
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        ShardsIterator shardIt = clusterService.state().getRoutingTable().allShards(new String[] { TEST_INDEX });
        Map<String, List<ShardRouting>> map = new HashMap<>();
        for (ShardRouting shard : shardIt) {
            if (map.containsKey(shard.currentNodeId()) == false) {
                map.put(shard.currentNodeId(), new ArrayList<>());
            }
            map.get(shard.currentNodeId()).add(shard);
        }
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            List<BroadcastShardOperationFailedException> exceptions = new ArrayList<>();
            long requestId = entry.getValue().get(0).requestId();
            if (rarely()) {
                transport.handleRemoteError(requestId, new Exception());
            } else {
                TransportBroadcastByNodeAction<Request, Response, TransportBroadcastByNodeAction.EmptyResult>.NodeResponse nodeResponse =
                    boundedAction.new NodeResponse(entry.getKey(), map.size(), List.of(), exceptions);
                transport.handleResponse(requestId, nodeResponse);
            }
        }
        assertTrue(boundedDiagnosticRequestPermits.tryAcquire());
    }

    public void testRequestRejectionBeyondTheLimit() {
        Request request = new Request(TEST_INDEX);
        PlainActionFuture<Response> listener1 = new PlainActionFuture<>();
        PlainActionFuture<Response> listener2 = new PlainActionFuture<>();

        TestDiagnosticTransportBroadcastByNodeAction boundedAction1 = new TestDiagnosticTransportBroadcastByNodeAction(
            "_successful",
            transportService,
            new ActionFilters(new HashSet<>()),
            new MyResolver(),
            Request::new,
            ThreadPool.Names.SAME,
            boundedDiagnosticRequestPermits
        );
        TestDiagnosticTransportBroadcastByNodeAction boundedAction2 = new TestDiagnosticTransportBroadcastByNodeAction(
            "_rejected",
            transportService,
            new ActionFilters(new HashSet<>()),
            new MyResolver(),
            Request::new,
            ThreadPool.Names.SAME,
            boundedDiagnosticRequestPermits
        );
        boundedAction1.doExecute(null, request, listener1);
        boundedAction2.doExecute(null, request, listener2);
        expectThrows(EsRejectedExecutionException.class, listener2::actionGet);
    }
}
