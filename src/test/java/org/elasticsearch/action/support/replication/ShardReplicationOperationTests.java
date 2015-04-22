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
package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.instanceOf;

public class ShardReplicationOperationTests extends ElasticsearchTestCase {

    static class Request extends ShardReplicationOperationRequest<Request> {
        ShardId shardId;
    }

    static class Response extends ActionWriteResponse {

    }

    static class CapturingTransport implements Transport {
        private TransportServiceAdapter adapter;

        static public class CapturedRequest {
            final public DiscoveryNode node;
            final public long requestId;
            final public String action;
            final public TransportRequest request;

            public CapturedRequest(DiscoveryNode node, long requestId, String action, TransportRequest request) {
                this.node = node;
                this.requestId = requestId;
                this.action = action;
                this.request = request;
            }
        }

        private BlockingQueue<CapturedRequest> capturedRequests = ConcurrentCollections.newBlockingQueue();

        public CapturedRequest[] capturedRequests() {
            return capturedRequests.toArray(new CapturedRequest[0]);
        }

        public void clear() {
            capturedRequests.clear();
        }

        public void handleResponse(final long requestId, final TransportResponse response) {
            adapter.onResponseReceived(requestId).handleResponse(response);
        }


        @Override
        public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) throws IOException, TransportException {
            capturedRequests.add(new CapturedRequest(node, requestId, action, request));
        }


        @Override
        public void transportServiceAdapter(TransportServiceAdapter adapter) {
            this.adapter = adapter;
        }

        @Override
        public BoundTransportAddress boundAddress() {
            return null;
        }

        @Override
        public Map<String, BoundTransportAddress> profileBoundAddresses() {
            return null;
        }

        @Override
        public TransportAddress[] addressesFromString(String address) throws Exception {
            return new TransportAddress[0];
        }

        @Override
        public boolean addressSupported(Class<? extends TransportAddress> address) {
            return false;
        }

        @Override
        public boolean nodeConnected(DiscoveryNode node) {
            return true;
        }

        @Override
        public void connectToNode(DiscoveryNode node) throws ConnectTransportException {

        }

        @Override
        public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {

        }

        @Override
        public void disconnectFromNode(DiscoveryNode node) {

        }

        @Override
        public long serverOpen() {
            return 0;
        }

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {

        }

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {

        }

        @Override
        public Transport start() throws ElasticsearchException {
            return null;
        }

        @Override
        public Transport stop() throws ElasticsearchException {
            return null;
        }

        @Override
        public void close() throws ElasticsearchException {

        }
    }

    static class Action extends TransportShardReplicationOperationAction<Request, Request, Response> {

        protected Action(Settings settings, String actionName, TransportService transportService,
                         ClusterService clusterService,
                         ThreadPool threadPool) {
            super(settings, actionName, transportService, clusterService, null, threadPool,
                    new ShardStateAction(settings, clusterService, transportService, null, null),
                    new ActionFilters(new HashSet<ActionFilter>()));
        }

        @Override
        protected Request newRequestInstance() {
            return new Request();
        }

        @Override
        protected Request newReplicaRequestInstance() {
            return new Request();
        }

        @Override
        protected Response newResponseInstance() {
            return new Response();
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Tuple<Response, Request> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
            return null;
        }

        @Override
        protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {

        }

        @Override
        protected ShardIterator shards(ClusterState clusterState, InternalRequest request) throws ElasticsearchException {
            return clusterState.getRoutingTable().index(request.concreteIndex()).shard(request.request().shardId.id()).shardsIt();
        }

        @Override
        protected boolean checkWriteConsistency() {
            return false;
        }

        @Override
        protected boolean resolveIndex() {
            return false;
        }
    }

    static CapturingTransport transport;
    static ThreadPool threadPool;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new ThreadPool("ShardReplicationOperationTests");
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    @Test
    public void testBlocks() throws ExecutionException, InterruptedException {
        TestClusterService clusterService = new TestClusterService(threadPool);
        Action action = new Action(ImmutableSettings.EMPTY, "test",
                new TransportService(transport, threadPool), clusterService, threadPool);

        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        ClusterBlocks.Builder block = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(1, "non retryable", false, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        clusterService.setState(ClusterState.builder(clusterService.state()).blocks(block));
        TransportShardReplicationOperationAction<Request, Request, Response>.PrimaryPhase primaryPhase = action.new PrimaryPhase(request, listener);
        assertFalse("primary phase should stop execution", primaryPhase.checkBlocks());
        try {
            listener.get();
            fail("primary phase should fail operation");
        } catch (ExecutionException ex) {
            assertThat(ex.getCause(), instanceOf(ClusterBlockException.class));
        }

        block = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(1, "retryable", true, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        clusterService.setState(ClusterState.builder(clusterService.state()).blocks(block));
        listener = new PlainActionFuture<>();
        primaryPhase = action.new PrimaryPhase(request, listener);
        assertFalse("primary phase should stop execution on retryable block", primaryPhase.checkBlocks());
        assertFalse("primary phase should wait on retryable block", listener.isDone());

        block = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(1, "non retryable", false, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        clusterService.setState(ClusterState.builder(clusterService.state()).blocks(block));
        try {
            listener.get();
            fail("primary phase should fail operation when moving from a retryable block a non-retryable one");
        } catch (ExecutionException ex) {
            assertThat(ex.getCause(), instanceOf(ClusterBlockException.class));
        }
    }
}
